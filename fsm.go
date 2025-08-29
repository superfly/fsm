package fsm

//go:generate rm -rf gen
//go:generate go run github.com/bufbuild/buf/cmd/buf@v1.28.1 generate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/benbjohnson/immutable"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	actionCounterVec = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "fsm_action_count",
			Help: "A count of action completions.",
		},
		[]string{"action", "resource", "status", "kind"},
	)

	actionDurationVec = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "fsm_action_duration_seconds",
			Help:    "Time spent performing an action.",
			Buckets: []float64{.5, 1, 2.5, 5, 10, 30, 60, 150, 300, 600, 1200},
		},
		[]string{"action", "resource", "status", "kind"},
	)
)

type Request[R, W any] struct {
	Msg *R
	W   Response[W]

	logger logrus.FieldLogger
	run    Run
}

func (r *Request[_, _]) Any() any {
	if r == nil {
		return nil
	}
	return r.Msg
}

func (r *Request[_, _]) Log() logrus.FieldLogger {
	return r.logger
}

func (r *Request[_, _]) Run() Run {
	return r.run
}

func (r *Request[_, _]) withLogger(logger logrus.FieldLogger) {
	r.logger = logger
}

func (r *Request[_, _]) withTransition(name string, version ulid.ULID) {
	r.logger = r.logger.WithFields(logrus.Fields{
		"transition":         name,
		"transition_version": version,
	})
	r.run.TransitionVersion = version
	r.run.CurrentState = name
}

func (r *Request[_, _]) withError(err RunErr) {
	r.run.fsmErr = err
}

// NewRequest creates a new request to be used for starting a FSM.
func NewRequest[R, W any](msg *R, w *W) *Request[R, W] {
	return &Request[R, W]{
		Msg: msg,
		W:   *NewResponse[W](w),
	}
}

type AnyRequest interface {
	Any() any

	Log() logrus.FieldLogger

	Run() Run

	withLogger(logrus.FieldLogger)

	withTransition(string, ulid.ULID)

	withError(RunErr)
}

// MockRequest takes an fsm request and customizes it with logger and run
// objects provided by the caller.
// Note: this should probably be deprecated once better test helpers for
// executing a transition are introduced
func MockRequest[R, W any](req *Request[R, W], logger logrus.FieldLogger, run Run) *Request[R, W] {
	return &Request[R, W]{
		Msg:    req.Msg,
		W:      req.W,
		logger: logger,
		run:    run,
	}
}

type Response[W any] struct {
	Msg *W
}

func (r *Response[_]) Any() any {
	if r == nil {
		return nil
	}
	return r.Msg
}

func (r *Response[_]) internalOnly() {}

func NewResponse[W any](msg *W) *Response[W] {
	return &Response[W]{
		Msg: msg,
	}
}

type AnyResponse interface {
	Any() any

	internalOnly()
}

type RunErr struct {
	Err error

	State string
}

// Run contains the information associated with an active FSM.
type Run struct {
	StartVersion ulid.ULID

	TransitionVersion ulid.ULID

	ID string

	Action string

	CurrentState string

	ResourceName string

	TypeName string

	Queue string

	Parent ulid.ULID

	// fsmErr is the error and originating state that caused the FSM to stop executing transitions.
	fsmErr RunErr
}

type fsm struct {
	action string

	typeName, alias string

	rCodec, wCodec Codec

	startState, endState string

	queue string

	parent ulid.ULID

	initializers []InitializerFunc

	transitions *immutable.List[string]

	// transitions is used to lookup a transition by key in order to execute it.
	registeredTransitions map[transitionKey]*transition
}

func (f *fsm) transitionSlice() []string {
	names := make([]string, 0, f.transitions.Len())
	itr := f.transitions.Iterator()
	for !itr.Done() {
		_, value := itr.Next()
		names = append(names, value)
	}
	return names
}

type transitionKey struct {
	action string

	typeName string

	name string
}

type transition struct {
	name string

	impl TransitionFunc
}

type TransitionFunc func(context.Context, AnyRequest) (AnyResponse, error)

// Attributable is an interface that can be implemented by a request to include additional Span
// attributes.
type Attributable interface {
	Attributes() []attribute.KeyValue
}

func newTransition[R, W any](name string, transitionFn func(context.Context, *Request[R, W]) (*Response[W], error), cfg TransitionConfig[R, W]) *transition {
	// Wrap the strongly-typed implementation so we can apply interceptors.
	untyped := TransitionFunc(func(ctx context.Context, request AnyRequest) (AnyResponse, error) {
		if context.Cause(ctx) == context.Canceled {
			return nil, ctx.Err()
		}
		typed, ok := request.(*Request[R, W])
		if !ok {
			return nil, fmt.Errorf("unexpected handler request type %T", request)
		}
		res, err := transitionFn(ctx, typed)
		if res != nil {
			typed.W = *res
		}
		return res, err
	})

	if interceptors := cfg.interceptors; interceptors != nil {
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			untyped = interceptor(untyped)
		}
	}

	return &transition{
		name: name,
		impl: untyped,
	}
}

type InitializerFunc func(context.Context, AnyRequest) context.Context

func newInitializer[R, W any](initFn func(context.Context, *Request[R, W]) context.Context) InitializerFunc {
	return InitializerFunc(func(ctx context.Context, request AnyRequest) context.Context {
		typed, ok := request.(*Request[R, W])
		if !ok {
			return ctx
		}
		return initFn(ctx, typed)
	})
}

type FinalizerFunc func(context.Context, AnyRequest, RunErr)

func newFinalizer[R, W any](finalFn func(context.Context, *Request[R, W], RunErr)) FinalizerFunc {
	return FinalizerFunc(func(ctx context.Context, request AnyRequest, err RunErr) {
		typedReq, ok := request.(*Request[R, W])
		if !ok {
			return
		}
		finalFn(ctx, typedReq, err)
	})
}

type runState struct {
	Run

	State fsmv1.RunState

	Error RunErr
}

func resume[R, W any](m *Manager, f *fsm) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		clearRun := func(run Run) {
			txn := m.db.Txn(true)
			defer txn.Abort()

			rs := runState{
				Run:   run,
				State: fsmv1.RunState_RUN_STATE_PENDING,
			}
			if err := txn.Delete(fsmTable, rs); err != nil {
				m.logger.WithError(err).Error("failed to update fsm state store")
			}
			txn.Commit()
			return
		}

		resources, err := m.store.Active(ctx, f)
		if err != nil {
			return err
		}

		for _, resource := range resources {
			r := Run{
				ID:           resource.active.GetResourceId(),
				StartVersion: resource.version,
				Action:       f.action,
				ResourceName: f.alias,
				TypeName:     f.typeName,
				Queue:        f.queue,
				Parent:       f.parent,
				fsmErr:       resource.fsmError,
			}

			var req R
			if err := f.rCodec.Unmarshal(resource.active.Resource, &req); err != nil {
				m.logger.WithError(err).Error("failed to unmarshal resource, unable to resume")
				defer clearRun(r)
				return err
			}

			var w W
			if resource.response != nil {
				if err := f.wCodec.Unmarshal(resource.response, &w); err != nil {
					m.logger.WithField("response_bytes", string(resource.response)).WithError(err).Error("failed to unmarshal response, unable to resume")
					defer clearRun(r)
					return err
				}
			}
			m.logger.WithField("completed", resource.completedTransitions).Debug("pruning completed transitions")

			remainingTransitions := immutable.NewList[*transition]()
			for _, name := range resource.active.Transitions {
				if !slices.Contains(resource.completedTransitions, name) {
					transition, ok := f.registeredTransitions[transitionKey{
						action:   f.action,
						typeName: f.typeName,
						name:     name,
					}]
					if !ok {
						m.logger.Warn("transition did not exist")
						transition = newTransition(name, noOp[R, W], TransitionConfig[R, W]{
							interceptors: []TransitionInterceptorFunc{
								skipper(),
								canceller(m.store, f.wCodec),
							},
						})
					}
					remainingTransitions = remainingTransitions.Append(transition)
				}
			}

			var startOpt startOptions
			if delayUntil := resource.active.GetOptions().GetDelayUntil(); delayUntil > 0 {
				startOpt.until = time.Unix(delayUntil, 0)
			}

			if runAfter := resource.active.GetOptions().GetRunAfter(); runAfter != nil {
				if err := startOpt.runAfter.UnmarshalText(runAfter); err != nil {
					m.logger.WithError(err).Error("failed to unmarshal run_after")
				}
			}

			f.queue = resource.active.GetOptions().GetQueue()

			if parentBytes := resource.active.GetOptions().GetParent(); parentBytes != nil {
				if err := startOpt.parent.UnmarshalText(parentBytes); err != nil {
					m.logger.WithError(err).Error("failed to unmarshal parent")
				}

				if startOpt.parent.Compare(ulid.ULID{}) != 0 {
					f.parent = startOpt.parent
				}
			}

			ctx := withRetry(ctx, resource.retryCount)
			ctx = withRestart(ctx, true)

			ctx = (propagation.TraceContext{}).Extract(ctx, propagation.MapCarrier(resource.active.TraceContext))

			runner := runnerFromOpts(&startOpt, m)

			request := NewRequest[R, W](&req, &w)
			request.run = r

			run(ctx, request, m, runner, &runInstance{initializers: f.initializers, transitions: remainingTransitions})
		}
		return nil
	}
}

type StartOptionsFn func(*startOptions)

type startOptions struct {
	until time.Time

	runAfter ulid.ULID

	queue string

	parent ulid.ULID
}

// WithDelayedStart will delay the start of the FSM until the provided time.
func WithDelayedStart(until time.Time) StartOptionsFn {
	return func(opts *startOptions) {
		opts.until = until
	}
}

// WithRunAfter will delay the start of the FSM until the FSM with the version has completed.
func WithRunAfter(version ulid.ULID) StartOptionsFn {
	return func(opts *startOptions) {
		opts.runAfter = version
	}
}

// WithQueue will attempt to run the FSM if there is capacity in the queue, otherwise it will queue
// the FSM to be run until capacity is available.
func WithQueue(queue string) StartOptionsFn {
	return func(opts *startOptions) {
		opts.queue = queue
	}
}

func WithParent(parent ulid.ULID) StartOptionsFn {
	return func(opts *startOptions) {
		opts.parent = parent
	}
}

// start attempts to start the FSM using the provided id and request. The id is used to uniquely
// identify the FSM associated with the req type along with the action used to register it.
func start[R, W any](m *Manager, f *fsm) func(ctx context.Context, id string, request *Request[R, W], opts ...StartOptionsFn) (ulid.ULID, error) {
	return func(ctx context.Context, id string, request *Request[R, W], opts ...StartOptionsFn) (ulid.ULID, error) {
		var startOpt startOptions
		for _, opt := range opts {
			opt(&startOpt)
		}

		logger := m.logger.WithFields(logrus.Fields{
			"run_id":    id,
			"run_type":  f.typeName,
			"run_alias": f.alias,
		})

		resource, err := f.rCodec.Marshal(request.Msg)
		if err != nil {
			logger.WithError(err).Error("failed to marshal request")
			return ulid.ULID{}, fmt.Errorf("failed to marshal request: %w", err)
		}

		runVersion := ulid.Make()

		ctx = withRestart(ctx, false)
		f.queue = startOpt.queue
		if startOpt.parent.Compare(ulid.ULID{}) != 0 {
			f.parent = startOpt.parent
		}

		r := runnerFromOpts(&startOpt, m)

		request.run = Run{
			ID:           id,
			StartVersion: runVersion,
			Action:       f.action,
			ResourceName: f.alias,
			TypeName:     f.typeName,
			Queue:        f.queue,
			Parent:       f.parent,
		}

		transitions := immutable.NewList[*transition]()
		iter := f.transitions.Iterator()
		for !iter.Done() {
			_, value := iter.Next()
			transitions = transitions.Append(f.registeredTransitions[transitionKey{
				action:   f.action,
				typeName: f.typeName,
				name:     value,
			}])
		}
		_, err = m.store.Append(ctx,
			request.run,
			&fsmv1.StateEvent{
				Type:         fsmv1.EventType_EVENT_TYPE_START,
				Id:           id,
				ResourceType: f.typeName,
				Action:       f.action,
				State:        f.startState,
			},
			startOpt.queue,
			withStartOption(resource, f.transitionSlice()),
			withDelayUntil(startOpt.until),
			withRunAfter(startOpt.runAfter),
			withParent(startOpt.parent),
		)
		if err != nil {
			m.logger.WithError(err).Error("failed to append start event")
			return ulid.ULID{}, err
		}

		run(ctx, request, m, r, &runInstance{initializers: f.initializers, transitions: transitions})

		return runVersion, nil
	}
}

type runInstance struct {
	initializers []InitializerFunc

	transitions *immutable.List[*transition]
}

func run(ctx context.Context, request AnyRequest, m *Manager, r runner, ri *runInstance) {
	// We create a new context that is not cancelable so that we can control the lifecycle of the FSM
	// separately from the context that is passed in.
	ctx = context.WithoutCancel(ctx)

	var (
		run        = request.Run()
		runVersion = run.StartVersion
		id         = run.ID
		action     = run.Action
		alias      = run.ResourceName
		typeName   = run.TypeName
		parent     = run.Parent
	)

	startAttrs := []attribute.KeyValue{
		attribute.String("fsm.action", action),
		attribute.String("fsm.alias", alias),
		attribute.String("fsm.type", typeName),
		attribute.String("fsm.version", runVersion.String()),
		attribute.Int("fsm.sdk_version", 2),
	}
	if attr, ok := request.Any().(Attributable); ok {
		startAttrs = append(startAttrs, attr.Attributes()...)
	}

	startOpts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(startAttrs...),
	}

	// If the FSM does not have a parent, we create a new root span and connect to the caller's span
	// with the link.
	if parent.Compare(ulid.ULID{}) == 0 {
		startOpts = append(startOpts,
			trace.WithNewRoot(),
			trace.WithLinks(trace.LinkFromContext(ctx)),
		)
	}

	ctx, span := m.tracer.Start(ctx, fmt.Sprintf("%s.%s", alias, action), startOpts...)

	logger := m.logger.WithFields(logrus.Fields{
		"run_id":      id,
		"run_type":    typeName,
		"run_alias":   alias,
		"run_version": runVersion.String(),
	})

	runFn := func() {
		ctx, cancel := context.WithCancelCause(ctx)

		m.mu.Lock()
		m.running[runVersion] = cancel
		m.mu.Unlock()

		defer func() {
			span.End()
			m.mu.Lock()
			delete(m.running, runVersion)
			m.mu.Unlock()
			cancel(nil)
		}()

		logger.Info("starting fsm")
		localActionCounterVec := actionCounterVec.MustCurryWith(prometheus.Labels{
			"action":   action,
			"resource": alias,
		})

		actionStartTime := ulid.Time(runVersion.Time())
		localActionDurationVec := actionDurationVec.MustCurryWith(prometheus.Labels{
			"action":   action,
			"resource": alias,
		})

		request.withLogger(logger)
		for _, init := range ri.initializers {
			ctx = init(ctx, request)
		}

		var err error
		iter := ri.transitions.Iterator()
		for !iter.Done() {
			_, transition := iter.Next()
			transitionName := transition.name
			transitionVersion := ulid.Make()
			logger = logger.WithFields(logrus.Fields{
				"transition":         transitionName,
				"transition_version": transitionVersion,
			})
			request.withTransition(transitionName, transitionVersion)

			select {
			case <-ctx.Done():
				switch ctxErr := context.Cause(ctx); {
				case errors.Is(ctxErr, context.Canceled):
					logger.Info("context canceled, fsm shutting down")
					return
				default:
					// continue running through the transitions until we reach the end
				}
			default:
			}

			logger.Info("running transition")

			errc := make(chan error)
			defer close(errc)
			go func() {
				_, implErr := transition.impl(ctx, request)
				errc <- implErr
			}()

			select {
			case <-ctx.Done():
				if context.Cause(ctx) == context.Canceled {
					logger.Debug("context canceled, fsm shutting down")
				}
				err = context.Cause(ctx)
				chanErr := <-errc
				if chanErr != nil {
					err = chanErr
				}
			case err = <-errc:
			}

			var (
				ae *AbortError
				ue *UnrecoverableError
				he *HandoffError
			)
			switch {
			case err == nil:
				continue
			case errors.As(err, &ae):
				localActionCounterVec.WithLabelValues("abort", "").Inc()
				localActionDurationVec.WithLabelValues("abort", "").Observe(time.Since(actionStartTime).Seconds())
				span.SetAttributes(attribute.String("fsm.error_kind", "abort"))
			case errors.As(err, &ue):
				kind := ue.Kind.String()
				localActionCounterVec.WithLabelValues("unrecoverable", kind).Inc()
				localActionDurationVec.WithLabelValues("unrecoverable", "").Observe(time.Since(actionStartTime).Seconds())
				span.SetAttributes(attribute.String("fsm.error_kind", kind))
				logger.WithError(err).Error("reached unrecoverable error, canceling FSM")
			case errors.As(err, &he):
				localActionCounterVec.WithLabelValues("fsm_handoff_error", "").Inc()
				localActionDurationVec.WithLabelValues("fsm_handoff_error", "").Observe(time.Since(actionStartTime).Seconds())
				span.SetAttributes(attribute.String("fsm.error_kind", "handoff"))
			}
			request.withError(RunErr{
				Err:   err,
				State: transitionName,
			})
		}
		if err == nil {
			localActionCounterVec.WithLabelValues("ok", "").Inc()
			localActionDurationVec.WithLabelValues("ok", "").Observe(time.Since(actionStartTime).Seconds())
		}
	}

	m.wg.Add(1)
	ack := make(chan struct{})
	go func() {
		defer m.wg.Done()
		r.Run(ctx, logger, ack, runFn)
	}()

	<-ack
	return
}

func noOp[R, W any](ctx context.Context, req *Request[R, W]) (*Response[W], error) {
	return nil, nil
}
