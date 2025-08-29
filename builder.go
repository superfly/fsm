package fsm

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/benbjohnson/immutable"
	"github.com/iancoleman/strcase"
	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type fsmStart[R, W any] struct {
	transitionStep[R, W]
}

type fsmTransition[R, W any] struct {
	transitionStep[R, W]
}

type fsmEnd[R, W any] struct {
	transitionStep[R, W]
}

type transitionStep[R, W any] struct {
	m *Manager

	f *fsm

	cfg *TransitionConfig[R, W]

	buildError error
}

type nameable interface {
	Name() string
}

// Register creates a new FSM and returns a builder to configure it.
func Register[R, W any](m *Manager, action string) *fsmStart[R, W] {
	var (
		r     R
		w     W
		name  = getType(r)
		alias = strcase.ToSnake(name)
	)

	if nameable, ok := NewRequest(&r, &w).Any().(nameable); ok {
		alias = nameable.Name()
	}

	fs := &fsmStart[R, W]{
		transitionStep: transitionStep[R, W]{
			m: m,
			f: &fsm{
				action:                action,
				typeName:              name,
				alias:                 alias,
				transitions:           immutable.NewList[string](),
				registeredTransitions: map[transitionKey]*transition{},
			},
			cfg: &TransitionConfig[R, W]{
				initializers: []Initializer[R, W]{},
				interceptors: []TransitionInterceptorFunc{},
			},
		},
	}

	rc, err := determineCodec(m.logger, r)
	if err != nil {
		fs.buildError = fmt.Errorf("unable to determine codec for Request type: %w", err)
	}
	fs.f.rCodec = rc

	wc, err := determineCodec(m.logger, w)
	if err != nil {
		fs.buildError = errors.Join(fs.buildError, fmt.Errorf("unable to determine codec for Response type: %w", err))
	}
	fs.f.wCodec = wc

	return fs
}

func getType(myvar any) string {
	t := reflect.TypeOf(myvar)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

type TransitionConfig[R, W any] struct {
	// initializers can only be configured when calling Start.
	initializers []Initializer[R, W]

	// interceptors can be configured when calling Start or To.
	interceptors []TransitionInterceptorFunc

	// finalizers can only be configured when calling End.
	finalizers []Finalizer[R, W]
}

type Option[R, W any] interface {
	apply(*TransitionConfig[R, W]) *TransitionConfig[R, W]
}

type StartOption[R, W any] interface {
	Option[R, W]
	applyStart(*TransitionConfig[R, W]) *TransitionConfig[R, W]
}

type EndOption[R, W any] interface {
	applyEnd(*TransitionConfig[R, W]) *TransitionConfig[R, W]
}

type initializerOption[R, W any] []Initializer[R, W]

func (o initializerOption[R, W]) apply(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	cfg.initializers = append(cfg.initializers, o...)
	return cfg
}

func (o initializerOption[R, W]) applyStart(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	return o.apply(cfg)
}

// Initializer can be used to modify the context.Context provided to transitions as well as modify
// the Request before the transition is executed.
type Initializer[R, W any] func(context.Context, *Request[R, W]) context.Context

// WithInitializers adds the provided initializers to the list of initializers to be executed before
// the first transition is executed.
func WithInitializers[R, W any](i ...Initializer[R, W]) StartOption[R, W] {
	return initializerOption[R, W](i)
}

type TransitionInterceptorFunc func(TransitionFunc) TransitionFunc

type interceptorOption[R, W any] []TransitionInterceptorFunc

func (o interceptorOption[R, W]) apply(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	cfg.interceptors = append(cfg.interceptors, o...)
	return cfg
}

func (o interceptorOption[R, W]) applyStart(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	return o.apply(cfg)
}

// WithInterceptors adds the provided TransitionInterceptorFunc to the list of interceptors to
// be executed with the transition and should take care to call the next function in the chain.
func WithInterceptors[R, W any](i ...TransitionInterceptorFunc) StartOption[R, W] {
	return interceptorOption[R, W](i)
}

type finalizerOption[R, W any] []Finalizer[R, W]

func (o finalizerOption[R, W]) applyEnd(cfg *TransitionConfig[R, W]) *TransitionConfig[R, W] {
	cfg.finalizers = append(cfg.finalizers, o...)
	return cfg
}

type Finalizer[R, W any] func(context.Context, *Request[R, W], RunErr)

// WithFinalizers adds the provided Finalizers to the list of finalizers to be executed when the FSM
// has completed.
func WithFinalizers[R, W any](f ...Finalizer[R, W]) EndOption[R, W] {
	return finalizerOption[R, W](f)
}

type Transition[R, W any] func(context.Context, *Request[R, W]) (*Response[W], error)

// Starts sets the initial state of the FSM and applies any options to the transition.
func (s *fsmStart[R, W]) Start(name string, transition Transition[R, W], startOpts ...StartOption[R, W]) *fsmTransition[R, W] {
	s.f.startState = name

	opts := make([]Option[R, W], 0, len(startOpts)+1)
	opts = append(opts, WithInitializers[R, W](setStarted[R, W](s.m.db)))
	for _, o := range startOpts {
		opts = append(opts, o)
	}

	return (&fsmTransition[R, W]{s.transitionStep}).To(name, transition, opts...)
}

// To sets the next state of the FSM and applies any options to the transition.
func (s *fsmTransition[R, W]) To(name string, transition Transition[R, W], opts ...Option[R, W]) *fsmTransition[R, W] {
	tk := transitionKey{
		action:   s.f.action,
		typeName: s.f.typeName,
		name:     name,
	}
	if _, ok := s.f.registeredTransitions[tk]; ok {
		s.m.logger.WithField("transition", name).Error("transition already registered")
		s.buildError = errors.Join(s.buildError, fmt.Errorf("transition %s already registered", name))
		return &fsmTransition[R, W]{s.transitionStep}
	}

	s.cfg.interceptors = []TransitionInterceptorFunc{
		skipper(),
		canceller(s.m.store, s.f.wCodec),
		retry(s.m.tracer, s.m.store),
	}

	for _, o := range opts {
		o.apply(s.cfg)
	}

	s.f.initializers = make([]InitializerFunc, 0, len(s.cfg.initializers))
	for _, i := range s.cfg.initializers {
		s.f.initializers = append(s.f.initializers, newInitializer(i))
	}

	s.f.registeredTransitions[tk] = newTransition(name, transition, *s.cfg)
	s.f.transitions = s.f.transitions.Append(name)

	return &fsmTransition[R, W]{s.transitionStep}
}

// End sets the final state of the FSM and applies any options as a global option for the FSM.
func (s *fsmTransition[R, W]) End(name string, opts ...EndOption[R, W]) *fsmEnd[R, W] {
	fk := fsmKey{
		name:   s.f.typeName,
		action: s.f.action,
	}

	if _, ok := s.m.fsms[fk]; ok {
		s.m.logger.WithField("fsm", s.f.typeName).Error("fsm already registered")
		s.buildError = errors.Join(s.buildError, fmt.Errorf("fsm %s:%s already registered", s.f.typeName, s.f.action))
		return &fsmEnd[R, W]{s.transitionStep}
	}

	tk := transitionKey{
		action:   s.f.action,
		typeName: s.f.typeName,
		name:     name,
	}
	if _, ok := s.f.registeredTransitions[tk]; ok {
		s.m.logger.WithField("transition", name).Error("transition already registered")
		s.buildError = errors.Join(s.buildError, fmt.Errorf("transition %s already registered", name))
		return &fsmEnd[R, W]{s.transitionStep}
	}

	cfg := TransitionConfig[R, W]{
		interceptors: []TransitionInterceptorFunc{
			retry(s.m.tracer, s.m.store),
		},
	}
	for _, opt := range opts {
		opt.applyEnd(&cfg)
	}

	finalizers := make([]FinalizerFunc, 0, len(cfg.finalizers))
	for _, f := range cfg.finalizers {
		finalizers = append(finalizers, newFinalizer(f))
	}

	s.f.registeredTransitions[tk] = newTransition(name, finisher[R, W](s.m, finalizers), cfg)
	s.f.transitions = s.f.transitions.Append(name)
	s.f.endState = name

	s.m.fsms[fk] = s.f

	return &fsmEnd[R, W]{s.transitionStep}
}

type Start[R, W any] func(ctx context.Context, id string, req *Request[R, W], opts ...StartOptionsFn) (ulid.ULID, error)

type Resume func(context.Context) error

// Build returns a function that can be used to run the FSM as well as resume any previously
// started runs.
func (s *fsmEnd[R, W]) Build(ctx context.Context) (Start[R, W], Resume, error) {
	if s.buildError != nil {
		return nil, nil, s.buildError
	}

	wrappedResume := func(ctx context.Context) error {
		if err := resume[R, W](s.m, s.f)(ctx); err != nil {
			return fmt.Errorf("failed to resume active FSMs: %w", err)
		}
		return nil
	}

	return start[R, W](s.m, s.f), wrappedResume, nil
}

func determineCodec(logger logrus.FieldLogger, req any) (Codec, error) {
	if codec, ok := req.(Codec); ok {
		logger.Info("using provided codec")
		return codec, nil
	}

	if _, ok := req.(proto.Message); ok {
		logger.Info("using proto codec")
		return &protoBinaryCodec{}, nil
	}

	codec := &jsonCodec{}
	b, err := codec.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("no codec provided and could not use json codec for %T: %w", req, err)
	}

	var req2 any
	if err := codec.Unmarshal(b, &req2); err != nil {
		return nil, fmt.Errorf("no codec provided and could not use json codec for %T: %w", req, err)
	}
	logger.Info("using json codec")

	return &jsonCodec{}, nil
}
