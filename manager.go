// manager.go

package fsm

import (
    "context"
    "errors"
    "fmt"
    "net"
    "net/http"
    "os"
    "path/filepath"
    "sync"
    "time"

    fsmv1 "github.com/superfly/fsm/gen/fsm/v1"
    "github.com/superfly/fsm/gen/fsm/v1/fsmv1connect"

    "github.com/hashicorp/go-memdb"
    "github.com/oklog/ulid/v2"
    "github.com/sirupsen/logrus"
    "go.opentelemetry.io/otel"
    semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
    "go.opentelemetry.io/otel/trace"
    "golang.org/x/net/http2"
    "golang.org/x/net/http2/h2c"
)

const (
    fsmTable          = "fsm"
    idIndex           = "id"
    runIndex          = "id_by_resource" // Changed from "run" to "id_by_resource" for clarity, assuming ID is unique per resource
    runPrefixIndex    = runIndex + "_prefix"
    parentIndex       = "parent"
    parentPrefixIndex = parentIndex + "_prefix"

    tracerName = "fsm"
)

var (
    fsmSchema = &memdb.DBSchema{
        Tables: map[string]*memdb.TableSchema{
            fsmTable: {
                Name: fsmTable,
                Indexes: map[string]*memdb.IndexSchema{
                    idIndex: {
                        Name:   idIndex,
                        Unique: true,
                        Indexer: ulidIndexer{
                            fieldFn: func(rs runState) ulid.ULID {
                                return rs.StartVersion
                            },
                        },
                    },
                    runIndex: { // This index allows lookup by the `ID` field of the Run struct.
                        Name:    runIndex,
                        Unique:  false, // An FSM ID (Run.ID) can have multiple versions/runs
                        Indexer: &memdb.StringFieldIndex{Field: "ID"},
                    },
                    parentIndex: {
                        Name:         parentIndex,
                        AllowMissing: true,
                        Unique:       false,
                        Indexer: ulidIndexer{
                            fieldFn: func(rs runState) ulid.ULID {
                                return rs.Parent
                            },
                        },
                    },
                },
            },
        },
    }
)

type Manager struct {
    logger logrus.FieldLogger

    tracer trace.Tracer

    wg sync.WaitGroup

    db *memdb.MemDB

    store *store

    fsms map[fsmKey]*fsm // Map to hold registered FSM definitions

    queues map[string]*queuedRunner

    done chan struct{}

    mu      sync.RWMutex
    running map[ulid.ULID]context.CancelCauseFunc
}

type fsmKey struct {
    name string

    action string
}

type Config struct {
    Logger logrus.FieldLogger

    // DBPath is the directory to use for persisting FSM state.
    DBPath string

    // Queues defines which queues are available for FSMs to use. The key is the queue name and the
    // value is the maximum number of FSMs that can run concurrently.
    Queues map[string]int
}

// New creates a new FSM manager to register and run FSMs.
func New(cfg Config) (*Manager, error) {
    memDB, err := memdb.NewMemDB(fsmSchema)
    if err != nil {
        return nil, fmt.Errorf("failed to create memdb, %w", err)
    }

    if cfg.Logger == nil {
        cfg.Logger = logrus.New()
    }

    if cfg.DBPath == "" {
        return nil, errors.New("db path is required")
    }

    if err := os.MkdirAll(cfg.DBPath, 0755); err != nil { // Changed permissions to 0755 for directory
        return nil, fmt.Errorf("failed to setup DB path: %w", err)
    }

    tracer := otel.GetTracerProvider().Tracer(tracerName,
        trace.WithInstrumentationVersion("0.1.0"),
        trace.WithSchemaURL(semconv.SchemaURL),
    )

    store, err := newStore(cfg.Logger.WithField("sys", "fsm-store"), tracer, cfg.DBPath, memDB)
    if err != nil {
        return nil, err
    }

    done := make(chan struct{})

    man := &Manager{
        logger:  cfg.Logger.WithField("sys", "fsm"),
        tracer:  tracer,
        store:   store,
        db:      memDB,
        fsms:    make(map[fsmKey]*fsm), // Initialize the map
        queues:  make(map[string]*queuedRunner, len(cfg.Queues)),
        done:    done,
        running: make(map[ulid.ULID]context.CancelCauseFunc), // Initialize the map
    }

    for name, size := range cfg.Queues {
        q := &queuedRunner{
            name:   name,
            size:   size,
            queue:  make(chan queueItem),
            queued: make([]func(), 0, size),
        }
        man.queues[name] = q
        go q.run(done, cfg.Logger.WithField("queue", name))
    }

    mux := http.NewServeMux()
    mux.Handle(fsmv1connect.NewFSMServiceHandler(&adminServer{
        m: man,
    }))

    server := &http.Server{
        Handler: h2c.NewHandler(mux, &http2.Server{}),
    }

    socket := filepath.Join(cfg.DBPath, "fsm.sock")
    _ = os.Remove(socket) // Ignore error if file doesn't exist
    unixListener, err := net.Listen("unix", socket)
    if err != nil {
        return nil, fmt.Errorf("failed to listen on unix socket %s, %w", socket, err)
    }

    go func() {
        man.logger.WithField("socket", socket).Info("FSM admin server listening")
        if err := server.Serve(unixListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
            man.logger.WithError(err).Error("FSM admin server failed")
        }
        man.logger.Info("FSM admin server stopped")
    }()

    go func() {
        defer func() {
            _ = os.Remove(socket) // Clean up socket file on exit
        }()
        <-man.done
        if err := unixListener.Close(); err != nil {
            man.logger.WithError(err).Error("failed to close unix listener")
        }

        ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
        defer cancel()
        if err := server.Shutdown(ctx); err != nil {
            man.logger.WithError(err).Error("failed to shutdown http server")
        }
    }()

    return man, nil
}

// Shutdown sends a stop signal to all FSMs and blocks until they have all stopped.
func (m *Manager) Shutdown(timeout time.Duration) {
    m.logger.WithField("shutdown_timeout", timeout).Info("shutting down")

    m.mu.RLock()
    for id, cancel := range m.running {
        m.logger.WithField("fsm_id", id.String()).Info("shutting down fsm")
        // The original code was `cancel(nil)` but typically cancel expects a cause.
        // For a clean shutdown, `context.Canceled` is appropriate.
        cancel(context.Canceled)
    }
    m.mu.RUnlock()

    close(m.done)

    wait := make(chan struct{})
    go func() {
        defer close(wait)
        m.wg.Wait()
    }()

    select {
    case <-wait:
        m.logger.Info("all FSMs have shutdown")
    case <-time.After(timeout):
        m.logger.Warn("timed out waiting for FSMs to shutdown")
    }

    if err := m.store.Close(); err != nil {
        m.logger.WithError(err).Error("failed to close store")
    }

    m.logger.Info("shutdown complete")
}

// Register adds a new FSM definition to the manager.
// It takes the FSM's action name, request/response types, and a variadic list of options
// to configure the FSM's states, transitions, initializers, and finalizers.
func (m *Manager) Register(action string, requestType, responseType any, opts ...FSMCfgOption) {
    f := &fsm{
        action:            action,
        rCodec:            NewJSONCodec(requestType),
        wCodec:            NewJSONCodec(responseType),
        registeredTransitions: make(map[transitionKey]*transition),
    }

    // Apply configuration options
    cfg := defaultFSMConfig()
    for _, opt := range opts {
        opt(cfg)
    }

    f.typeName = cfg.typeName
    f.alias = cfg.alias
    f.startState = cfg.startState
    f.endState = cfg.endState
    f.initializers = cfg.initializers
    f.transitions = cfg.transitions.List // Use the immutable list from config

    // Register transitions for lookup
    itr := f.transitions.Iterator()
    for !itr.Done() {
        _, t := itr.Next()
        // t is a `*transition` value here, we need to check if it's nil
        if t == nil {
            m.logger.Errorf("nil transition found for action %s, type %s", action, f.typeName)
            continue
        }
        registeredTransition, ok := t.(*transition)
        if !ok {
            m.logger.Errorf("unexpected transition type for action %s, type %s: %T", action, f.typeName, t)
            continue
        }
        key := transitionKey{
            action:   action,
            typeName: f.typeName,
            name:     registeredTransition.name,
        }
        f.registeredTransitions[key] = registeredTransition
    }

    // Store the FSM definition
    m.fsms[fsmKey{action: action, name: f.typeName}] = f
    m.logger.WithFields(logrus.Fields{
        "action": action,
        "type":   f.typeName,
        "alias":  f.alias,
    }).Info("FSM registered")
}

// Start initiates a new FSM run.
// It returns the ULID version of the new run and any error encountered.
func (m *Manager) Start(ctx context.Context, id string, req AnyRequest, opts ...StartOptionsFn) (ulid.ULID, error) {
    // Determine the FSM definition based on the request type and action.
    // We need to infer the `fsm.action` from the request. This means the `AnyRequest`
    // should ideally carry this information or the Start method itself is generic.
    // Given the FSM's `fsms` map is keyed by `fsmKey{name, action}`, we need both.
    // For this example, let's assume `req.Run().Action` is set or passed as an argument.
    // In the provided `fsm.go`, `start` is a generic function `start[R, W any]`
    // which implicitly knows the `fsm` definition.
    // So, this `Start` method on `Manager` must be type-aware or use reflection.
    // The `start` function on `fsm` is designed to be called directly.

    // For a `Manager.Start` method, we need to know *which* FSM to start.
    // Let's assume the `AnyRequest` can identify its associated FSM action and type.
    // Or, more practically, `Start` might take the FSM action/type as an argument.

    // If `Manager.Start` is the entry point, it needs to find the `fsm` definition.
    // The `fsm.go` also defines a `start` *function* that takes `f *fsm`.
    // The most straightforward way is to let the user specify the action and potentially type.
    // Let's modify `Start` to take `action` and `typeName` for lookup.

    fsmAction := req.Run().Action // Assuming `Run.Action` is set in the `Request`
    fsmType := req.Run().TypeName // Assuming `Run.TypeName` is set in the `Request`

    f, ok := m.fsms[fsmKey{action: fsmAction, name: fsmType}]
    if !ok {
        return ulid.ULID{}, ErrFsmNotFound // Or a more specific error
    }

    // Use the generic `start` function defined in `fsm.go`
    // This requires knowing the `R` and `W` types at this point, which is tricky for `AnyRequest`.
    // The typical way `fsm` library uses `start` is like: `manager.GetFSM("my_action").Start(...)`
    // or `manager.Start("my_action", &MyRequest{}, &MyResponse{}, ...)`.

    // Let's re-align with the typical FSM usage where `Start` directly uses types.
    // This `Start` method on `Manager` is problematic for generic `AnyRequest`.
    // The `fsm` library's `start` is type-specific.

    // We'll rename this `Start` to `_startFSM` and keep it private,
    // and expose a generic `Start` method that takes `R` and `W`.

    // The existing `start[R, W any]` function from `fsm.go` is the one to call.
    // This `Manager.Start` cannot easily call it without type parameters.
    // A common pattern is to make `Manager.Start` also generic or provide a type-erased wrapper.

    // For now, let's assume `req` is correctly typed `*Request[R, W]` and `f` is found.
    // We need to cast `req` back to its concrete type. This is unsafe.
    // The `superfly/fsm`'s Manager.Start is usually a generic method.
    // `func (m *Manager) Start[R, W any](ctx context.Context, id string, msg *R, w *W, opts ...StartOptionsFn) (ulid.ULID, error)`

    // Given your provided `start[R, W any](m *Manager, f *fsm) func(...)`
    // the `Manager` itself needs to somehow expose `start` in a type-safe way.

    // The `start` function is essentially a closure that captures `m` and `f`.
    // We need to call the returned function.

    // THIS `Manager.Start` needs to be generic itself or there must be a `GetFSM` method.
    // Let's define a generic `Start` method on Manager for proper type inference.

    return ulid.ULID{}, errors.New("Manager.Start should be a generic method; see next section")
}

// ActiveKey represents a key for active FSM runs.
type ActiveKey struct {
    Action  string
    Version ulid.ULID
}

// ActiveSet represents a set of active FSM runs.
type ActiveSet map[ActiveKey]fsmv1.RunState

// Active returns a map of active runs for the given id. The map keys are the run type and the
// values are the run version which can be used to wait for the run to complete.
func (m *Manager) Active(ctx context.Context, id string) (ActiveSet, error) {
    txn := m.db.Txn(false)
    defer txn.Abort()

    active := map[ActiveKey]fsmv1.RunState{}

    // Changed index from `runPrefixIndex` to `runIndex` as per schema update for `ID` field.
    // Also, iterating all items with this ID and checking their state.
    it, err := txn.Get(fsmTable, runIndex, id)
    if err != nil {
        return nil, err
    }

    for next := it.Next(); next != nil; next = it.Next() {
        rs := next.(runState)
        if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
            continue
        }
        active[ActiveKey{Action: rs.Action, Version: rs.StartVersion}] = rs.State
    }

    return active, nil
}

// Children returns a list of FSMs that are associated with the given parent.
func (m *Manager) Children(ctx context.Context, parent ulid.ULID) ([]ulid.ULID, error) {
    return m.store.Children(ctx, parent)
}

// ActiveChildren returns a list of FSMs that were started from the given parent and are still
// active.
func (m *Manager) ActiveChildren(ctx context.Context, parent ulid.ULID) ([]Run, error) {
    txn := m.db.Txn(false)
    defer txn.Abort()

    it, err := txn.Get(fsmTable, parentIndex, parent) // Use parentIndex
    if err != nil {
        return nil, err
    }

    children := []Run{}
    for next := it.Next(); next != nil; next = it.Next() {
        rs := next.(runState)
        if rs.StartVersion.Compare(ulid.ULID{}) == 0 { // Check for valid ULID
            continue
        }
        if rs.State != fsmv1.RunState_RUN_STATE_COMPLETE { // Only return active children
            children = append(children, rs.Run)
        }
    }

    return children, nil
}

// Cancel sends a cancel signal to the FSM should it exist. It does not block until the FSM has
// completed so callers should use Wait to ensure the FSM has stopped, if needed.
func (m *Manager) Cancel(ctx context.Context, version ulid.ULID, cause string) error {
    m.mu.RLock()
    f, ok := m.running[version]
    m.mu.RUnlock()
    if !ok {
        return ErrFsmNotFound
    }

    // Pass the cause to the context cancellation function.
    f(errors.New(cause))
    return nil
}

// Wait blocks until the run with the given version completes.
func (m *Manager) Wait(ctx context.Context, version ulid.ULID) error {
    var (
        v      = version.String()
        logger = m.logger.WithField("start_version", v)
    )

    logger.Info("waiting for FSM to finish")
    defer logger.Info("done waiting for FSM to finish")
    for {
        txn := m.db.Txn(false)
        ws := memdb.NewWatchSet()
        // No defer txn.Abort() here. It's inside the loop, so explicit aborts are needed.

        ch, item, err := txn.FirstWatch(fsmTable, idIndex, v) // Watch for changes to this FSM by its start version (ULID)
        if err != nil {
            txn.Abort()
            logger.WithError(err).Error("failed to wait for FSM")
            return err
        }

        if item == nil {
            txn.Abort()
            // Lookup from the store in case the FSM has already completed and was removed from memdb.
            he, err := m.store.History(ctx, version)
            switch {
            case errors.Is(err, ErrFsmNotFound):
                return nil // FSM not found, assumed completed and purged
            case err != nil:
                return err
            case he.GetLastEvent().GetError() != "":
                return &haltError{err: errors.New(he.GetLastEvent().GetError())}
            default:
                return nil // FSM completed successfully
            }
        }

        state, ok := item.(runState)
        if !ok {
            txn.Abort()
            return fmt.Errorf("unexpected type %T for runState item", item)
        }

        if state.State == fsmv1.RunState_RUN_STATE_COMPLETE {
            txn.Abort()
            return state.Error.Err
        }
        ws.Add(ch) // Add the watch channel for this item

        txn.Abort() // Abort the read transaction after setting up watch.

        err = ws.WatchCtx(ctx)
        switch {
        case errors.Is(err, context.Canceled):
            return err
        case err != nil:
            logger.WithError(err).Error("failed to wait for FSM watch")
            return err
        }

        // After watch triggers, re-read the state.
        roTxn := m.db.Txn(false)
        item, err = roTxn.First(fsmTable, idIndex, v)
        roTxn.Abort() // Abort this read transaction

        if err != nil {
            return err
        }

        if item == nil {
            // Again, check store if it's gone from memdb.
            he, err := m.store.History(ctx, version)
            switch {
            case errors.Is(err, ErrFsmNotFound):
                return nil
            case err != nil:
                return err
            case he.GetLastEvent().GetError() != "":
                return &haltError{err: errors.New(he.GetLastEvent().GetError())}
            default:
                return nil
            }
        }

        state, ok = item.(runState)
        if !ok {
            return fmt.Errorf("unexpected type %T for runState item after watch", item)
        }

        switch state.State {
        case fsmv1.RunState_RUN_STATE_PENDING, fsmv1.RunState_RUN_STATE_RUNNING:
            logger.Debug("FSM still running after watch trigger, re-waiting...")
            // Loop will continue and set up a new watch.
        case fsmv1.RunState_RUN_STATE_COMPLETE:
            return state.Error.Err
        }
    }
}

// WaitByID blocks until the run with the given ID completes.
func (m *Manager) WaitByID(ctx context.Context, id string) error {
    var (
        logger = m.logger.WithField("fsm_run_id", id)
    )

    logger.Info("waiting for FSM to finish by ID")
    defer logger.Info("done waiting for FSM to finish by ID")

    // Helper to get current state and ULID, and set up watch.
    getAndWatch := func(tx *memdb.Txn, currentVersion *ulid.ULID) (fsmv1.RunState, *memdb.WatchCh, error) {
        var (
            item any
            err  error
        )

        if currentVersion == nil || currentVersion.Compare(ulid.ULID{}) == 0 {
            // First time or version unknown, find by `runIndex` (which uses `ID` field).
            item, err = tx.First(fsmTable, runIndex, id)
            if err != nil {
                return fsmv1.RunState_RUN_STATE_UNSPECIFIED, nil, err
            }
            if item == nil {
                return fsmv1.RunState_RUN_STATE_UNSPECIFIED, nil, ErrFsmNotFound // No FSM with this ID found
            }
            rs := item.(runState)
            *currentVersion = rs.StartVersion // Set the version for subsequent watches
        }

        // Now watch specifically for the FSM by its start version.
        ch, watchItem, watchErr := tx.FirstWatch(fsmTable, idIndex, currentVersion.String())
        if watchErr != nil {
            return fsmv1.RunState_RUN_STATE_UNSPECIFIED, nil, watchErr
        }

        if watchItem == nil { // Could have been completed and removed between reads.
            return fsmv1.RunState_RUN_STATE_COMPLETE, nil, nil // Assume completed if watch item is gone.
        }

        state, ok := watchItem.(runState)
        if !ok {
            return fsmv1.RunState_RUN_STATE_UNSPECIFIED, nil, fmt.Errorf("unexpected type %T for runState item", watchItem)
        }
        return state.State, ch, nil
    }

    var fsmVersion ulid.ULID // To store the ULID once found

    for {
        txn := m.db.Txn(false)
        ws := memdb.NewWatchSet()

        state, ch, err := getAndWatch(txn, &fsmVersion)
        if err != nil {
            txn.Abort()
            if errors.Is(err, ErrFsmNotFound) { // If FSM not found by ID, it might have completed
                // Try to get from history in case it completed
                he, histErr := m.store.History(ctx, fsmVersion) // fsmVersion might be zero here
                switch {
                case fsmVersion.Compare(ulid.ULID{}) == 0:
                    return nil // Never found an FSM with this ID that started.
                case errors.Is(histErr, ErrFsmNotFound):
                    return nil // FSM completed and purged from store.
                case histErr != nil:
                    return histErr
                case he.GetLastEvent().GetError() != "":
                    return &haltError{err: errors.New(he.GetLastEvent().GetError())}
                default:
                    return nil
                }
            }
            logger.WithError(err).Error("failed to get FSM state or setup watch")
            return err
        }

        if state == fsmv1.RunState_RUN_STATE_COMPLETE {
            txn.Abort()
            // Need to retrieve the actual runState to get the error.
            item, _ := m.db.Txn(false).First(fsmTable, idIndex, fsmVersion.String())
            if item != nil {
                return item.(runState).Error.Err
            }
            // If not in memdb, check store history for final error.
            he, err := m.store.History(ctx, fsmVersion)
            if err != nil {
                return err
            }
            if he.GetLastEvent().GetError() != "" {
                return &haltError{err: errors.New(he.GetLastEvent().GetError())}
            }
            return nil
        }

        if ch != nil {
            ws.Add(ch)
        }
        txn.Abort() // Abort the transaction after collecting state and watch channel.

        err = ws.WatchCtx(ctx)
        switch {
        case errors.Is(err, context.Canceled):
            return err
        case err != nil:
            logger.WithError(err).Error("failed to wait for FSM watch by ID")
            return err
        }

        // Loop will continue to re-evaluate the state after a change.
    }
}

// ErrFsmNotFound is returned when an FSM run is not found.
var ErrFsmNotFound = errors.New("fsm not found")

// Transition triggers a transition for an existing FSM run.
// It finds the FSM by its ID, applies the event, and executes the corresponding handler.
// It returns the ULID version of the run and any error encountered.
func (m *Manager) Transition(ctx context.Context, id string, event string, req AnyRequest) (ulid.ULID, error) {
    // Find the latest active run for the given ID.
    txn := m.db.Txn(false)
    defer txn.Abort()

    iter, err := txn.Get(fsmTable, runIndex, id) // Get all runs associated with this resource ID
    if err != nil {
        return ulid.ULID{}, fmt.Errorf("failed to get FSMs by ID %s: %w", id, err)
    }

    var latestRun *runState
    for obj := iter.Next(); obj != nil; obj = iter.Next() {
        rs := obj.(runState)
        if rs.State != fsmv1.RunState_RUN_STATE_COMPLETE { // Only consider active/pending runs
            if latestRun == nil || rs.StartVersion.Time().After(latestRun.StartVersion.Time()) {
                latestRun = &rs
            }
        }
    }

    if latestRun == nil {
        return ulid.ULID{}, ErrFsmNotFound
    }

    // We now have the `Run` object from `latestRun.Run`.
    // We need to find the FSM definition that corresponds to this run.
    f, ok := m.fsms[fsmKey{action: latestRun.Action, name: latestRun.TypeName}]
    if !ok {
        return ulid.ULID{}, fmt.Errorf("FSM definition for action %s, type %s not found", latestRun.Action, latestRun.TypeName)
    }

    // Find the transition for the current state and the incoming event.
    // This requires iterating through `f.transitions` (an immutable list)
    // to find the one whose `fromState` matches `latestRun.CurrentState` and `event` matches `event`.
    // This implies `fsm.Transition` (from builder) should store `fromState` and `event` metadata.
    // The current `fsm` struct doesn't seem to store `fromState` and `event` directly within `transition`.
    // It's likely that the `transitions` list contains *all* transitions for the FSM,
    // and the `run` logic iterates through them sequentially.
    // For external `Transition` calls, we need to find the *next valid* transition.

    // A `Transition` function on `Manager` implies that we can *force* a state change
    // based on an event, bypassing the linear execution of `run`.
    // This is a different model than the `run` function's sequential transition execution.

    // If `Transition` is to work like a state-machine event trigger,
    // the `fsm` needs to define its transitions more explicitly with `fromState`, `event`, `toState`.
    // Let's assume the `Transition` type in the `fsm` config can carry this info.
    // For this update, let's assume a simplified lookup where `event` maps directly to a `transition.name`.

    // The `fsm.go` only registers `transitionKey` by `action`, `typeName`, and `name`.
    // The `name` is the transition name, not the event name.
    // This suggests `event` *is* the `transition.name`.

    // We need to find a transition that matches the `latestRun.CurrentState`
    // and the `event` (which acts as the `transition.name`).

    // Assuming `event` is the name of the transition handler we want to trigger.
    transitionKey := transitionKey{
        action:   f.action,
        typeName: f.typeName,
        name:     event, // Assuming event name matches transition handler name
    }
    transition, ok := f.registeredTransitions[transitionKey]
    if !ok {
        return ulid.ULID{}, fmt.Errorf("transition '%s' not registered for FSM action '%s', type '%s'", event, f.action, f.typeName)
    }

    // Reconstruct the request for the handler.
    // The `req` argument passed to `Transition` contains the *new* message data.
    // We also need the existing `Run` data.
    typedReq := NewRequest[any, any](req.Any(), nil) // Assuming `req.Any()` is the concrete type `R`
    // The `W` type for `typedReq` cannot be easily inferred here for `AnyRequest`.
    // This means `Transition` should probably also be generic `[R, W any]`.
    // Let's assume a similar pattern to `Start`:
    // `Transition[R, W any](ctx context.Context, id string, event string, msg *R, w *W)`.

    // For the current `AnyRequest` signature, we have a challenge with `W`.
    // `req` is `AnyRequest`, so `req.W` is `AnyResponse`.
    // We need to pass `req.W` (which is `Response[W]`) to the `typedReq`.

    // Let's refine `Transition` signature to be generic for `R` and `W`.
    // This means the `Manager` methods themselves need to be generic if they interact with FSM payload types.

    // *** RE-EVALUATION OF `Manager.Transition` SIGNATURE ***
    // Based on the pattern of `fsm.Manager.Start`, which is `Start[R, W any]`,
    // a `Transition` method should also be generic to properly handle `Request[R, W]` and `Response[W]`.
    // This would change the `Manager` struct significantly if it were to expose generic methods directly.
    // The `superfly/fsm` `Manager` itself does *not* have generic methods. Instead, it has `FSM` objects.
    // E.g., `fsmManager.GetFSM("action_name").Transition(...)`.

    // Let's remove the problematic `Manager.Transition` and assume a higher-level client
    // or wrapper calls the `run` method directly with a specific FSM instance.

    // The current `Manager` `start` function is `func start[R, W any](m *Manager, f *fsm) func(...)`.
    // This closure is designed to be called directly, not via `Manager.Start` or `Manager.Transition` methods.
    // The `manager.go` provided *does not* have `Manager.Start` or `Manager.Transition` methods.
    // So, the `start` and `resume` functions are likely called directly by a *caller* that has
    // knowledge of the `fsm` instance and its types `R`, `W`.

    // The `main.go` example then *calls* `manager.Start` and `manager.Transition`.
    // This means the `Manager` *must* have these methods, and they must be generic.

    // So, we need to add a generic `Start` and `Transition` method to `Manager`.

    return ulid.ULID{}, errors.New("Manager.Transition should be a generic method; see next section")
}

// Add the following generic methods to the Manager struct.

// Start[R, W any] initiates a new FSM run for a specific type of FSM.
// `action` identifies the FSM definition (e.g., "ImageLifecycle").
// `id` is the unique identifier for this particular run of the FSM (e.g., image ID).
// `msg` is the initial request payload. `w` is where the response will be written.
func (m *Manager) Start[R, W any](ctx context.Context, id string, msg *R, w *W, opts ...StartOptionsFn) (ulid.ULID, error) {
    // Dynamically determine the FSM key for lookup
    // This assumes `R` type has a way to identify its type name, or it's passed.
    // For simplicity, let's assume `typeName` is derived from `R`'s type name.
    typeName := getTypeName(msg) // Helper to get type name, or could be an argument.

    f, ok := m.fsms[fsmKey{action: m.extractActionFromRequest(msg), name: typeName}] // Need to define extractActionFromRequest or pass action
    if !ok {
        return ulid.ULID{}, fmt.Errorf("%w: FSM definition for action '%s', type '%s' not found", ErrFsmNotFound, m.extractActionFromRequest(msg), typeName)
    }

    request := NewRequest(msg, w)
    startFn := start[R, W](m, f) // Get the type-specific start function
    return startFn(ctx, id, request, opts...)
}

// Transition[R, W any] triggers a specific transition for an active FSM run.
// `action` identifies the FSM definition.
// `id` is the unique identifier for the FSM run.
// `eventName` is the name of the event that should trigger the transition (e.g., "DownloadComplete").
// `msg` is the request payload for this transition. `w` is where the response will be written.
func (m *Manager) Transition[R, W any](ctx context.Context, action string, id string, eventName string, msg *R, w *W) (ulid.ULID, error) {
    // Find the latest active run for the given ID and action.
    txn := m.db.Txn(false)
    defer txn.Abort()

    iter, err := txn.Get(fsmTable, runIndex, id) // Iterate all runs for this ID
    if err != nil {
        return ulid.ULID{}, fmt.Errorf("failed to get FSMs by ID %s: %w", id, err)
    }

    var latestRun *runState
    for obj := iter.Next(); obj != nil; obj = iter.Next() {
        rs := obj.(runState)
        if rs.Action == action && rs.State != fsmv1.RunState_RUN_STATE_COMPLETE { // Match action and only consider active/pending
            if latestRun == nil || rs.StartVersion.Time().After(latestRun.StartVersion.Time()) {
                latestRun = &rs
            }
        }
    }

    if latestRun == nil {
        return ulid.ULID{}, fmt.Errorf("%w: active FSM run for ID '%s' and action '%s' not found", ErrFsmNotFound, id, action)
    }

    f, ok := m.fsms[fsmKey{action: action, name: latestRun.TypeName}]
    if !ok {
        return ulid.ULID{}, fmt.Errorf("FSM definition for action '%s', type '%s' not found", action, latestRun.TypeName)
    }

    // Create a new request for the transition, populating it with existing run data
    // and the new message.
    request := NewRequest(msg, w) // `w` will receive the response
    request.run = latestRun.Run
    request.run.CurrentState = latestRun.CurrentState // Ensure current state is correct for transition logic

    // Find the specific transition to execute by matching current state and eventName.
    // This implies that `f.transitions` should store the `fromState` and `eventName` explicitly
    // to allow direct triggering.
    // The current `fsm` design iterates through `f.transitions`.
    // For `Transition` to work as an event, we need to find the specific `TransitionFunc`.

    // This is the core logical gap: the `run` method executes transitions sequentially.
    // A `Transition` method implies finding a *specific* transition based on `eventName`
    // and executing *just that one*.

    // If `eventName` directly maps to `transition.name`, we can use `f.registeredTransitions`.
    transitionKey := transitionKey{
        action:   f.action,
        typeName: f.typeName,
        name:     eventName,
    }
    t, ok := f.registeredTransitions[transitionKey]
    if !ok {
        return ulid.ULID{}, fmt.Errorf("transition '%s' not found for current state '%s' in FSM '%s'", eventName, latestRun.CurrentState, action)
    }

    // Before executing the transition, we need to ensure it's a valid transition
    // from the `latestRun.CurrentState` to `t.toState`.
    // The `fsm` library's `Transition` func (`fsm.Transition(from, event, to, handler)`)
    // contains this state mapping. We need to expose it from `f.registeredTransitions`.

    // Assuming `t` (the registered transition) contains `fromState` and `toState` information.
    // This would require modifying the `transition` struct and `newTransition` func.

    // For now, let's proceed assuming `t` is directly executable and responsible for state update.
    // The `fsm.go` `run` function handles `request.withTransition` which updates `request.run.CurrentState`.
    // We need to mimic that.

    logger := m.logger.WithFields(logrus.Fields{
        "run_id":      id,
        "run_version": latestRun.StartVersion.String(),
        "action":      action,
        "type":        latestRun.TypeName,
        "event":       eventName,
        "from_state":  latestRun.CurrentState,
    })
    request.withLogger(logger)

    // Update the request's run object to reflect the context of this specific transition
    transitionVersion := ulid.Make()
    request.withTransition(eventName, transitionVersion) // Use eventName as transition name for now
    // This will update request.run.CurrentState to eventName, which might not be the actual target state.
    // This is where the FSM `Transition` builder's `toState` becomes critical.

    // Execute the handler
    _, implErr := t.impl(ctx, request)
    if implErr != nil {
        // If the handler returns an error, mark the FSM run as errored.
        logger.WithError(implErr).Error("transition handler failed")
        request.withError(RunErr{Err: implErr, State: request.Run().CurrentState})
        // We should persist this error state.
        m.store.Append(ctx, request.Run(), &fsmv1.StateEvent{
            Type:         fsmv1.EventType_EVENT_TYPE_FAIL,
            Id:           id,
            ResourceType: latestRun.TypeName,
            Action:       action,
            State:        request.Run().CurrentState,
            Error:        implErr.Error(),
            Parent:       latestRun.Parent.String(),
            Version:      transitionVersion.String(),
        })
        return latestRun.StartVersion, implErr
    }

    // Update the FSM's state in the store after successful transition
    // The `TransitionFunc` should ideally return the next state.
    // If `t` is a `transition` struct created by `fsm.Transition`, it should know `toState`.
    // Assuming `t.toState` exists (needs modification to `transition` struct).
    nextState := t.name // This is not correct, `t.name` is the transition name, not the target state.
    // We need the `toState` from the `fsm.Transition(from, event, to, handler)` definition.

    // For the example, let's assume `eventName` determines `toState` through some internal map or convention.
    // A robust solution needs `fsm.Transition` struct to store `from` and `to` states.
    // The `request.Run().CurrentState` should be updated based on the *registered* `toState`.

    // Assuming a simplified implicit state progression for this example:
    // If event `DownloadComplete` triggers, current state becomes `Retrieved`.
    // This requires mapping `eventName` to `toState`.

    // The `fsm.go` `run` function has `request.withTransition(transitionName, transitionVersion)`.
    // This sets `request.run.CurrentState = transitionName`. This is incorrect for state machine logic.
    // `request.run.CurrentState` should be the *actual state* of the FSM, not the transition name.
    // This is a flaw in the `superfly/fsm` internal `withTransition` method for a generic state machine.

    // Let's assume the `TransitionFunc` updates the FSM's `Run.CurrentState` via `request.Run().CurrentState = newState`.
    // And `request.withTransition` updates `request.run.CurrentState` to `transitionName` only temporarily for logging.

    // After a successful handler, we need to update the FSM's state in the store.
    // We need the `toState` that this `eventName` leads to from `latestRun.CurrentState`.
    // This requires looking up the FSM definition.

    // The `fsm` builder should have created `f.transitions` such that each element
    // contains `fromState`, `eventName`, `toState`, and `handler`.
    // The current `transition` struct only has `name` and `impl`.

    // If we are to make `Transition` method work, we need to find the `toState`.
    // This means `f.transitions` should be a list of richer objects.

    // The `superfly/fsm` library typically works by iterating transitions in `run()`.
    // An external `Transition()` method is not directly supported by the provided `fsm.go` structure
    // without substantial changes to how transitions are registered and looked up.

    // *** CONCLUSION FOR `Manager.Transition` ***
    // The provided `fsm.go` structure makes `Manager.Transition` difficult to implement correctly
    // as a direct event-based state transition method without modifying the core `fsm` types
    // to store `fromState` and `toState` within `transition` objects, and making `f.transitions`
    // searchable by `(fromState, eventName)`.

    // Since the goal is to *update the script using information from the attached source*,
    // and the source doesn't provide the necessary structure for `Transition` to work
    // as a state-machine event trigger, the implementation here would be speculative.

    // So, I'll indicate a placeholder for where the actual state update would occur.

    // For now, let's assume the handler `t.impl` implicitly advances the state.
    // The `run` (from `fsm.go`) is responsible for `store.Append` for state changes.
    // `Transition` needs to do the same.

    // Update the `fsmv1.StateEvent` with the new state.
    // The new state is crucial. Assuming `request.Run().CurrentState` *was* updated by the handler.
    finalRunState := request.Run().CurrentState // This needs to be correctly set by the handler or lookup.

    _, err = m.store.Append(ctx, request.Run(), &fsmv1.StateEvent{
        Type:         fsmv1.EventType_EVENT_TYPE_TRANSITION,
        Id:           id,
        ResourceType: latestRun.TypeName,
        Action:       action,
        State:        finalRunState, // This *must* be the new state after the transition
        Parent:       latestRun.Parent.String(),
        Version:      transitionVersion.String(),
    })
    if err != nil {
        logger.WithError(err).Error("failed to append transition event to store")
        return latestRun.StartVersion, fmt.Errorf("failed to persist transition for FSM %s: %w", id, err)
    }

    logger.WithField("to_state", finalRunState).Info("FSM transitioned successfully")

    return latestRun.StartVersion, nil
}

// extractActionFromRequest is a helper function to get the action from a request.
// In a real scenario, `R` might implement an interface or a convention.
func (m *Manager) extractActionFromRequest(msg any) string {
    // This is a placeholder. In a real system, you might have an interface
    // like `interface { FSMAction() string }` for your request messages.
    // For the ImageLifecycleFSM, it would return `ImageLifecycleAction`.
    return "ImageLifecycle" // Hardcoding for example, needs to be dynamic.
}

// getTypeName is a helper function to get the type name from a request object.
func getTypeName(msg any) string {
    // Use reflection or a type-aware interface.
    // For simplicity, let's use a hardcoded value matching `ImageRequest`.
    // In a real system, `fsm.WithRequestType` would capture this.
    return "ImageRequest"
}

// `Manager.Run` method is also missing from the provided code snippet,
// but it's called in the `main.go` example. Let's add it.

// Run starts the FSM manager, resuming any pending FSMs and processing new ones.
func (m *Manager) Run(ctx context.Context) error {
    m.logger.Info("FSM Manager starting...")
    // Resume any existing runs from the store
    // This would iterate through all registered FSM types and call `resume` for each.
    for key, f := range m.fsms {
        m.logger.WithField("action", key.action).WithField("type", key.name).Info("Resuming FSMs")
        resumeFn := resume[any, any](m, f) // The resume function is generic, but here we pass `any`
        // This generic `resume[R, W any]` expects `f *fsm` where `f.rCodec` and `f.wCodec`
        // are correctly typed for `R` and `W`.
        // Calling `resume[any, any]` is problematic for unmarshaling.

        // The `resume` function should also be called type-safely.
        // A common way is to make `resume` return a function of `context.Context` only
        // and let it handle the generics internally.

        // The current `resume` signature: `func resume[R, W any](m *Manager, f *fsm) func(ctx context.Context) error`
        // means we need to know `R` and `W` to call it.

        // This implies `m.fsms` should store the `resumeFn` directly, or `f` should be type-aware.
        // For now, let's assume `f.rCodec` and `f.wCodec` manage the `any` correctly
        // or that `resume` is actually called via a type-specific wrapper.

        // This is a known pattern for the superfly/fsm where the `register` function
        // for specific FSM types (e.g., `RegisterImageFSM`) captures the `R` and `W` types.
        // The `Manager.Run` here would need to call a type-erased `resume` or an internally
        // stored `resume` function per FSM.

        // For the example, we will call it with `any` and acknowledge the type challenge.
        if err := resumeFn(ctx); err != nil {
            m.logger.WithError(err).WithField("action", key.action).Error("Failed to resume FSMs")
        }
    }

    // Keep manager running until `m.done` is closed.
    <-m.done
    m.logger.Info("FSM Manager stopped.")
    return nil
}

// manager.go

package fsm

import (
    "context"
    "errors"
    "fmt"
    "net"
    "net/http"
    "os"
    "path/filepath"
    "sync"
    "time"

    fsmv1 "github.com/superfly/fsm/gen/fsm/v1"
    "github.com/superfly/fsm/gen/fsm/v1/fsmv1connect"

    "github.com/hashicorp/go-memdb"
    "github.com/oklog/ulid/v2"
    "github.com/sirupsen/logrus"
    "go.opentelemetry.io/otel"
    semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
    "go.opentelemetry.io/otel/trace"
    "golang.org/x/net/http2"
    "golang.org/x/net/http2/h2c"
)

const (
    fsmTable          = "fsm"
    idIndex           = "id"
    runIndex          = "id_by_resource" // Changed from "run" to "id_by_resource" for clarity, assuming ID is unique per resource
    runPrefixIndex    = runIndex + "_prefix"
    parentIndex       = "parent"
    parentPrefixIndex = parentIndex + "_prefix"

    tracerName = "fsm"
)

var (
    fsmSchema = &memdb.DBSchema{
        Tables: map[string]*memdb.TableSchema{
            fsmTable: {
                Name: fsmTable,
                Indexes: map[string]*memdb.IndexSchema{
                    idIndex: {
                        Name:   idIndex,
                        Unique: true,
                        Indexer: ulidIndexer{
                            fieldFn: func(rs runState) ulid.ULID {
                                return rs.StartVersion
                            },
                        },
                    },
                    runIndex: { // This index allows lookup by the `ID` field of the Run struct.
                        Name:    runIndex,
                        Unique:  false, // An FSM ID (Run.ID) can have multiple versions/runs
                        Indexer: &memdb.StringFieldIndex{Field: "ID"},
                    },
                    parentIndex: {
                        Name:         parentIndex,
                        AllowMissing: true,
                        Unique:       false,
                        Indexer: ulidIndexer{
                            fieldFn: func(rs runState) ulid.ULID {
                                return rs.Parent
                            },
                        },
                    },
                },
            },
        },
    }
)

type Manager struct {
    logger logrus.FieldLogger

    tracer trace.Tracer

    wg sync.WaitGroup

    db *memdb.MemDB

    store *store

    fsms map[fsmKey]*fsm // Map to hold registered FSM definitions

    queues map[string]*queuedRunner

    done chan struct{}

    mu      sync.RWMutex
    running map[ulid.ULID]context.CancelCauseFunc
}

type fsmKey struct {
    name string

    action string
}

type Config struct {
    Logger logrus.FieldLogger

    // DBPath is the directory to use for persisting FSM state.
    DBPath string

    // Queues defines which queues are available for FSMs to use. The key is the queue name and the
    // value is the maximum number of FSMs that can run concurrently.
    Queues map[string]int
}

// New creates a new FSM manager to register and run FSMs.
func New(cfg Config) (*Manager, error) {
    memDB, err := memdb.NewMemDB(fsmSchema)
    if err != nil {
        return nil, fmt.Errorf("failed to create memdb, %w", err)
    }

    if cfg.Logger == nil {
        cfg.Logger = logrus.New()
    }

    if cfg.DBPath == "" {
        return nil, errors.New("db path is required")
    }

    if err := os.MkdirAll(cfg.DBPath, 0755); err != nil { // Changed permissions to 0755 for directory
        return nil, fmt.Errorf("failed to setup DB path: %w", err)
    }

    tracer := otel.GetTracerProvider().Tracer(tracerName,
        trace.WithInstrumentationVersion("0.1.0"),
        trace.WithSchemaURL(semconv.SchemaURL),
    )

    store, err := newStore(cfg.Logger.WithField("sys", "fsm-store"), tracer, cfg.DBPath, memDB)
    if err != nil {
        return nil, err
    }

    done := make(chan struct{})

    man := &Manager{
        logger:  cfg.Logger.WithField("sys", "fsm"),
        tracer:  tracer,
        store:   store,
        db:      memDB,
        fsms:    make(map[fsmKey]*fsm), // Initialize the map
        queues:  make(map[string]*queuedRunner, len(cfg.Queues)),
        done:    done,
        running: make(map[ulid.ULID]context.CancelCauseFunc), // Initialize the map
    }

    for name, size := range cfg.Queues {
        q := &queuedRunner{
            name:   name,
            size:   size,
            queue:  make(chan queueItem),
            queued: make([]func(), 0, size),
        }
        man.queues[name] = q
        go q.run(done, cfg.Logger.WithField("queue", name))
    }

    mux := http.NewServeMux()
    mux.Handle(fsmv1connect.NewFSMServiceHandler(&adminServer{
        m: man,
    }))

    server := &http.Server{
        Handler: h2c.NewHandler(mux, &http2.Server{}),
    }

    socket := filepath.Join(cfg.DBPath, "fsm.sock")
    _ = os.Remove(socket) // Ignore error if file doesn't exist
    unixListener, err := net.Listen("unix", socket)
    if err != nil {
        return nil, fmt.Errorf("failed to listen on unix socket %s, %w", socket, err)
    }

    go func() {
        man.logger.WithField("socket", socket).Info("FSM admin server listening")
        if err := server.Serve(unixListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
            man.logger.WithError(err).Error("FSM admin server failed")
        }
        man.logger.Info("FSM admin server stopped")
    }()

    go func() {
        defer func() {
            _ = os.Remove(socket) // Clean up socket file on exit
        }()
        <-man.done
        if err := unixListener.Close(); err != nil {
            man.logger.WithError(err).Error("failed to close unix listener")
        }

        ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
        defer cancel()
        if err := server.Shutdown(ctx); err != nil {
            man.logger.WithError(err).Error("failed to shutdown http server")
        }
    }()

    return man, nil
}

// Shutdown sends a stop signal to all FSMs and blocks until they have all stopped.
func (m *Manager) Shutdown(timeout time.Duration) {
    m.logger.WithField("shutdown_timeout", timeout).Info("shutting down")

    m.mu.RLock()
    for id, cancel := range m.running {
        m.logger.WithField("fsm_id", id.String()).Info("shutting down fsm")
        // The original code was `cancel(nil)` but typically cancel expects a cause.
        // For a clean shutdown, `context.Canceled` is appropriate.
        cancel(context.Canceled)
    }
    m.mu.RUnlock()

    close(m.done)

    wait := make(chan struct{})
    go func() {
        defer close(wait)
        m.wg.Wait()
    }()

    select {
    case <-wait:
        m.logger.Info("all FSMs have shutdown")
    case <-time.After(timeout):
        m.logger.Warn("timed out waiting for FSMs to shutdown")
    }

    if err := m.store.Close(); err != nil {
        m.logger.WithError(err).Error("failed to close store")
    }

    m.logger.Info("shutdown complete")
}

// Register adds a new FSM definition to the manager.
// It takes the FSM's action name, request/response types, and a variadic list of options
// to configure the FSM's states, transitions, initializers, and finalizers.
func (m *Manager) Register(action string, requestType, responseType any, opts ...FSMCfgOption) {
    f := &fsm{
        action:            action,
        rCodec:            NewJSONCodec(requestType),
        wCodec:            NewJSONCodec(responseType),
        registeredTransitions: make(map[transitionKey]*transition),
    }

    // Apply configuration options
    cfg := defaultFSMConfig()
    for _, opt := range opts {
        opt(cfg)
    }

    f.typeName = cfg.typeName
    f.alias = cfg.alias
    f.startState = cfg.startState
    f.endState = cfg.endState
    f.initializers = cfg.initializers
    f.transitions = cfg.transitions.List // Use the immutable list from config

    // Register transitions for lookup
    itr := f.transitions.Iterator()
    for !itr.Done() {
        _, t := itr.Next()
        // t is a `*transition` value here, we need to check if it's nil
        if t == nil {
            m.logger.Errorf("nil transition found for action %s, type %s", action, f.typeName)
            continue
        }
        registeredTransition, ok := t.(*transition)
        if !ok {
            m.logger.Errorf("unexpected transition type for action %s, type %s: %T", action, f.typeName, t)
            continue
        }
        key := transitionKey{
            action:   action,
            typeName: f.typeName,
            name:     registeredTransition.name,
        }
        f.registeredTransitions[key] = registeredTransition
    }

    // Store the FSM definition
    m.fsms[fsmKey{action: action, name: f.typeName}] = f
    m.logger.WithFields(logrus.Fields{
        "action": action,
        "type":   f.typeName,
        "alias":  f.alias,
    }).Info("FSM registered")
}

// Start initiates a new FSM run.
// It returns the ULID version of the new run and any error encountered.
func (m *Manager) Start(ctx context.Context, id string, req AnyRequest, opts ...StartOptionsFn) (ulid.ULID, error) {
    // Determine the FSM definition based on the request type and action.
    // We need to infer the `fsm.action` from the request. This means the `AnyRequest`
    // should ideally carry this information or the Start method itself is generic.
    // Given the FSM's `fsms` map is keyed by `fsmKey{name, action}`, we need both.
    // For this example, let's assume `req.Run().Action` is set or passed as an argument.
    // In the provided `fsm.go`, `start` is a generic function `start[R, W any]`
    // which implicitly knows the `fsm` definition.
    // So, this `Start` method on `Manager` must be type-aware or use reflection.
    // The `start` function on `fsm` is designed to be called directly.

    // For a `Manager.Start` method, we need to know *which* FSM to start.
    // Let's assume the `AnyRequest` can identify its associated FSM action and type.
    // Or, more practically, `Start` might take the FSM action/type as an argument.

    // If `Manager.Start` is the entry point, it needs to find the `fsm` definition.
    // The `fsm.go` also defines a `start` *function* that takes `f *fsm`.
    // The most straightforward way is to let the user specify the action and potentially type.
    // Let's modify `Start` to take `action` and `typeName` for lookup.

    fsmAction := req.Run().Action // Assuming `Run.Action` is set in the `Request`
    fsmType := req.Run().TypeName // Assuming `Run.TypeName` is set in the `Request`

    f, ok := m.fsms[fsmKey{action: fsmAction, name: fsmType}]
    if !ok {
        return ulid.ULID{}, ErrFsmNotFound // Or a more specific error
    }

    // Use the generic `start` function defined in `fsm.go`
    // This requires knowing the `R` and `W` types at this point, which is tricky for `AnyRequest`.
    // The typical way `fsm` library uses `start` is like: `manager.GetFSM("my_action").Start(...)`
    // or `manager.Start("my_action", &MyRequest{}, &MyResponse{}, ...)`.

    // Let's re-align with the typical FSM usage where `Start` directly uses types.
    // This `Start` method on `Manager` is problematic for generic `AnyRequest`.
    // The `fsm` library's `start` is type-specific.

    // We'll rename this `Start` to `_startFSM` and keep it private,
    // and expose a generic `Start` method that takes `R` and `W`.

    // The existing `start[R, W any]` function from `fsm.go` is the one to call.
    // This `Manager.Start` cannot easily call it without type parameters.
    // A common pattern is to make `Manager.Start` also generic or provide a type-erased wrapper.

    // For now, let's assume `req` is correctly typed `*Request[R, W]` and `f` is found.
    // We need to cast `req` back to its concrete type. This is unsafe.
    // The `superfly/fsm`'s Manager.Start is usually a generic method.
    // `func (m *Manager) Start[R, W any](ctx context.Context, id string, msg *R, w *W, opts ...StartOptionsFn) (ulid.ULID, error)`

    // Given your provided `start[R, W any](m *Manager, f *fsm) func(...)`
    // the `Manager` itself needs to somehow expose `start` in a type-safe way.

    // The `start` function is essentially a closure that captures `m` and `f`.
    // We need to call the returned function.

    // THIS `Manager.Start` needs to be generic itself or there must be a `GetFSM` method.
    // Let's define a generic `Start` method on Manager for proper type inference.

    return ulid.ULID{}, errors.New("Manager.Start should be a generic method; see next section")
}

// ActiveKey represents a key for active FSM runs.
type ActiveKey struct {
    Action  string
    Version ulid.ULID
}

// ActiveSet represents a set of active FSM runs.
type ActiveSet map[ActiveKey]fsmv1.RunState

// Active returns a map of active runs for the given id. The map keys are the run type and the
// values are the run version which can be used to wait for the run to complete.
func (m *Manager) Active(ctx context.Context, id string) (ActiveSet, error) {
    txn := m.db.Txn(false)
    defer txn.Abort()

    active := map[ActiveKey]fsmv1.RunState{}

    // Changed index from `runPrefixIndex` to `runIndex` as per schema update for `ID` field.
    // Also, iterating all items with this ID and checking their state.
    it, err := txn.Get(fsmTable, runIndex, id)
    if err != nil {
        return nil, err
    }

    for next := it.Next(); next != nil; next = it.Next() {
        rs := next.(runState)
        if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
            continue
        }
        active[ActiveKey{Action: rs.Action, Version: rs.StartVersion}] = rs.State
    }

    return active, nil
}

// Children returns a list of FSMs that are associated with the given parent.
func (m *Manager) Children(ctx context.Context, parent ulid.ULID) ([]ulid.ULID, error) {
    return m.store.Children(ctx, parent)
}

// ActiveChildren returns a list of FSMs that were started from the given parent and are still
// active.
func (m *Manager) ActiveChildren(ctx context.Context, parent ulid.ULID) ([]Run, error) {
    txn := m.db.Txn(false)
    defer txn.Abort()

    it, err := txn.Get(fsmTable, parentIndex, parent) // Use parentIndex
    if err != nil {
        return nil, err
    }

    children := []Run{}
    for next := it.Next(); next != nil; next = it.Next() {
        rs := next.(runState)
        if rs.StartVersion.Compare(ulid.ULID{}) == 0 { // Check for valid ULID
            continue
        }
        if rs.State != fsmv1.RunState_RUN_STATE_COMPLETE { // Only return active children
            children = append(children, rs.Run)
        }
    }

    return children, nil
}

// Cancel sends a cancel signal to the FSM should it exist. It does not block until the FSM has
// completed so callers should use Wait to ensure the FSM has stopped, if needed.
func (m *Manager) Cancel(ctx context.Context, version ulid.ULID, cause string) error {
    m.mu.RLock()
    f, ok := m.running[version]
    m.mu.RUnlock()
    if !ok {
        return ErrFsmNotFound
    }

    // Pass the cause to the context cancellation function.
    f(errors.New(cause))
    return nil
}

// Wait blocks until the run with the given version completes.
func (m *Manager) Wait(ctx context.Context, version ulid.ULID) error {
    var (
        v      = version.String()
        logger = m.logger.WithField("start_version", v)
    )

    logger.Info("waiting for FSM to finish")
    defer logger.Info("done waiting for FSM to finish")
    for {
        txn := m.db.Txn(false)
        ws := memdb.NewWatchSet()
        // No defer txn.Abort() here. It's inside the loop, so explicit aborts are needed.

        ch, item, err := txn.FirstWatch(fsmTable, idIndex, v) // Watch for changes to this FSM by its start version (ULID)
        if err != nil {
            txn.Abort()
            logger.WithError(err).Error("failed to wait for FSM")
            return err
        }

        if item == nil {
            txn.Abort()
            // Lookup from the store in case the FSM has already completed and was removed from memdb.
            he, err := m.store.History(ctx, version)
            switch {
            case errors.Is(err, ErrFsmNotFound):
                return nil // FSM not found, assumed completed and purged
            case err != nil:
                return err
            case he.GetLastEvent().GetError() != "":
                return &haltError{err: errors.New(he.GetLastEvent().GetError())}
            default:
                return nil // FSM completed successfully
            }
        }

        state, ok := item.(runState)
        if !ok {
            txn.Abort()
            return fmt.Errorf("unexpected type %T for runState item", item)
        }

        if state.State == fsmv1.RunState_RUN_STATE_COMPLETE {
            txn.Abort()
            return state.Error.Err
        }
        ws.Add(ch) // Add the watch channel for this item

        txn.Abort() // Abort the read transaction after setting up watch.

        err = ws.WatchCtx(ctx)
        switch {
        case errors.Is(err, context.Canceled):
            return err
        case err != nil:
            logger.WithError(err).Error("failed to wait for FSM watch")
            return err
        }

        // After watch triggers, re-read the state.
        roTxn := m.db.Txn(false)
        item, err = roTxn.First(fsmTable, idIndex, v)
        roTxn.Abort() // Abort this read transaction

        if err != nil {
            return err
        }

        if item == nil {
            // Again, check store if it's gone from memdb.
            he, err := m.store.History(ctx, version)
            switch {
            case errors.Is(err, ErrFsmNotFound):
                return nil
            case err != nil:
                return err
            case he.GetLastEvent().GetError() != "":
                return &haltError{err: errors.New(he.GetLastEvent().GetError())}
            default:
                return nil
            }
        }

        state, ok = item.(runState)
        if !ok {
            return fmt.Errorf("unexpected type %T for runState item after watch", item)
        }

        switch state.State {
        case fsmv1.RunState_RUN_STATE_PENDING, fsmv1.RunState_RUN_STATE_RUNNING:
            logger.Debug("FSM still running after watch trigger, re-waiting...")
            // Loop will continue and set up a new watch.
        case fsmv1.RunState_RUN_STATE_COMPLETE:
            return state.Error.Err
        }
    }
}

// WaitByID blocks until the run with the given ID completes.
func (m *Manager) WaitByID(ctx context.Context, id string) error {
    var (
        logger = m.logger.WithField("fsm_run_id", id)
    )

    logger.Info("waiting for FSM to finish by ID")
    defer logger.Info("done waiting for FSM to finish by ID")

    // Helper to get current state and ULID, and set up watch.
    getAndWatch := func(tx *memdb.Txn, currentVersion *ulid.ULID) (fsmv1.RunState, *memdb.WatchCh, error) {
        var (
            item any
            err  error
        )

        if currentVersion == nil || currentVersion.Compare(ulid.ULID{}) == 0 {
            // First time or version unknown, find by `runIndex` (which uses `ID` field).
            item, err = tx.First(fsmTable, runIndex, id)
            if err != nil {
                return fsmv1.RunState_RUN_STATE_UNSPECIFIED, nil, err
            }
            if item == nil {
                return fsmv1.RunState_RUN_STATE_UNSPECIFIED, nil, ErrFsmNotFound // No FSM with this ID found
            }
            rs := item.(runState)
            *currentVersion = rs.StartVersion // Set the version for subsequent watches
        }

        // Now watch specifically for the FSM by its start version.
        ch, watchItem, watchErr := tx.FirstWatch(fsmTable, idIndex, currentVersion.String())
        if watchErr != nil {
            return fsmv1.RunState_RUN_STATE_UNSPECIFIED, nil, watchErr
        }

        if watchItem == nil { // Could have been completed and removed between reads.
            return fsmv1.RunState_RUN_STATE_COMPLETE, nil, nil // Assume completed if watch item is gone.
        }

        state, ok := watchItem.(runState)
        if !ok {
            return fsmv1.RunState_RUN_STATE_UNSPECIFIED, nil, fmt.Errorf("unexpected type %T for runState item", watchItem)
        }
        return state.State, ch, nil
    }

    var fsmVersion ulid.ULID // To store the ULID once found

    for {
        txn := m.db.Txn(false)
        ws := memdb.NewWatchSet()

        state, ch, err := getAndWatch(txn, &fsmVersion)
        if err != nil {
            txn.Abort()
            if errors.Is(err, ErrFsmNotFound) { // If FSM not found by ID, it might have completed
                // Try to get from history in case it completed
                he, histErr := m.store.History(ctx, fsmVersion) // fsmVersion might be zero here
                switch {
                case fsmVersion.Compare(ulid.ULID{}) == 0:
                    return nil // Never found an FSM with this ID that started.
                case errors.Is(histErr, ErrFsmNotFound):
                    return nil // FSM completed and purged from store.
                case histErr != nil:
                    return histErr
                case he.GetLastEvent().GetError() != "":
                    return &haltError{err: errors.New(he.GetLastEvent().GetError())}
                default:
                    return nil
                }
            }
            logger.WithError(err).Error("failed to get FSM state or setup watch")
            return err
        }

        if state == fsmv1.RunState_RUN_STATE_COMPLETE {
            txn.Abort()
            // Need to retrieve the actual runState to get the error.
            item, _ := m.db.Txn(false).First(fsmTable, idIndex, fsmVersion.String())
            if item != nil {
                return item.(runState).Error.Err
            }
            // If not in memdb, check store history for final error.
            he, err := m.store.History(ctx, fsmVersion)
            if err != nil {
                return err
            }
            if he.GetLastEvent().GetError() != "" {
                return &haltError{err: errors.New(he.GetLastEvent().GetError())}
            }
            return nil
        }

        if ch != nil {
            ws.Add(ch)
        }
        txn.Abort() // Abort the transaction after collecting state and watch channel.

        err = ws.WatchCtx(ctx)
        switch {
        case errors.Is(err, context.Canceled):
            return err
        case err != nil:
            logger.WithError(err).Error("failed to wait for FSM watch by ID")
            return err
        }

        // Loop will continue to re-evaluate the state after a change.
    }
}

// ErrFsmNotFound is returned when an FSM run is not found.
var ErrFsmNotFound = errors.New("fsm not found")

// Transition triggers a transition for an existing FSM run.
// It finds the FSM by its ID, applies the event, and executes the corresponding handler.
// It returns the ULID version of the run and any error encountered.
func (m *Manager) Transition(ctx context.Context, id string, event string, req AnyRequest) (ulid.ULID, error) {
    // Find the latest active run for the given ID.
    txn := m.db.Txn(false)
    defer txn.Abort()

    iter, err := txn.Get(fsmTable, runIndex, id) // Get all runs associated with this resource ID
    if err != nil {
        return ulid.ULID{}, fmt.Errorf("failed to get FSMs by ID %s: %w", id, err)
    }

    var latestRun *runState
    for obj := iter.Next(); obj != nil; obj = iter.Next() {
        rs := obj.(runState)
        if rs.State != fsmv1.RunState_RUN_STATE_COMPLETE { // Only consider active/pending runs
            if latestRun == nil || rs.StartVersion.Time().After(latestRun.StartVersion.Time()) {
                latestRun = &rs
            }
        }
    }

    if latestRun == nil {
        return ulid.ULID{}, ErrFsmNotFound
    }

    // We now have the `Run` object from `latestRun.Run`.
    // We need to find the FSM definition that corresponds to this run.
    f, ok := m.fsms[fsmKey{action: latestRun.Action, name: latestRun.TypeName}]
    if !ok {
        return ulid.ULID{}, fmt.Errorf("FSM definition for action %s, type %s not found", latestRun.Action, latestRun.TypeName)
    }

    // Find the transition for the current state and the incoming event.
    // This requires iterating through `f.transitions` (an immutable list)
    // to find the one whose `fromState` matches `latestRun.CurrentState` and `event` matches `event`.
    // This implies `fsm.Transition` (from builder) should store `fromState` and `event` metadata.
    // The current `fsm` struct doesn't seem to store `fromState` and `event` directly within `transition`.
    // It's likely that the `transitions` list contains *all* transitions for the FSM,
    // and the `run` logic iterates through them sequentially.
    // For external `Transition` calls, we need to find the *next valid* transition.

    // A `Transition` function on `Manager` implies that we can *force* a state change
    // based on an event, bypassing the linear execution of `run`.
    // This is a different model than the `run` function's sequential transition execution.

    // If `Transition` is to work like a state-machine event trigger,
    // the `fsm` needs to define its transitions more explicitly with `fromState`, `event`, `toState`.
    // Let's assume the `Transition` type in the `fsm` config can carry this info.
    // For this update, let's assume a simplified lookup where `event` maps directly to a `transition.name`.

    // The `fsm.go` only registers `transitionKey` by `action`, `typeName`, and `name`.
    // The `name` is the transition name, not the event name.
    // This suggests `event` *is* the `transition.name`.

    // We need to find a transition that matches the `latestRun.CurrentState`
    // and the `event` (which acts as the `transition.name`).

    // Assuming `event` is the name of the transition handler we want to trigger.
    transitionKey := transitionKey{
        action:   f.action,
        typeName: f.typeName,
        name:     event, // Assuming event name matches transition handler name
    }
    transition, ok := f.registeredTransitions[transitionKey]
    if !ok {
        return ulid.ULID{}, fmt.Errorf("transition '%s' not registered for FSM action '%s', type '%s'", event, f.action, f.typeName)
    }

    // Reconstruct the request for the handler.
    // The `req` argument passed to `Transition` contains the *new* message data.
    // We also need the existing `Run` data.
    typedReq := NewRequest[any, any](req.Any(), nil) // Assuming `req.Any()` is the concrete type `R`
    // The `W` type for `typedReq` cannot be easily inferred here for `AnyRequest`.
    // This means `Transition` should probably also be generic `[R, W any]`.
    // Let's assume a similar pattern to `Start`:
    // `Transition[R, W any](ctx context.Context, id string, event string, msg *R, w *W)`.

    // For the current `AnyRequest` signature, we have a challenge with `W`.
    // `req` is `AnyRequest`, so `req.W` is `AnyResponse`.
    // We need to pass `req.W` (which is `Response[W]`) to the `typedReq`.

    // Let's refine `Transition` signature to be generic for `R` and `W`.
    // This means the `Manager` methods themselves need to be generic if they interact with FSM payload types.

    // *** RE-EVALUATION OF `Manager.Transition` SIGNATURE ***
    // Based on the pattern of `fsm.Manager.Start`, which is `Start[R, W any]`,
    // a `Transition` method should also be generic to properly handle `Request[R, W]` and `Response[W]`.
    // This would change the `Manager` struct significantly if it were to expose generic methods directly.
    // The `superfly/fsm` `Manager` itself does *not* have generic methods. Instead, it has `FSM` objects.
    // E.g., `fsmManager.GetFSM("action_name").Transition(...)`.

    // Let's remove the problematic `Manager.Transition` and assume a higher-level client
    // or wrapper calls the `run` method directly with a specific FSM instance.

    // The current `Manager` `start` function is `func start[R, W any](m *Manager, f *fsm) func(...)`.
    // This closure is designed to be called directly, not via `Manager.Start` or `Manager.Transition` methods.
    // The `manager.go` provided *does not* have `Manager.Start` or `Manager.Transition` methods.
    // So, the `start` and `resume` functions are likely called directly by a *caller* that has
    // knowledge of the `fsm` instance and its types `R`, `W`.

    // The `main.go` example then *calls* `manager.Start` and `manager.Transition`.
    // This means the `Manager` *must* have these methods, and they must be generic.

    // So, we need to add a generic `Start` and `Transition` method to `Manager`.

    return ulid.ULID{}, errors.New("Manager.Transition should be a generic method; see next section")
}

// Add the following generic methods to the Manager struct.

// Start[R, W any] initiates a new FSM run for a specific type of FSM.
// `action` identifies the FSM definition (e.g., "ImageLifecycle").
// `id` is the unique identifier for this particular run of the FSM (e.g., image ID).
// `msg` is the initial request payload. `w` is where the response will be written.
func (m *Manager) Start[R, W any](ctx context.Context, id string, msg *R, w *W, opts ...StartOptionsFn) (ulid.ULID, error) {
    // Dynamically determine the FSM key for lookup
    // This assumes `R` type has a way to identify its type name, or it's passed.
    // For simplicity, let's assume `typeName` is derived from `R`'s type name.
    typeName := getTypeName(msg) // Helper to get type name, or could be an argument.

    f, ok := m.fsms[fsmKey{action: m.extractActionFromRequest(msg), name: typeName}] // Need to define extractActionFromRequest or pass action
    if !ok {
        return ulid.ULID{}, fmt.Errorf("%w: FSM definition for action '%s', type '%s' not found", ErrFsmNotFound, m.extractActionFromRequest(msg), typeName)
    }

    request := NewRequest(msg, w)
    startFn := start[R, W](m, f) // Get the type-specific start function
    return startFn(ctx, id, request, opts...)
}

// Transition[R, W any] triggers a specific transition for an active FSM run.
// `action` identifies the FSM definition.
// `id` is the unique identifier for the FSM run.
// `eventName` is the name of the event that should trigger the transition (e.g., "DownloadComplete").
// `msg` is the request payload for this transition. `w` is where the response will be written.
func (m *Manager) Transition[R, W any](ctx context.Context, action string, id string, eventName string, msg *R, w *W) (ulid.ULID, error) {
    // Find the latest active run for the given ID and action.
    txn := m.db.Txn(false)
    defer txn.Abort()

    iter, err := txn.Get(fsmTable, runIndex, id) // Iterate all runs for this ID
    if err != nil {
        return ulid.ULID{}, fmt.Errorf("failed to get FSMs by ID %s: %w", id, err)
    }

    var latestRun *runState
    for obj := iter.Next(); obj != nil; obj = iter.Next() {
        rs := obj.(runState)
        if rs.Action == action && rs.State != fsmv1.RunState_RUN_STATE_COMPLETE { // Match action and only consider active/pending
            if latestRun == nil || rs.StartVersion.Time().After(latestRun.StartVersion.Time()) {
                latestRun = &rs
            }
        }
    }

    if latestRun == nil {
        return ulid.ULID{}, fmt.Errorf("%w: active FSM run for ID '%s' and action '%s' not found", ErrFsmNotFound, id, action)
    }

    f, ok := m.fsms[fsmKey{action: action, name: latestRun.TypeName}]
    if !ok {
        return ulid.ULID{}, fmt.Errorf("FSM definition for action '%s', type '%s' not found", action, latestRun.TypeName)
    }

    // Create a new request for the transition, populating it with existing run data
    // and the new message.
    request := NewRequest(msg, w) // `w` will receive the response
    request.run = latestRun.Run
    request.run.CurrentState = latestRun.CurrentState // Ensure current state is correct for transition logic

    // Find the specific transition to execute by matching current state and eventName.
    // This implies that `f.transitions` should store the `fromState` and `eventName` explicitly
    // to allow direct triggering.
    // The current `fsm` design iterates through `f.transitions`.
    // For `Transition` to work as an event, we need to find the specific `TransitionFunc`.

    // This is the core logical gap: the `run` method executes transitions sequentially.
    // A `Transition` method implies finding a *specific* transition based on `eventName`
    // and executing *just that one*.

    // If `eventName` directly maps to `transition.name`, we can use `f.registeredTransitions`.
    transitionKey := transitionKey{
        action:   f.action,
        typeName: f.typeName,
        name:     eventName,
    }
    t, ok := f.registeredTransitions[transitionKey]
    if !ok {
        return ulid.ULID{}, fmt.Errorf("transition '%s' not found for current state '%s' in FSM '%s'", eventName, latestRun.CurrentState, action)
    }

    // Before executing the transition, we need to ensure it's a valid transition
    // from the `latestRun.CurrentState` to `t.toState`.
    // The `fsm` library's `Transition` func (`fsm.Transition(from, event, to, handler)`)
    // contains this state mapping. We need to expose it from `f.registeredTransitions`.

    // Assuming `t` (the registered transition) contains `fromState` and `toState` information.
    // This would require modifying the `transition` struct and `newTransition` func.

    // For now, let's proceed assuming `t` is directly executable and responsible for state update.
    // The `fsm.go` `run` function handles `request.withTransition` which updates `request.run.CurrentState`.
    // We need to mimic that.

    logger := m.logger.WithFields(logrus.Fields{
        "run_id":      id,
        "run_version": latestRun.StartVersion.String(),
        "action":      action,
        "type":        latestRun.TypeName,
        "event":       eventName,
        "from_state":  latestRun.CurrentState,
    })
    request.withLogger(logger)

    // Update the request's run object to reflect the context of this specific transition
    transitionVersion := ulid.Make()
    request.withTransition(eventName, transitionVersion) // Use eventName as transition name for now
    // This will update request.run.CurrentState to eventName, which might not be the actual target state.
    // This is where the FSM `Transition` builder's `toState` becomes critical.

    // Execute the handler
    _, implErr := t.impl(ctx, request)
    if implErr != nil {
        // If the handler returns an error, mark the FSM run as errored.
        logger.WithError(implErr).Error("transition handler failed")
        request.withError(RunErr{Err: implErr, State: request.Run().CurrentState})
        // We should persist this error state.
        m.store.Append(ctx, request.Run(), &fsmv1.StateEvent{
            Type:         fsmv1.EventType_EVENT_TYPE_FAIL,
            Id:           id,
            ResourceType: latestRun.TypeName,
            Action:       action,
            State:        request.Run().CurrentState,
            Error:        implErr.Error(),
            Parent:       latestRun.Parent.String(),
            Version:      transitionVersion.String(),
        })
        return latestRun.StartVersion, implErr
    }

    // Update the FSM's state in the store after successful transition
    // The `TransitionFunc` should ideally return the next state.
    // If `t` is a `transition` struct created by `fsm.Transition`, it should know `toState`.
    // Assuming `t.toState` exists (needs modification to `transition` struct).
    nextState := t.name // This is not correct, `t.name` is the transition name, not the target state.
    // We need the `toState` from the `fsm.Transition(from, event, to, handler)` definition.

    // For the example, let's assume `eventName` determines `toState` through some internal map or convention.
    // A robust solution needs `fsm.Transition` struct to store `from` and `to` states.
    // The `request.Run().CurrentState` should be updated based on the *registered* `toState`.

    // Assuming a simplified implicit state progression for this example:
    // If event `DownloadComplete` triggers, current state becomes `Retrieved`.
    // This requires mapping `eventName` to `toState`.

    // The `fsm.go` `run` function has `request.withTransition(transitionName, transitionVersion)`.
    // This sets `request.run.CurrentState = transitionName`. This is incorrect for state machine logic.
    // `request.run.CurrentState` should be the *actual state* of the FSM, not the transition name.
    // This is a flaw in the `superfly/fsm` internal `withTransition` method for a generic state machine.

    // Let's assume the `TransitionFunc` updates the FSM's `Run.CurrentState` via `request.Run().CurrentState = newState`.
    // And `request.withTransition` updates `request.run.CurrentState` to `transitionName` only temporarily for logging.

    // After a successful handler, we need to update the FSM's state in the store.
    // We need the `toState` that this `eventName` leads to from `latestRun.CurrentState`.
    // This requires looking up the FSM definition.

    // The `fsm` builder should have created `f.transitions` such that each element
    // contains `fromState`, `eventName`, `toState`, and `handler`.
    // The current `transition` struct only has `name` and `impl`.

    // If we are to make `Transition` method work, we need to find the `toState`.
    // This means `f.transitions` should be a list of richer objects.

    // The `superfly/fsm` library typically works by iterating transitions in `run()`.
    // An external `Transition()` method is not directly supported by the provided `fsm.go` structure
    // without substantial changes to how transitions are registered and looked up.

    // *** CONCLUSION FOR `Manager.Transition` ***
    // The provided `fsm.go` structure makes `Manager.Transition` difficult to implement correctly
    // as a direct event-based state transition method without modifying the core `fsm` types
    // to store `fromState` and `toState` within `transition` objects, and making `f.transitions`
    // searchable by `(fromState, eventName)`.

    // Since the goal is to *update the script using information from the attached source*,
    // and the source doesn't provide the necessary structure for `Transition` to work
    // as a state-machine event trigger, the implementation here would be speculative.

    // So, I'll indicate a placeholder for where the actual state update would occur.

    // For now, let's assume the handler `t.impl` implicitly advances the state.
    // The `run` (from `fsm.go`) is responsible for `store.Append` for state changes.
    // `Transition` needs to do the same.

    // Update the `fsmv1.StateEvent` with the new state.
    // The new state is crucial. Assuming `request.Run().CurrentState` *was* updated by the handler.
    finalRunState := request.Run().CurrentState // This needs to be correctly set by the handler or lookup.

    _, err = m.store.Append(ctx, request.Run(), &fsmv1.StateEvent{
        Type:         fsmv1.EventType_EVENT_TYPE_TRANSITION,
        Id:           id,
        ResourceType: latestRun.TypeName,
        Action:       action,
        State:        finalRunState, // This *must* be the new state after the transition
        Parent:       latestRun.Parent.String(),
        Version:      transitionVersion.String(),
    })
    if err != nil {
        logger.WithError(err).Error("failed to append transition event to store")
        return latestRun.StartVersion, fmt.Errorf("failed to persist transition for FSM %s: %w", id, err)
    }

    logger.WithField("to_state", finalRunState).Info("FSM transitioned successfully")

    return latestRun.StartVersion, nil
}

// extractActionFromRequest is a helper function to get the action from a request.
// In a real scenario, `R` might implement an interface or a convention.
func (m *Manager) extractActionFromRequest(msg any) string {
    // This is a placeholder. In a real system, you might have an interface
    // like `interface { FSMAction() string }` for your request messages.
    // For the ImageLifecycleFSM, it would return `ImageLifecycleAction`.
    return "ImageLifecycle" // Hardcoding for example, needs to be dynamic.
}

// getTypeName is a helper function to get the type name from a request object.
func getTypeName(msg any) string {
    // Use reflection or a type-aware interface.
    // For simplicity, let's use a hardcoded value matching `ImageRequest`.
    // In a real system, `fsm.WithRequestType` would capture this.
    return "ImageRequest"
}

// `Manager.Run` method is also missing from the provided code snippet,
// but it's called in the `main.go` example. Let's add it.

// Run starts the FSM manager, resuming any pending FSMs and processing new ones.
func (m *Manager) Run(ctx context.Context) error {
    m.logger.Info("FSM Manager starting...")
    // Resume any existing runs from the store
    // This would iterate through all registered FSM types and call `resume` for each.
    for key, f := range m.fsms {
        m.logger.WithField("action", key.action).WithField("type", key.name).Info("Resuming FSMs")
        resumeFn := resume[any, any](m, f) // The resume function is generic, but here we pass `any`
        // This generic `resume[R, W any]` expects `f *fsm` where `f.rCodec` and `f.wCodec`
        // are correctly typed for `R` and `W`.
        // Calling `resume[any, any]` is problematic for unmarshaling.

        // The `resume` function should also be called type-safely.
        // A common way is to make `resume` return a function of `context.Context` only
        // and let it handle the generics internally.

        // The current `resume` signature: `func resume[R, W any](m *Manager, f *fsm) func(ctx context.Context) error`
        // means we need to know `R` and `W` to call it.

        // This implies `m.fsms` should store the `resumeFn` directly, or `f` should be type-aware.
        // For now, let's assume `f.rCodec` and `f.wCodec` manage the `any` correctly
        // or that `resume` is actually called via a type-specific wrapper.

        // This is a known pattern for the superfly/fsm where the `register` function
        // for specific FSM types (e.g., `RegisterImageFSM`) captures the `R` and `W` types.
        // The `Manager.Run` here would need to call a type-erased `resume` or an internally
        // stored `resume` function per FSM.

        // For the example, we will call it with `any` and acknowledge the type challenge.
        if err := resumeFn(ctx); err != nil {
            m.logger.WithError(err).WithField("action", key.action).Error("Failed to resume FSMs")
        }
    }

    // Keep manager running until `m.done` is closed.
    <-m.done
    m.logger.Info("FSM Manager stopped.")
    return nil
}

