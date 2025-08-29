package fsm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/hashicorp/go-memdb"
	"github.com/oklog/ulid/v2"
	"github.com/sirupsen/logrus"
	"go.etcd.io/bbolt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

const (
	stateDB   = "fsm-state.db"
	historyDB = "fsm-history.db"
)

var (
	activeBucket   = []byte("ACTIVE")
	archiveBucket  = []byte("ARCHIVE")
	eventsBucket   = []byte("EVENTS")
	childrenBucket = []byte("CHILDREN")
	historyBucket  = []byte("HISTORY")

	keySeparator = []byte("#")
	emptyPrefix  = []byte{}

	errInvalidEventType = errors.New("invalid event type")

	errEventArchived = errors.New("event archived")
)

type store struct {
	logger logrus.FieldLogger

	tracer trace.Tracer

	cancel context.CancelFunc

	db *bbolt.DB

	history *bbolt.DB

	memDB *memdb.MemDB

	// archiveCh is only used in tests to signal the archive loop to run.
	archiveCh chan struct{}
}

func newStore(logger logrus.FieldLogger, tracer trace.Tracer, path string, memDB *memdb.MemDB) (*store, error) {
	db, err := bbolt.Open(filepath.Join(path, stateDB), 0o600, &bbolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(activeBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(eventsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(archiveBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(childrenBucket); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	history, err := bbolt.Open(filepath.Join(path, historyDB), 0o600, &bbolt.Options{
		Timeout: 1 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &store{
		logger:    logger,
		tracer:    tracer,
		cancel:    cancel,
		archiveCh: make(chan struct{}),
		db:        db,
		history:   history,
		memDB:     memDB,
	}

	go s.archive(ctx)

	return s, nil
}

func (s *store) Close() error {
	s.logger.Info("shutting down store")

	var err error
	s.cancel()

	s.logger.Info("waiting for archive loop to finish")
	<-s.archiveCh
	s.logger.Info("archive loop finished")

	if err := s.db.Close(); err != nil {
		err = fmt.Errorf("failed to close state db, %w", err)
	}

	if err := s.history.Close(); err != nil {
		err = errors.Join(err, fmt.Errorf("failed to close history db, %w", err))
	}

	return err
}

type archiveEvent struct {
	archiveKey []byte

	historyEvent *fsmv1.HistoryEvent
}

func (s *store) archive(ctx context.Context) {
	// TODO: clear out date buckets older than X days
	runArchive := func(ctx context.Context) {
		ctx, rootSpan := s.tracer.Start(ctx, "store.archive")
		defer rootSpan.End()

		archiveEvents := map[string][]*archiveEvent{}

		_, gatherSpan := s.tracer.Start(ctx, "store.archive.gather")
		s.db.View(func(tx *bbolt.Tx) error {
			return tx.Bucket(archiveBucket).ForEach(func(k, v []byte) error {
				var ae fsmv1.ActiveEvent
				if err := proto.Unmarshal(v, &ae); err != nil {
					s.logger.WithError(err).Error("failed to unmarshal active event")
					gatherSpan.RecordError(err)
					return nil
				}

				var version ulid.ULID
				if err := version.UnmarshalText(ae.StartVersion); err != nil {
					s.logger.WithError(err).Error("failed to unmarshal version")
					gatherSpan.RecordError(err)
					// TODO: delete active event
					return nil
				}

				var se fsmv1.StateEvent
				err := proto.Unmarshal(tx.Bucket(eventsBucket).Get(ae.EndEvent), &se)
				if err != nil {
					s.logger.WithError(err).Error("failed to unmarshal end event")
					gatherSpan.RecordError(err)
					// TODO: delete archive event
					return nil
				}

				startTime := ulid.Time(version.Time())
				startDate := startTime.Format(time.DateOnly)

				toArchive, ok := archiveEvents[startDate]
				if !ok {
					toArchive = []*archiveEvent{}
				}
				archiveEvents[startDate] = append(toArchive, &archiveEvent{
					archiveKey: k,
					historyEvent: &fsmv1.HistoryEvent{
						ActiveEvent: &ae,
						LastEvent:   &se,
					},
				})
				return nil
			})
		})
		gatherSpan.End()
		switch {
		case ctx.Err() != nil:
			s.logger.Info("context canceled, exiting archive loop")
			return
		case len(archiveEvents) == 0:
			s.logger.Debug("no active events to archive")
			return
		default:
		}

		_, processSpan := s.tracer.Start(ctx, "store.archive.process",
			trace.WithAttributes(attribute.Int("archive_events", len(archiveEvents))),
		)
		defer processSpan.End()
		for date, events := range archiveEvents {
			if ctx.Err() != nil {
				s.logger.Info("context canceled, exiting archive loop")
				return
			}

			todayBucket := bytes.Join([][]byte{historyBucket, []byte(date)}, keySeparator)
			s.logger.WithField("date", date).WithField("count", len(events)).Info("archiving events")

			// NOTE: we don't care about the error here
			s.history.Update(func(tx *bbolt.Tx) error {
				historyB, err := tx.CreateBucketIfNotExists(todayBucket)
				if err != nil {
					s.logger.WithError(err).Error("failed to create history bucket")
					return err
				}

				for _, event := range events {
					historyEvent := event.historyEvent
					historyBytes, err := proto.Marshal(historyEvent)
					if err != nil {
						s.logger.WithError(err).Error("failed to marshal history event")
						processSpan.RecordError(err)
						continue
					}
					historyB.Put(historyEvent.GetActiveEvent().GetStartEvent(), historyBytes)
				}
				return nil
			})

			for _, event := range events {
				if ctx.Err() != nil {
					s.logger.Info("context canceled, exiting archive loop")
					return
				}

				childrenToDelete := [][]byte{}
				parentPrefix := bytes.Join([][]byte{event.archiveKey, emptyPrefix}, keySeparator)
				s.db.View(func(tx *bbolt.Tx) error {
					childrenC := tx.Bucket(childrenBucket).Cursor()
					for k, _ := childrenC.Seek(parentPrefix); k != nil && bytes.HasPrefix(k, parentPrefix); k, _ = childrenC.Next() {
						childrenToDelete = append(childrenToDelete, k)
					}
					return nil
				})

				if ctx.Err() != nil {
					s.logger.Info("context canceled, exiting archive loop")
					return
				}

				eventsToDelete := [][]byte{}
				s.db.View(func(tx *bbolt.Tx) error {
					startEvent := event.historyEvent.GetActiveEvent().GetStartEvent()
					endEvent := event.historyEvent.GetActiveEvent().GetEndEvent()
					eventB := tx.Bucket(eventsBucket)
					eventC := eventB.Cursor()
					for k, _ := eventC.Seek(startEvent); k != nil && bytes.Compare(k, endEvent) <= 0; k, _ = eventC.Next() {
						eventsToDelete = append(eventsToDelete, k)
					}
					return nil
				})

				if ctx.Err() != nil {
					s.logger.Info("context canceled, exiting archive loop")
					return
				}
				s.db.Update(func(tx *bbolt.Tx) error {
					eventsB := tx.Bucket(eventsBucket)
					for _, k := range eventsToDelete {
						if err := eventsB.Delete(k); err != nil {
							s.logger.WithError(err).Error("failed to delete event")
							processSpan.RecordError(err)
						}
					}

					childrenB := tx.Bucket(childrenBucket)
					for _, k := range childrenToDelete {
						if err := childrenB.Delete(k); err != nil {
							s.logger.WithError(err).Error("failed to delete child")
							processSpan.RecordError(err)
						}
					}

					if err := tx.Bucket(archiveBucket).Delete(event.archiveKey); err != nil {
						s.logger.WithError(err).Error("failed to delete archive event")
						processSpan.RecordError(err)
					}

					return nil
				})
			}
		}
	}

	defer close(s.archiveCh)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("context canceled, exiting archive loop")
			return
		case <-s.archiveCh:
			s.logger.Info("archive loop signaled to run")
		case <-time.After(1 * time.Minute):
			s.logger.Info("running event archive")
		}
		runArchive(ctx)
	}
}

type activeResource struct {
	version ulid.ULID

	active *fsmv1.ActiveEvent

	completedTransitions []string

	response []byte

	retryCount uint64

	fsmError RunErr
}

func (s *store) Active(ctx context.Context, f *fsm) ([]*activeResource, error) {
	var (
		resourceType         = f.typeName
		activeEvents         []*activeResource
		completedTransitions []string
		// "<resource_name>#"
		resourcePrefixKey = bytes.Join([][]byte{[]byte(resourceType), emptyPrefix}, keySeparator)
	)

	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(activeBucket)
		eventB := tx.Bucket(eventsBucket)

		cursor := b.Cursor()
		for k, v := cursor.Seek(resourcePrefixKey); k != nil && bytes.HasPrefix(k, resourcePrefixKey); k, v = cursor.Next() {
			logger := s.logger.WithField("key", string(k))
			var ae fsmv1.ActiveEvent
			if err := proto.Unmarshal(v, &ae); err != nil {
				logger.WithError(err).Error("failed to unmarshal active event")
				continue
			}

			if ae.EndEvent != nil {
				logger.Info("active event has end event, skipping")
				continue
			}

			var version ulid.ULID
			if err := version.UnmarshalText(ae.StartVersion); err != nil {
				logger.WithError(err).Error("failed to unmarshal version")
				continue
			}

			// EVENT Bucket
			// <resource_id>#<action>#<run_version>
			eventPrefix := bytes.Join([][]byte{[]byte(ae.GetResourceId()), []byte(ae.GetAction()), ae.StartVersion, emptyPrefix}, keySeparator)
			eventCursor := eventB.Cursor()
			logger.WithField("start_event", string(ae.StartEvent)).WithField("event_prefix", string(eventPrefix)).Info("iterating events")
			var (
				response   []byte
				retryCount uint64
				fsmError   RunErr
			)
			for eventKey, eventValue := eventCursor.Seek(ae.StartEvent); eventKey != nil && bytes.HasPrefix(eventKey, eventPrefix); eventKey, eventValue = eventCursor.Next() {
				var event fsmv1.StateEvent
				if err := proto.Unmarshal(eventValue, &event); err != nil {
					logger.WithError(err).Error("failed to unmarshal event")
					continue
				}

				switch event.Type {
				case fsmv1.EventType_EVENT_TYPE_COMPLETE:
					completedTransitions = append(completedTransitions, event.GetState())
					if event.GetResponse() != nil {
						response = event.GetResponse()
					}
				case fsmv1.EventType_EVENT_TYPE_CANCEL:
					completedTransitions = append(completedTransitions, event.GetState())
					fsmError = RunErr{
						Err:   errors.New(event.GetError()),
						State: event.GetState(),
					}
				case fsmv1.EventType_EVENT_TYPE_ERROR:
					retryCount = event.GetRetryCount()
				}
			}

			activeEvents = append(activeEvents, &activeResource{
				version:              version,
				active:               &ae,
				completedTransitions: completedTransitions,
				response:             response,
				retryCount:           retryCount,
				fsmError:             fsmError,
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	txn := s.memDB.Txn(true)
	defer txn.Abort()
	for _, ae := range activeEvents {
		rs := runState{
			Run: Run{
				ID:           ae.active.GetResourceId(),
				StartVersion: ae.version,
				Action:       f.action,
				ResourceName: f.alias,
				TypeName:     f.typeName,
				Queue:        f.queue,
				Parent:       f.parent,
				fsmErr:       ae.fsmError,
			},
			State: fsmv1.RunState_RUN_STATE_PENDING,
		}
		if err := txn.Insert(fsmTable, rs); err != nil {
			return nil, err
		}
	}
	txn.Commit()

	return activeEvents, nil
}

type appendOptionFunc func(*appendOption) error

type appendOption struct {
	delayUntil int64

	runAfter []byte

	parent []byte

	start *startOption
}

type startOption struct {
	transitions []string

	resource []byte
}

func withDelayUntil(delayUntil time.Time) appendOptionFunc {
	return func(opt *appendOption) error {
		if !delayUntil.IsZero() {
			opt.delayUntil = delayUntil.Unix()
		}
		return nil
	}
}

func withStartOption(resource []byte, transitions []string) appendOptionFunc {
	return func(opt *appendOption) error {
		opt.start = &startOption{
			resource:    resource,
			transitions: transitions,
		}
		return nil
	}
}

func withRunAfter(version ulid.ULID) appendOptionFunc {
	return func(opt *appendOption) error {
		if version.Compare(ulid.ULID{}) == 0 {
			return nil
		}
		runAfter, err := version.MarshalText()
		if err != nil {
			return err
		}
		opt.runAfter = runAfter
		return nil
	}
}

func withParent(parent ulid.ULID) appendOptionFunc {
	return func(opt *appendOption) error {
		if parent.Compare(ulid.ULID{}) == 0 {
			return nil
		}
		parentBytes, err := parent.MarshalText()
		if err != nil {
			return err
		}
		opt.parent = parentBytes
		return nil
	}
}

func (s *store) Append(ctx context.Context, run Run, event *fsmv1.StateEvent, queue string, opts ...appendOptionFunc) (ulid.ULID, error) {
	var ao appendOption
	for _, opt := range opts {
		if err := opt(&ao); err != nil {
			return ulid.ULID{}, err
		}
	}

	if ao.start == nil && event.GetType() == fsmv1.EventType_EVENT_TYPE_START {
		return ulid.ULID{}, errors.New("start option must be set")
	}

	if run.StartVersion.Compare(ulid.ULID{}) == 0 {
		return ulid.ULID{}, errors.New("runVersion must be set")
	}
	runVersionBytes, err := run.StartVersion.MarshalText()
	if err != nil {
		return ulid.ULID{}, err
	}
	event.RunVersion = runVersionBytes

	eventVersion := ulid.Make()
	version, err := eventVersion.MarshalText()
	if err != nil {
		return ulid.ULID{}, err
	}

	eventBytes, err := proto.Marshal(event)
	if err != nil {
		return ulid.ULID{}, err
	}

	eventIdentifier := bytes.Join([][]byte{
		[]byte(event.GetId()),
		[]byte(event.GetAction()),
	}, keySeparator)

	// EVENT Bucket
	// <resource_id>#<action>#<run_version>#<event_version>
	eventKey := bytes.Join([][]byte{eventIdentifier, event.GetRunVersion(), version}, keySeparator)

	// ACTIVE Bucket
	// <resource_name>#<resource_id>#<action>#<run_version_or_empty>
	activeKeyVersion := ulid.ULID{}
	if queue != "" {
		activeKeyVersion = run.StartVersion
	}

	activeKeyVersionBytes, err := activeKeyVersion.MarshalText()
	if err != nil {
		return ulid.ULID{}, err
	}

	aeEventKey := bytes.Join([][]byte{[]byte(event.GetResourceType()), eventIdentifier, activeKeyVersionBytes}, keySeparator)
	var aeEventBytes []byte
	if event.GetType() == fsmv1.EventType_EVENT_TYPE_START {
		ae := &fsmv1.ActiveEvent{
			StartEvent:   eventKey,
			StartVersion: runVersionBytes,
			Action:       event.GetAction(),
			ResourceId:   event.GetId(),
			Resource:     ao.start.resource,
			Transitions:  ao.start.transitions,
			Options: &fsmv1.EventOptions{
				DelayUntil: ao.delayUntil,
				RunAfter:   ao.runAfter,
				Queue:      queue,
				Parent:     ao.parent,
			},
			TraceContext: map[string]string{},
		}
		(propagation.TraceContext{}).Inject(ctx, propagation.MapCarrier(ae.TraceContext))

		aeEventBytes, err = proto.Marshal(ae)
		if err != nil {
			return ulid.ULID{}, err
		}
	}

	rs := runState{
		Run: run,
	}

	txn := s.memDB.Txn(true)
	defer txn.Abort()
	err = s.db.Update(func(tx *bbolt.Tx) error {
		activeB := tx.Bucket(activeBucket)
		eventB := tx.Bucket(eventsBucket)

		switch event.GetType() {
		case fsmv1.EventType_EVENT_TYPE_START:
			// NOTE: we only allow one active event per resource unless it's getting queued
			if queue == "" && activeB.Get(aeEventKey) != nil {
				var ae fsmv1.ActiveEvent
				if err := proto.Unmarshal(activeB.Get(aeEventKey), &ae); err != nil {
					return err
				}

				if ae.EndEvent == nil {
					var sv ulid.ULID
					if err := sv.UnmarshalText(ae.StartVersion); err != nil {
						return err
					}
					return &AlreadyRunningError{Version: sv}
				}
			}

			if err := activeB.Put(aeEventKey, aeEventBytes); err != nil {
				return err
			}

			if err := eventB.Put(eventKey, eventBytes); err != nil {
				return err
			}

			if ao.parent != nil {
				// <parent_run_version>#<child_run_version>
				parentKey := bytes.Join([][]byte{ao.parent, runVersionBytes}, keySeparator)
				if err := tx.Bucket(childrenBucket).Put(parentKey, runVersionBytes); err != nil {
					return err
				}
			}

			if queue == "" {
				switch deleted, err := txn.DeleteAll(fsmTable, runIndex, run.ID); {
				case err != nil:
					return err
				case deleted > 0:
					s.logger.WithField("id", run.ID).Info("deleted existing run")
				}
			} else {
				switch iter, err := txn.Get(fsmTable, runIndex, run.ID); {
				case err != nil:
					return err
				default:
					for next := iter.Next(); next != nil; next = iter.Next() {
						rs := next.(runState)
						if rs.State != fsmv1.RunState_RUN_STATE_COMPLETE {
							continue
						}
						if err := txn.Delete(fsmTable, next.(runState)); err != nil {
							return err
						}
					}
				}

			}

			rs.State = fsmv1.RunState_RUN_STATE_PENDING
			if err := txn.Insert(fsmTable, rs); err != nil {
				return fmt.Errorf("failed to update state: %w", err)
			}

			return nil
		case fsmv1.EventType_EVENT_TYPE_ERROR,
			fsmv1.EventType_EVENT_TYPE_COMPLETE,
			fsmv1.EventType_EVENT_TYPE_CANCEL:
			if err := eventB.Put(eventKey, eventBytes); err != nil {
				return err
			}

			return nil
		case fsmv1.EventType_EVENT_TYPE_FINISH:
			if err := eventB.Put(eventKey, eventBytes); err != nil {
				return err
			}

			activeResource := activeB.Get(aeEventKey)
			if activeResource == nil {
				s.logger.WithField("key", string(aeEventKey)).Warn("active event not found")
				return nil
			}

			var ae fsmv1.ActiveEvent
			if err := proto.Unmarshal(activeResource, &ae); err != nil {
				return err
			}
			ae.EndEvent = eventKey

			activeVersionBytes, err := proto.Marshal(&ae)
			if err != nil {
				return err
			}

			if err := activeB.Delete(aeEventKey); err != nil {
				return err
			}

			// ARCHIVE Bucket
			// <run_version>
			if err := tx.Bucket(archiveBucket).Put(runVersionBytes, activeVersionBytes); err != nil {
				return err
			}

			rs.State = fsmv1.RunState_RUN_STATE_COMPLETE
			rs.Error = run.fsmErr
			if err := txn.Insert(fsmTable, rs); err != nil {
				return fmt.Errorf("failed to update state: %w", err)
			}

			return nil
		default:
			return fmt.Errorf("%T: %w", event.Type, errInvalidEventType)
		}
	})
	if err != nil {
		s.logger.WithError(err).Error("failed to append event")
		return ulid.ULID{}, err
	}
	txn.Commit()

	return eventVersion, nil
}

func (s *store) History(ctx context.Context, runVersion ulid.ULID) (*fsmv1.HistoryEvent, error) {
	runVersionBytes, err := runVersion.MarshalText()
	if err != nil {
		return nil, err
	}

	var historyEvent fsmv1.HistoryEvent
	err = s.db.View(func(tx *bbolt.Tx) error {
		archive := tx.Bucket(archiveBucket)

		aeBytes := archive.Get(runVersionBytes)
		if aeBytes == nil {
			// Lookup in History
			date := ulid.Time(runVersion.Time()).Format(time.DateOnly)
			historyBucket := bytes.Join([][]byte{historyBucket, []byte(date)}, keySeparator)
			return s.history.View(func(tx *bbolt.Tx) error {
				historyB := tx.Bucket(historyBucket)
				if historyB == nil {
					return fmt.Errorf("history bucket not found, %s, %w", date, ErrFsmNotFound)
				}

				historyBytes := historyB.Get(runVersionBytes)
				if historyBytes == nil {
					return fmt.Errorf("history event not found, %s, %w", runVersion, ErrFsmNotFound)
				}

				if err := proto.Unmarshal(historyBytes, &historyEvent); err != nil {
					s.logger.WithError(err).Error("failed to unmarshal history event")
					return err
				}

				return nil
			})
		}

		var ae fsmv1.ActiveEvent
		if err := proto.Unmarshal(aeBytes, &ae); err != nil {
			s.logger.WithError(err).Error("failed to unmarshal active event")
			return err
		}
		historyEvent.ActiveEvent = &ae

		var se fsmv1.StateEvent
		err := proto.Unmarshal(tx.Bucket(eventsBucket).Get(ae.EndEvent), &se)
		if err != nil {
			s.logger.WithError(err).Error("failed to unmarshal end event")
			return err
		}
		historyEvent.LastEvent = &se

		return nil

	})
	return &historyEvent, err
}

func (s *store) Children(ctx context.Context, parent ulid.ULID) ([]ulid.ULID, error) {
	parentyBytes, err := parent.MarshalText()
	if err != nil {
		return nil, err
	}

	children := []ulid.ULID{}
	err = s.db.View(func(tx *bbolt.Tx) error {
		childrenB := tx.Bucket(childrenBucket)
		cursor := childrenB.Cursor()
		parentPrefix := bytes.Join([][]byte{parentyBytes, emptyPrefix}, keySeparator)
		for k, v := cursor.Seek(parentPrefix); k != nil && bytes.HasPrefix(k, parentPrefix); k, v = cursor.Next() {
			if v == nil {
				continue
			}

			var child ulid.ULID
			if err := child.UnmarshalText(v); err != nil {
				return err
			}
			children = append(children, child)
		}
		return nil
	})
	return children, err
}
