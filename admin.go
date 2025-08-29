package fsm

import (
	"context"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"
	"github.com/superfly/fsm/gen/fsm/v1/fsmv1connect"

	"connectrpc.com/connect"
	"github.com/oklog/ulid/v2"
)

var _ fsmv1connect.FSMServiceHandler = (*adminServer)(nil)

type adminServer struct {
	m *Manager
}

func (s *adminServer) ListRegistered(context.Context, *connect.Request[fsmv1.ListRegisteredRequest]) (*connect.Response[fsmv1.ListRegisteredResponse], error) {
	fsms := make([]*fsmv1.FSM, 0, len(s.m.fsms))
	for _, fsm := range s.m.fsms {
		f := &fsmv1.FSM{
			Action:      fsm.action,
			TypeName:    fsm.typeName,
			Alias:       fsm.alias,
			StartState:  fsm.startState,
			EndState:    fsm.endState,
			Transitions: fsm.transitionSlice(),
		}
		fsms = append(fsms, f)
	}

	return connect.NewResponse(&fsmv1.ListRegisteredResponse{
		Fsms: fsms,
	}), nil
}

func (s *adminServer) ListActive(context.Context, *connect.Request[fsmv1.ListActiveRequest]) (*connect.Response[fsmv1.ListActiveResponse], error) {
	txn := s.m.db.Txn(false)
	defer txn.Abort()

	it, err := txn.Get(fsmTable, idIndex)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	active := []*fsmv1.ActiveFSM{}
	for next := it.Next(); next != nil; next = it.Next() {
		rs := next.(runState)
		if rs.State == fsmv1.RunState_RUN_STATE_COMPLETE {
			continue
		}
		af := &fsmv1.ActiveFSM{
			Id:           rs.ID,
			Action:       rs.Action,
			Version:      rs.StartVersion.String(),
			RunState:     rs.State,
			CurrentState: rs.CurrentState,
			Queue:        rs.Queue,
		}
		if rs.TransitionVersion.Compare(ulid.ULID{}) != 0 {
			af.TransitionVersion = rs.TransitionVersion.String()
		}
		// TODO - What should we do about Error.State here?
		if rs.Error.Err != nil {
			af.Error = rs.Error.Err.Error()
		}
		active = append(active, af)
	}
	return connect.NewResponse(&fsmv1.ListActiveResponse{
		Active: active,
	}), nil
}

func (s *adminServer) GetHistoryEvent(ctx context.Context, req *connect.Request[fsmv1.GetHistoryEventRequest]) (*connect.Response[fsmv1.HistoryEvent], error) {
	runVersion, err := ulid.Parse(req.Msg.GetRunVersion())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	he, err := s.m.store.History(ctx, runVersion)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&fsmv1.HistoryEvent{
		ActiveEvent: he.GetActiveEvent(),
		LastEvent:   he.GetLastEvent(),
	}), nil
}
