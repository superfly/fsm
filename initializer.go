package fsm

import (
	"context"

	fsmv1 "github.com/superfly/fsm/gen/fsm/v1"

	"github.com/hashicorp/go-memdb"
)

func setStarted[R, W any](db *memdb.MemDB) func(context.Context, *Request[R, W]) context.Context {
	return func(ctx context.Context, req *Request[R, W]) context.Context {
		var (
			run    = req.Run()
			logger = req.Log()
			rs     = runState{
				Run:   run,
				State: fsmv1.RunState_RUN_STATE_RUNNING,
			}
			txn = db.Txn(true)
		)
		defer txn.Abort()

		if err := txn.Insert(fsmTable, rs); err != nil {
			logger.WithError(err).Error("failed to update fsm state store")
			// return nil, err
		}
		txn.Commit()

		return ctx
	}
}
