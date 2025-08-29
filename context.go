package fsm

import "context"

type contextKey string

func (c contextKey) String() string {
	return string(c)
}

var (
	retryContextKey     = contextKey("retry")
	isRestartContextKey = contextKey("is-restart")
)

func withRetry(ctx context.Context, count uint64) context.Context {
	return context.WithValue(ctx, retryContextKey, count)
}

func RetryFromContext(ctx context.Context) uint64 {
	v := ctx.Value(retryContextKey)
	if v == nil {
		return 0
	}
	return v.(uint64)
}

func withRestart(ctx context.Context, restart bool) context.Context {
	return context.WithValue(ctx, isRestartContextKey, restart)
}

func IsRestartFromContext(ctx context.Context) bool {
	v := ctx.Value(isRestartContextKey)
	if v == nil {
		return false
	}
	return v.(bool)
}
