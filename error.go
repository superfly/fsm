package fsm

import (
	"errors"
	"fmt"

	"github.com/oklog/ulid/v2"
)

var (
	ErrFsmNotFound = errors.New("FSM not found")
)

type AlreadyRunningError struct {
	Version ulid.ULID
}

func (e *AlreadyRunningError) Error() string {
	return fmt.Sprintf("FSM already running, version = %s", e.Version.String())
}

// haltError wraps the underlying error returned from a transition and signals the FSM to halt
// execution.
type haltError struct {
	err error
}

func (e *haltError) Error() string {
	return e.err.Error()
}

func (e *haltError) Unwrap() error {
	return e.err
}

func halt(err error) error {
	if err == nil {
		return nil
	}
	return &haltError{err: err}
}

// AbortError signals that the transition should not be retried and FSM aborted.
type AbortError struct {
	err error
}

func (e *AbortError) Error() string {
	return e.err.Error()
}

func (e *AbortError) Unwrap() error {
	return e.err
}

// Abort wraps the given err in a *AbortError.
func Abort(err error) error {
	if err == nil {
		return nil
	}
	return &AbortError{err: err}
}

type HandoffError struct {
	NewFSM ulid.ULID
}

func (e *HandoffError) Error() string {
	return fmt.Sprintf("FSM handoff to %s", e.NewFSM.String())
}

// Handoff wraps the given err in a *HandoffError.
func Handoff(version ulid.ULID) error {
	if (ulid.ULID{}).Compare(version) == 0 {
		return nil
	}
	return &HandoffError{
		NewFSM: version,
	}
}

type UnexpectedStateError struct {
	CurrentState  string
	ExpectedState string
}

func (e *UnexpectedStateError) Error() string {
	return fmt.Sprintf("invalid state, start_state = %s, current_state = %s", e.ExpectedState, e.CurrentState)
}

type ErrorKind string

func (k ErrorKind) String() string {
	return string(k)
}

const (
	ErrorKindSystem = "system"
	ErrorKindUser   = "user"
)

type UnrecoverableError struct {
	error error
	Kind  ErrorKind
}

func NewUnrecoverableSystemError(err error) *UnrecoverableError {
	return &UnrecoverableError{error: err, Kind: ErrorKindSystem}
}

func NewUnrecoverableUserError(err error) *UnrecoverableError {
	return &UnrecoverableError{error: err, Kind: ErrorKindUser}
}

func (e *UnrecoverableError) Error() string {
	return e.error.Error()
}

func (e *UnrecoverableError) Unwrap() error {
	return e.error
}
