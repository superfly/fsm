package fsm

import (
	"fmt"

	"github.com/oklog/ulid/v2"
)

type ulidIndexer struct {
	fieldFn func(rs runState) ulid.ULID
}

func (ulidIndexer) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("wrong number of args %d, expected 1", len(args))
	}

	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("wrong type for arg %T, expected string", args[0])
	}

	return ulid.MustParse(s).Bytes(), nil
}

func (u ulidIndexer) FromObject(raw interface{}) (bool, []byte, error) {
	s, ok := raw.(runState)
	if !ok {
		return false, nil, fmt.Errorf("wrong type for arg %T, expected runState", raw)
	}

	val := u.fieldFn(s)
	if val.Compare(ulid.ULID{}) == 0 {
		return false, nil, nil
	}

	return true, val.Bytes(), nil
}

func (ulidIndexer) PrefixFromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("wrong number of args %d, expected 1", len(args))
	}

	s, ok := args[0].(ulid.ULID)
	if !ok {
		return nil, fmt.Errorf("wrong type for arg %T, expected ulid.ULID", args[0])
	}

	return s.Bytes(), nil
}
