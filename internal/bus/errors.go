package bus

import (
	"errors"
	"fmt"
)

// Typed errors. Callers must be able to distinguish "lost the race" from
// "transport failed" (audit Avoid #4) — ErrCASLost maps to ClaimLost, any
// other error maps to ClaimError.
var (
	// ErrCASLost means another writer legitimately won the compare-and-set.
	ErrCASLost = errors.New("bus: cas lost")
	// ErrNoSuchKey means the key does not exist (or its TTL expired).
	ErrNoSuchKey = errors.New("bus: no such key")
	// ErrClosed means the client or server has been closed by its owner.
	ErrClosed = errors.New("bus: closed")
	// ErrDisconnected means the connection dropped; the client is
	// reconnecting in the background and the operation may be retried.
	ErrDisconnected = errors.New("bus: disconnected")
)

// RequestError is a typed server-side rejection of a request.
type RequestError struct {
	Code    string
	Message string
}

func (e *RequestError) Error() string {
	return fmt.Sprintf("bus: %s: %s", e.Code, e.Message)
}

// errorFromFrame maps a response frame error onto typed sentinel errors.
func errorFromFrame(fe *frameError) error {
	if fe == nil {
		return &RequestError{Code: "missing_error", Message: "response not ok but carried no error"}
	}
	switch fe.Code {
	case errCodeCASLost:
		return ErrCASLost
	case errCodeNoSuchKey:
		return ErrNoSuchKey
	default:
		return &RequestError{Code: fe.Code, Message: fe.Message}
	}
}
