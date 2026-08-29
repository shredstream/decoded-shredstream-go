package decodedshredstream

import (
	"errors"
	"fmt"
)

var (
	ErrAuthRefused = errors.New("authentication refused (check your token and product)")

	ErrKicked = errors.New("kicked by operator — do not reconnect in a loop")

	ErrInvalidFilter = errors.New("invalid filter")

	ErrClosed = errors.New("client closed")
)

type filterError struct{ msg string }

func (e *filterError) Error() string { return "invalid filter: " + e.msg }

func (e *filterError) Is(target error) bool { return target == ErrInvalidFilter }

func invalidFilterf(format string, args ...any) error {
	return &filterError{msg: fmt.Sprintf(format, args...)}
}
