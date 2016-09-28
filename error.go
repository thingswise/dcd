package main

type OperationError struct {
	Message string
	Type    int
}

const (
	UnknownErrorType  = -1
	InternalError     = 1
	NotCheckedOut     = 2
	AlreadyCheckedOut = 3
	CheckoutMismatch  = 4
	UnknownFile       = 5
	InvalidRequest    = 6
)

func NewOperationError(t int, message string) *OperationError {
	return &OperationError{
		Message: message,
		Type:    t,
	}
}

func (e *OperationError) Error() string {
	return e.Message
}

func GetErrorType(err error) int {
	if e, ok := err.(*OperationError); ok {
		return e.Type
	} else {
		return UnknownErrorType
	}
}

type ErrorMessage struct {
	Message string `json:"message"`
}
