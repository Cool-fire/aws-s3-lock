package s3lock

type ErrorCode int

const (
	None ErrorCode = iota
	LockAlreadyOwned
	MultipleSavesInProgress
	TooSlowAbandoned
	NoLockAcquired
	DuringLockRelease
)

func NewS3LockError(errorCode ErrorCode, msg string) S3LockError {
	return S3LockError{
		Code: errorCode,
		Msg: msg,
	}
}

type S3LockError struct {
	Code ErrorCode
	Msg string
}

func (u S3LockError) Error() string {
	return u.Msg
}