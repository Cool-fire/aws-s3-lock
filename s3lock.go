package s3lock

import "errors"


const ACQUIRE_LOCK_MAX_DURATION_IN_MINUTES int = 2

type Intializabel interface {
	Init() error
}

type S3Lock struct {
	awsBucketName string
	awsLockFolder string
	lockName string
}

func New(awsBucketName string, awsLockFolder string, lockName string) *S3Lock {
	return &S3Lock{
		awsBucketName: awsBucketName,
		awsLockFolder: awsLockFolder,
		lockName: lockName,
	}
} 

func (s *S3Lock) Init() error {
	if s.awsBucketName == "" || s.awsLockFolder == "" || s.lockName == "" {
		return errors.New("lock not properly configured")
	}
	return nil
}

func (s *S3Lock) acquireLock(newOwnerName string) {

}

func (s *S3Lock) getLockStatus(expectedLockOwnerName string) {

}

func releaseLock(expectedCurrentOwnerName string) {
	
}