package s3lock

import (
	"fmt"

	"github.com/Cool-fire/aws-s3-lock/store"
)


const ACQUIRE_LOCK_MAX_DURATION_IN_MINUTES int = 2

type IsLockAcquired bool

type S3Lock struct {
	rw store.LockReadWriter
}


func New(awsBucketName string, awsLockFolder string, lockName string) (*S3Lock, error) {
	s3Opts := store.S3StoreOpts{
		AwsBucketName: awsBucketName,
		AwsLockFolder: awsLockFolder,
		LockName: lockName,
	}
	s3rw, err  := store.NewS3Store(s3Opts)
	if err != nil {
		return nil, fmt.Errorf("error creating S3lock %w", err)
	}

	return &S3Lock{
		rw: s3rw,
	}, nil	
} 

func (s *S3Lock) acquireLock(newOwnerName string) (IsLockAcquired, error) {
	_, err  := s.rw.GetLockCounter()
	if err != nil {
		return false, fmt.Errorf("unable to acquire lock %w", err)
	}

	// TODO
	return false, nil 

}

func (s *S3Lock) getLockStatus(expectedLockOwnerName string) {

}

func releaseLock(expectedCurrentOwnerName string) {

}