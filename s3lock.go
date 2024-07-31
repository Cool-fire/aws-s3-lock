package s3lock

import (
	"errors"
	"fmt"
	"time"

	"github.com/Cool-fire/aws-s3-lock/store"
)


const ACQUIRE_LOCK_MAX_DURATION_IN_MINUTES int = 2

type IsLockAcquired bool

type S3Lock struct {
	maxHoldTimeMins int
	rw store.LockReadWriter
}


func New(awsBucketName string, awsLockFolder string, lockName string, maxHoldTimeMins int) (*S3Lock, error) {
	s3Opts := store.S3StoreOpts{
		AwsBucketName: awsBucketName,
		AwsLockFolder: awsLockFolder,
		LockName: lockName,
	}
	s3rw, err  := store.NewS3Store(s3Opts)
	if err != nil {
		return nil, fmt.Errorf("error creating S3lock %w", err)
	}

	if maxHoldTimeMins <= 0 {
		return nil, fmt.Errorf("max hold time should be greater than 2 mins")
	}

	return &S3Lock{
		rw: s3rw,
		maxHoldTimeMins: maxHoldTimeMins,
	}, nil	
} 

func (s *S3Lock) AcquireLock(newOwnerName string) (IsLockAcquired, *S3LockError) {

	// Initial lock counter
	ilc, err  := s.rw.GetLockCounter()
	if err != nil {
		return false, NewS3LockError(NoLockAcquired, err.Error())
	}

	// Lock Owner - Initial check + main lock acquire
	clo, err := s.rw.GetLockOwner()
	if err != nil {
		return false, NewS3LockError(NoLockAcquired, err.Error())
	}

	var isLockExpired bool = false
	if (clo == nil || clo.GetRemainingTimeinSeconds() <= int64(s.maxHoldTimeMins) * 60) {
		isLockExpired = true
	}

	if clo == nil || clo.Name == newOwnerName || isLockExpired {
		acquireLockDurationInMins := (s.maxHoldTimeMins + ACQUIRE_LOCK_MAX_DURATION_IN_MINUTES)
		lockExprytimeAsEpoch := time.Now().Add(time.Minute * time.Duration(acquireLockDurationInMins)).Unix()
		clo = &store.LockOwner{
			Name: newOwnerName,
			ExpiryTime: lockExprytimeAsEpoch,
		}

		if err := s.rw.SetLockOwner(*clo); err != nil {
			return false, NewS3LockError(NoLockAcquired, fmt.Sprintf("Unable to set the new lock owner %s", newOwnerName))
		}
	} else {
		return false, NewS3LockError(LockAlreadyOwned, fmt.Sprintf("lock is currently held by owner %s, wait for %d seconds before retrying", clo.Name, clo.GetRemainingTimeinSeconds()))
	}

	clc, err := s.rw.GetLockCounter()
	if err != nil {
		return false, NewS3LockError(NoLockAcquired, err.Error())
	}

	// Revert 
	if ((ilc == nil && clc == nil)|| (ilc.Counter == clc.Counter)) {
		var newCounter int = 0
		if clc != nil {
			newCounter = clc.Counter + 1
		}

		nlc := store.LockCounter {
			Counter: newCounter,
		}
		if err := s.rw.SetLockCounter(nlc); err != nil {
			defer s.releaseOwner(newOwnerName)
			return false, NewS3LockError(NoLockAcquired, "error updating the lock counter while acquiring the lock")
		}
	} else {
		defer s.releaseOwner(newOwnerName)
		return false, NewS3LockError(MultipleSavesInProgress,  "There is another attempting to acquire the lock at the same time. Please retry.")
	}

	// Lock Owner - Final Check
	clo, err = s.rw.GetLockOwner()
	if err != nil {
		return false, NewS3LockError(NoLockAcquired, err.Error())
	}

	if clo == nil {
		return false, NewS3LockError(NoLockAcquired, "Lock is not currently held by anyone but should be")
	} else if (newOwnerName != clo.Name) {
		return false, NewS3LockError(LockAlreadyOwned, fmt.Sprintf("Lock is currentlyheld by owner %s, wait for %d seconds before retrying", clo.Name, clo.GetRemainingTimeinSeconds()))
	} else if (clo.GetRemainingTimeinSeconds() <= int64(s.maxHoldTimeMins) * 60) {
		return false, NewS3LockError(TooSlowAbandoned, fmt.Sprintf("Acquiring the lock took too long, you potentially do not have enough time to perform your opertion limit set to %d minutes", clo.GetRemainingTimeinSeconds()))
	}
	
	return true, nil
}

func (s *S3Lock) releaseOwner(newOwnerName string) {
	// Rolling back owner can potentially throw error, in which case lock will exist until timestamp expires
	currentOwner, _ := s.rw.GetLockOwner()
	if currentOwner != nil && currentOwner.Name == newOwnerName {
		s.rw.RollBackLockOwner()
	}
}

func (s *S3Lock) GetLockOwner() (*store.LockOwner, error) {
	clo, err  := s.rw.GetLockOwner()
	if err != nil {
		return nil, errors.New("Error getting the current lock owner")
	}

	// This will return nil if there is no one holding the lock
	return clo, nil
}

func (s *S3Lock) ReleaseLock(expectedCurrentOwnerName string) error {
	clo, err  := s.rw.GetLockOwner()
	if err != nil {
		return errors.New("Error releasing the lock")
	}

	if clo != nil && clo.Name == expectedCurrentOwnerName {
		if err = s.rw.RollBackLockOwner(); err != nil {
			return errors.New("Error releasing the lock")
		}
	}

	return nil
}