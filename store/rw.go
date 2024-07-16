package store

import (
	"time"
)

type LockReadWriter interface {
	GetLockOwner() (*LockOwner, error)
	SetLockOwner(LockOwner) error
	SetLockCounter(int) error 
	GetLockCounter() (int, error)
}

type LockOwner struct {
	Name string `json:"name"`
	ExpiryTime int64 `json:"expiryTime"` 
}

func (l LockOwner) getRemainingTimeinSeconds() int64 {
	return time.Now().Unix() - l.ExpiryTime
}

