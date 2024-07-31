package store

import (
	"time"
)

type LockReadWriter interface {
	GetLockOwner() (*LockOwner, error)
	SetLockOwner(LockOwner) error
	SetLockCounter(LockCounter) error 
	GetLockCounter() (*LockCounter, error)
	RollBackLockOwner() error 
}

type LockOwner struct {
	Name string `json:"name"`
	ExpiryTime int64 `json:"expiryTime"` 
}

func (l LockOwner) GetRemainingTimeinSeconds() int64 {
	return time.Now().Unix() - l.ExpiryTime
}

type LockCounter struct {
	Counter int
} 
