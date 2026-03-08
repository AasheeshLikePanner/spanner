package main

import (
	"fmt"
	"sync"
	"time"
)

type Mode int

const (
	READ Mode = iota
	WRITE
)

type LockManager struct {
	locks map[string]*LockEntry
	mu    sync.Mutex
}

type LockEntry struct {
	txnID   string
	ts      int64
	mode    Mode
	woundCh chan error
	waiters []chan error
}

func (lm *LockManager) Acquire(key string, mode Mode, txnid string, ts int64, wait chan error) error {
	lm.mu.Lock()

	le, ok := lm.locks[key]
	if !ok {
		lm.locks[key] = &LockEntry{txnID: txnid, mode: mode, ts: ts, woundCh: wait,
			waiters: []chan error{}}
		lm.mu.Unlock()
		return nil
	}
	
	if le.ts < ts {
		le.waiters = append(le.waiters, wait)
		lm.mu.Unlock()
		
		select {
		case err := <-wait:
			return err
		case <-time.After(100 * time.Millisecond):
			return fmt.Errorf("timeout waiting for lock on %s", key)
		}
	} else {
		le.woundCh <- fmt.Errorf("wounded by older txn")
		lm.locks[key] = &LockEntry{txnID: txnid, mode: mode, ts: ts, woundCh: wait,
			waiters: le.waiters}
		lm.mu.Unlock()
		return nil
	}
}

func (lm *LockManager) Release(txnID string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for key, entry := range lm.locks {
		if entry.txnID == txnID {
			for _, waiterCh := range entry.waiters {
				waiterCh <- nil
			}
			delete(lm.locks, key)
		}
	}
}

func NewLockManager() *LockManager {
	return &LockManager{
		locks: make(map[string]*LockEntry),
	}
}

func (lm *LockManager) IsLocked(key string, txnID string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	entry, ok := lm.locks[key]
	if !ok {
		return false
	}
	return entry.txnID != txnID
}
