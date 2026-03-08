package main

import "time"

type TxnState int

const (
	ACTIVE TxnState = iota
	PREPARING
	COMMITTED
	ABORTED
)

type Write struct {
	Key   string
	Value string
	Group string
}

type Transaction struct {
	id      string
	writes  []Write
	state   TxnState
	startTS int64
	tt      TrueTime
}

type PaxosGroup struct {
	id      string
	store   *Store
	oracle  *TimestampOracle
	lockMgr *LockManager
	node    *Node
}

func NewTransaction(id string, tt TrueTime) *Transaction {
	return &Transaction{
		id:      id,
		startTS: tt.Now().earliest.UnixNano(),
		state:   ACTIVE,
		writes:  make([]Write, 0),
	}
}

func NewPaxosGroup(id string, node *Node) *PaxosGroup {
	return &PaxosGroup{
		id:      id,
		store:   NewStore(),
		oracle:  NewTimestampOracle(TrueTime{uncertainty: 5 * time.Millisecond}),
		lockMgr: NewLockManager(),
		node:    node,
	}
}

func (tx *Transaction) BufferWrite(key string, value string, group string) {
	tx.writes = append(tx.writes, Write{
		Key:   key,
		Value: value,
		Group: group,
	})
}

func (pg *PaxosGroup) Prepare(txnID string, writes []Write, startTS int64) (int64, error) {
	for _, w := range writes {
		err := pg.lockMgr.Acquire(w.Key, WRITE, txnID, startTS, make(chan error, 1))
		if err != nil {
			return 0, err
		}
	}
	prepareTimeStamp := pg.oracle.Assign()
	for _, w := range writes {
		pg.oracle.RegisterPrepare(w.Key, prepareTimeStamp)
	}
	return prepareTimeStamp, nil
}

func (pg *PaxosGroup) CommitTxn(txnID string, ts int64, writes []Write) {
	for _, w := range writes {
		pg.store.Write(w.Key, w.Value, ts)
	}
	for _, w := range writes {
		pg.oracle.ClearPrepare(w.Key)
	}
	pg.oracle.AdvancePaxosSafe(ts)
	pg.lockMgr.Release(txnID)
}

func (pg *PaxosGroup) AbortTxn(txnID string, writes []Write) {
	for _, w := range writes {
		pg.oracle.ClearPrepare(w.Key)
	}
	pg.lockMgr.Release(txnID)
}

func (tx *Transaction) Commit(groups map[string]*PaxosGroup) error {
	tx.state = PREPARING
	groupByPaxos := make(map[string][]Write)
	for _, w := range tx.writes {
		groupByPaxos[w.Group] = append(groupByPaxos[w.Group], w)
	}
	prepareTimestamps := make(map[string]int64)
	var prepareErr error

	for groupID, writes := range groupByPaxos {
		pg := groups[groupID]
		ts, err := pg.Prepare(tx.id, writes, tx.startTS)
		if err != nil {
			prepareErr = err
			break
		}
		prepareTimestamps[groupID] = ts
	}
	if prepareErr != nil {
		for groupId, writes := range groupByPaxos {
			pg := groups[groupId]
			pg.AbortTxn(tx.id, writes)
		}
		tx.state = ABORTED
		return prepareErr
	}
	var finalTS int64
	for _, ts := range prepareTimestamps {
		if ts > finalTS {
			finalTS = ts
		}
	}
	now := tx.tt.Now().latest.UnixNano()
	if now > finalTS {
		finalTS = now
	}
	for !tx.tt.After(time.Unix(0, finalTS)) {
		time.Sleep(1 * time.Millisecond)
	}
	for groupID, writes := range groupByPaxos {
		pg := groups[groupID]
		pg.CommitTxn(tx.id, finalTS, writes)
	}
	tx.state = COMMITTED
	return nil
}
