package main

import (
	"math"
	"sync"
	"time"
)

type TimestampOracle struct {
	mu 				sync.Mutex
	tt				TrueTime
	smax			int64
	minNextTS		map[int]int64
	tPaxosSafe		int64
	tTMSafe			map[string]int64
	prepared		map[string]int64
}

func NewTimestampOracle(tt TrueTime) *TimestampOracle {
	return &TimestampOracle{
		tt: tt,
		smax: 0,
		minNextTS: make(map[int]int64),
		tPaxosSafe: 0,
		tTMSafe: make(map[string]int64),
		prepared: make(map[string]int64),
	}
}

func (o *TimestampOracle) Assign() int64 {

	o.mu.Lock()
	defer o.mu.Unlock()
	localSmax := o.tt.Now().latest.UnixNano()

	if localSmax <= o.smax {
		localSmax = o.smax + 1
	}
	o.smax = localSmax
	return o.smax
}

func (o *TimestampOracle) AdvancePaxosSafe(ts int64){
	o.mu.Lock()
	defer o.mu.Unlock()

	if ts > o.tPaxosSafe {
		o.tPaxosSafe = ts
	}
}

func (o *TimestampOracle) RegisterPrepare(key string, ts int64){
	o.mu.Lock()
	defer o.mu.Unlock()

	if ts > o.prepared[key] {
		o.prepared[key] = ts
	}
}

func (o *TimestampOracle) ClearPrepare(key string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	delete(o.prepared, key)
}

func (o *TimestampOracle) TSafe(key string) int64{
	o.mu.Lock()
    defer o.mu.Unlock()
	tTMSafe := int64(math.MaxInt64) 
    if ts, ok := o.prepared[key]; ok {
        tTMSafe = ts - 1
    }
    return min(o.tPaxosSafe, tTMSafe)
}

func (o *TimestampOracle) waitUntilSafe(key string, ts int64){
	for {
		safe := o.TSafe(key)
		if safe >= ts {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
}