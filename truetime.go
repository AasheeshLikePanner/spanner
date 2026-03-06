package main

import "time"

type TrueTime struct {
	uncertainty time.Duration
}

type Interval struct {
	earliest time.Time
	latest   time.Time
}

func (tt *TrueTime) Now() Interval {
    now := time.Now()
    return Interval{
        earliest: now.Add(-tt.uncertainty),  
        latest:   now.Add(tt.uncertainty),   
    }
}

func (tt *TrueTime) After(t time.Time) bool {
    return tt.Now().earliest.After(t)  
}
