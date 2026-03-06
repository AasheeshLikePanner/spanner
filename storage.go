package main

import (
	"sort"
	"sync"
)

type VersionedValue struct {
	Value     string
	Timestamp int64
}

type Store struct {
	mu   sync.RWMutex
	data map[string][]VersionedValue // key → history of values
}

func NewStore() *Store {
	return &Store{
		data: make(map[string][]VersionedValue),
	}
}

func (s *Store) Write(key string, value string, ts int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = append(s.data[key], VersionedValue{
		Value:     value,
		Timestamp: ts,
	})

	vals := s.data[key]
	sort.Slice(vals, func(i, j int) bool {
		return vals[i].Timestamp < vals[j].Timestamp
	})

	s.data[key] = vals
}

func (s *Store) LastTS(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	values := s.data[key]
	if len(values) == 0 {
		return 0
	}
	return values[len(values)-1].Timestamp
}

func (s *Store) ReadAt(key string, ts int64) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	values := s.data[key]
	result := ""
	found := false
	for _, v := range values {
		if v.Timestamp == ts {
			result = v.Value
			found = true
		}
	}
	return result, found
}