package main

import (
	"fmt"
	"time"
)

type Client struct {
	groups map[string]*PaxosGroup
	tt TrueTime
}

func NewClient(groups map[string]*PaxosGroup, tt TrueTime) *Client {
	return &Client{
		groups: groups,
		tt: tt,
	}
}

func (c *Client) Write(key string, value string, groupID string) error {
	id := fmt.Sprintf("%d", time.Now().UnixNano());
	tx := NewTransaction(id, c.tt)
	tx.BufferWrite(key, value, groupID)
	return tx.Commit(c.groups)
}

func (c *Client) Read(key string, groupID string) (string, bool) {
	group := c.groups[groupID]
	lastTs := group.store.LastTS(key);
	group.oracle.waitUntilSafe(key, lastTs);
	val, ok := group.store.ReadAt(key, lastTs)
	if !ok {
		return "", false
	}
	return val, true
}

func (c *Client) RunTransaction(fn func(*Transaction)) error {
	id := fmt.Sprintf("%d", time.Now().UnixNano());
	tx := NewTransaction(id, c.tt)
	fn(tx)
	return tx.Commit(c.groups)
}

func (c *Client) ReadOnly(keys []string, groups []string) (map[string]string, error) {
	var sread int64

	for _, groupID := range groups {
		group := c.groups[groupID]
		for _, key := range keys {
			ts := group.store.LastTS(key)
			if ts > sread {
				sread = ts
			}
		}
	}

	results := make(map[string]string)

	for _, groupID := range groups {
		group := c.groups[groupID]

		for _, key := range keys {
			group.oracle.waitUntilSafe(key, sread)
			
			val, ok := group.store.ReadAt(key, sread)
			if ok {
				results[key] = val
			}
		}
	}

	return results, nil
}
