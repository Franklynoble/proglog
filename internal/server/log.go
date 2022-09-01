package server

import (
	"fmt"
	"sync"
)

type Log struct {
	mu      sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

/*
To append a record to the log, you just append to the slice. Each time we read
a record given an index,
*/
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	/*
		we use that index to look up the record in the slice.
		If the offset given by the client doesn’t exist, we return an error
	*/
	if offset >= uint64(len(c.records)) {

		return Record{}, ErrOfsetNotFound
	}
	return c.records[offset], nil

}

var ErrOfsetNotFound = fmt.Errorf("Offset not found")
