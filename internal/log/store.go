package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

//enc defines the encoding that
//we persist record sizes and index entries in and lenWidth defines the number
// of bytes used to store the record’s length.

var (
	enc = binary.BigEndian
)

// lenWidth defines the number
// of bytes used to store the record’s length.
const (
	lenWidth = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())

	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil

}

// Append([]byte) persists the given bytes to the store.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()

	defer s.mu.Unlock()

	//We write to the buffered writer instead of directly to the file to reduce the
	// number of system calls and improve performance.
	//
	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil

	/* Then we return the number of bytes
	written, which similar Go APIs conventionally do, and the position where the
	store holds the record in its file. The segment will use this position when it
	creates an associated index entry for this record
	*/
}

/*

Read(pos uint64) returns the record stored
at the given position. First it flushes
the writer buffer, in case we’re about
 to try to read a record that the buffer
hasn’t flushed to disk yet

*/

/*
We find out how many bytes we have to read to
get the whole record, and then we fetch and return the record. The compiler
allocates byte slices that don’t escape the functions they’re declared in on the
stack

*/

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()

	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)

	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))

	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

//Close() persists any buffered data before closing the file
func (s *store) Close() error {

	s.mu.Lock()

	defer s.mu.Unlock()

	err := s.buf.Flush()

	if err != nil {
		return err
	}
	return s.File.Close()
}


