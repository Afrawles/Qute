// Package log implements a simple append-only log with segmented files.
package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

type store struct {
	file *os.File
	buf *bufio.Writer
	size uint64
	mu sync.Mutex
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	s := &store{
		file: f,
		buf: bufio.NewWriter(f),
		size: uint64(fi.Size()),
	} 

	return s, nil
}

// Append adds bytes to the end of the store file.
// Returns: bytes written, start position, error
func (s *store) Append(p []byte)(uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos := s.size
	if err := binary.Write(s.buf, binary.BigEndian, uint64(len(p))); err != nil {
		return 0, 0, nil
	}
	nn, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, nil
	}

	total := uint64(nn) + 8 // 8 -> prefix length
	return total, pos, nil
}

// Read reads a record from the store at the given position.
// pos is the starting offset of the length prefix (8 bytes).
// Returns the record data or an error.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	
	size:= make([]byte, 8)
	if _, err := s.file.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint64(size)
	data := make([]byte, length) 
	if _, err := s.file.ReadAt(data, int64(pos+8)); err != nil {
		return nil, err
	} 

	return data, nil
}

// ReadAt reads len(p) bytes from the store starting at pos into p.
// Returns the number of bytes read and any error.
func (s *store) ReadAt(p []byte, pos uint64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.Flush(); err != nil {
		return 0, nil
	}

	return s.file.ReadAt(p, int64(pos))
}

// Flush writes any buffered data to the underlying file.
// Returns any error encountered during flush
func (s *store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.buf.Flush()
}

// Close flushes any remaining buffered data and closes the file.
// Returns any error from flush or close
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.Flush(); err != nil {
		return err
	}

	return s.file.Close()
}
