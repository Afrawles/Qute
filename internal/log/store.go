// Package log implements a simple append-only log with segmented files.
package log

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync"
)


const (
	lenWidth = 8
	crcWidth = 4
)

var (
	hasher = crc32.ChecksumIEEE
	enc = binary.BigEndian
)

var (
	ErrCorrupted = errors.New("record corrupted: CRC mismatch")
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
	
	checksum := hasher(p)

	if err := binary.Write(s.buf, enc, checksum); err != nil {
		return 0, 0, err
	}

	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	nn, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	nn += lenWidth + crcWidth 
	s.size += uint64(nn)

	return uint64(nn), pos, nil
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
	
	header := make([]byte, lenWidth+crcWidth)
	if _, err := s.file.ReadAt(header, int64(pos)); err != nil {
		return nil, err
	}

	storedCRC := enc.Uint32(header[:crcWidth])
	length := enc.Uint64(header[crcWidth:]) 

	data := make([]byte, length)

	if _, err := s.file.ReadAt(data, int64(pos+lenWidth+crcWidth)); err != nil {
		return nil, err
	} 

	if hasher(data) != storedCRC {
		return nil, ErrCorrupted
	}

	return data, nil
}

// ReadAt reads len(p) bytes from the store starting at pos into p.
// Returns the number of bytes read and any error.
func (s *store) ReadAt(p []byte, pos uint64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.file.ReadAt(p, int64(pos))
}


// Close flushes any remaining buffered data and closes the file.
// Returns any error from flush or close
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}

	return s.file.Close()
}
