package log

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	api "github.com/Afrawles/Qute/api/v1"
	"google.golang.org/protobuf/proto"
)


const (
	fileFormat = "%010d%s"
)

type segment struct {
	store *store
	index *index
	baseOffset uint64
	nextOffset uint64
	config Config
}


func newSegment(dir string, baseOffset uint64, cfg Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config: cfg,
	}

	storeFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf(fileFormat, baseOffset, ".store")), 
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644)
	if err != nil {
		return  nil, err
	} 

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf(fileFormat, baseOffset, ".index")), 
		os.O_CREATE|os.O_RDWR, 
		0644)

	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, cfg); err != nil {
		return nil, err
	}

	if off, _, err := s.index.readLast(); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil

}

// write adds message to segment and 
// returns offset
func (s *segment) write(message *api.Message) (uint64, error) {
	if s.isFull() {
		return 0, io.EOF 
	}
	cur := s.nextOffset
	message.Offset = cur

	p, err := proto.Marshal(message)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	relOffset := uint32(s.nextOffset - s.baseOffset)
	if err = s.index.writeAt(relOffset, pos); err != nil {
		return 0, err
	}

	s.nextOffset++

	return cur, nil
} 

// read returns message/ record for given offset
func (s *segment) read(off uint64) (*api.Message, error) {
	if off < s.baseOffset {
		return nil, fmt.Errorf("offset before base offset")
	}

	message := &api.Message{}
	relOffset := off - s.baseOffset 

	_, pos, err := s.index.readAt(relOffset)
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	err = proto.Unmarshal(p, message)

	return message, err

}

func (s *segment) close() error {
	if err := s.index.close(); err != nil {
		return err
	}
	return s.store.Close()
}

func (s *segment) remove() error {
	if err := s.close(); err != nil {
		return err
	}

	if exists(s.index.Name()) {
		if err := os.Remove(s.index.Name()); err != nil {
			return err
		}
	}

	if exists(s.store.file.Name()) {
		if err := os.Remove(s.store.file.Name()); err != nil {
			return err
		} 
	}

	return nil
}

func (s *segment) isFull() bool {
	return s.config.Segment.MaxIndexBytes <= s.index.size || s.store.size >= s.config.Segment.MaxStoreBytes
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func roundDownToMultiple(number, multiple uint64) uint64 {
	return multiple * (number/multiple)
}
