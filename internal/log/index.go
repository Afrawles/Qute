package log

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offsetWidth uint64 = 4
	positionWidth uint64 = 8
	entryWidth = offsetWidth + positionWidth
)

type index struct {
	mmap gommap.MMap
	file *os.File
	size uint64
}

func newIndex(f *os.File, cfg Config) (*index, error) {
	idx := &index{file: f}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())

	if err = os.Truncate(f.Name(), int64(cfg.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE,gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	return idx, nil
}

// ReadAt takes in offset & returns the relative 
// offset, postion in store
func (idx *index) readAt(offset int64) (uint32, uint64, error) {
	pos := uint64(offset) * entryWidth
	if idx.size == 0 {
		return 0, 0, io.EOF
	}

	if idx.size < pos + entryWidth {
		return 0, 0, io.EOF
	}

	relOffset := binary.BigEndian.Uint32(idx.mmap[pos:pos+offsetWidth])
	position := binary.BigEndian.Uint64(idx.mmap[pos+offsetWidth:pos+entryWidth])

	return relOffset, position, nil
}

// Write appends relative offset & psotion in idx
func (idx *index) writeAt(offset uint32, pos uint64) error {
	if idx.mmapCapacity() < idx.size+entryWidth {
		return io.EOF
	}

	binary.BigEndian.PutUint32(idx.mmap[idx.size:idx.size+offsetWidth], offset)
	binary.BigEndian.PutUint64(idx.mmap[idx.size+offsetWidth:idx.size+entryWidth], pos)

	idx.size += entryWidth
	return nil
}

func (idx *index) sync() error {
	if err := idx.mmap.Sync(gommap.MS_SYNC); err!=nil {
		return err
	}

	if err := idx.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (idx *index) shrink() error {
	return idx.file.Truncate(int64(idx.size))
}

func (idx *index) Name() string {
	return idx.file.Name()
}

func (idx *index) close() error {
	if err := idx.sync(); err != nil {
		return err
	} 

	if err := idx.shrink(); err != nil {
		return err
	}

	return idx.file.Close()
}

func (idx *index) mmapCapacity() uint64 {
	return uint64(len(idx.mmap))
}
