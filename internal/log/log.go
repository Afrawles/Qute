package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/Afrawles/Qute/api/v1"
)

const (
	defaultSegmentMaxByteSize = 100 << 20 // 100 MBs
	defaultIndexMaxByteSize = 100 << 20 // 100 MBs
)

type messageLog struct {
	mu sync.RWMutex
	dir string
	config Config
	segments []*segment
	activeSegment *segment
}

type storeReader struct {
	*store
	off uint64
}

func newMessageLog(dir string, cfg Config) (*messageLog, error) {
	if cfg.Segment.MaxStoreBytes == 0 {
		cfg.Segment.MaxStoreBytes = defaultSegmentMaxByteSize
	}

	if cfg.Segment.MaxIndexBytes == 0 {
		cfg.Segment.MaxIndexBytes = defaultIndexMaxByteSize
	}

	l := &messageLog{
		dir: dir,
		config: cfg,
	}

	return  l, l.setup()

}

func (l *messageLog) setup() error {

	if l.dir == "" {
		return errors.New("could not create log directory")
	}
	err := os.MkdirAll(l.dir, 0755)
	if err != nil {
		return err
	}

	files, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}

	seen := make(map[uint64]struct{})
	var baseoffsets []uint64

	for _, file := range files {
		fp := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, err := strconv.ParseUint(fp, 10, 0)
		if err != nil {
			continue
		}

		if _, ok := seen[off]; !ok {
			seen[off] = struct{}{}
			baseoffsets = append(baseoffsets, off)
		}
	}

	slices.Sort(baseoffsets) 

	for _, off := range baseoffsets {
		s, err := newSegment(l.dir, off, l.config)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, s)
		l.activeSegment = s
	}

	if l.segments == nil {
		s, err := newSegment(l.dir, l.config.Segment.InitialOffset, l.config)
		if err != nil {
			return err
		}

		l.segments = append(l.segments, s)
		l.activeSegment = s

	}

	return nil
}


func (l *messageLog) Read(off uint64) (*api.Message, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	
	pos := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > off
	}) - 1

	if  pos < 0 {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	s := l.segments[pos]
	if s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	return s.read(uint64(pos))
}

func (l *messageLog) Append(message *api.Message) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.write(message)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.isFull() {
		s, err := newSegment(l.dir, off+1, l.config)
		if err != nil {
			return 0, err
		}
		l.segments = append(l.segments, s)
		l.activeSegment = s
	}

	return off, nil

}

func (l *messageLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.close(); err != nil {
			return err
		}
	}
	 return nil
}

func (l *messageLog) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.dir)
}


func (l *messageLog) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

func (l *messageLog) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

func (l *messageLog) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}

	return off-1, nil
}

func (l *messageLog) Truncate(lowest uint64) error {
    l.mu.Lock()
    defer l.mu.Unlock()

    var keep []*segment
    for _, s := range l.segments {
        if s.nextOffset <= lowest+1 {
            if err := s.remove(); err != nil {
                return err
            }
        } else {
            keep = append(keep, s)
        }
    }
    l.segments = keep
    return nil
}

func (l *messageLog) NewReader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, 0, len(l.segments))
	for _, s := range l.segments {
		sr := &storeReader{s.store, 0}
		readers = append(readers, sr)
	}

	return io.MultiReader(readers...)

}

func (sr *storeReader) Read(p []byte) (int, error) {
	n, err := sr.ReadAt(p, sr.off)
	sr.off += uint64(n)

	return  n, err
}
