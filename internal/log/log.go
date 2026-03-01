package log

import (
	"errors"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
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


func (l *messageLog) Read(off uint64)
