package log

import (
	"io"
	"testing"

	api "github.com/Afrawles/Qute/api/v1"
	"github.com/Afrawles/Qute/internal/assert"
)

func TestSegment(t *testing.T) {
	baseOffset := uint64(16)
	msg := &api.Message{Value: []byte("testing segment")}

	t.Run("write and read messages", func(t *testing.T) {
		dir := t.TempDir()
		var cfg Config
		cfg.Segment.MaxStoreBytes = 1024
		cfg.Segment.MaxIndexBytes = entryWidth * 3

		s, err := newSegment(dir, baseOffset, cfg)
		assert.Equal(t, err, nil)
		assert.Equal(t, s.nextOffset, baseOffset)
		assert.Equal(t, s.isFull(), false)

		for i := uint64(0); i < 3; i++ {
			off, err := s.write(msg)
			assert.Equal(t, err, nil)
			assert.Equal(t, off, baseOffset+i)

			got, err := s.read(off)
			assert.Equal(t, err, nil)
			assert.Equal(t, got.Value, msg.Value)
		}
	})

	t.Run("segment is maxed by index", func(t *testing.T) {
		dir := t.TempDir()
		var cfg Config
		cfg.Segment.MaxStoreBytes = 1024
		cfg.Segment.MaxIndexBytes = entryWidth * 3

		s, err := newSegment(dir, baseOffset, cfg)
		assert.Equal(t, err, nil)

		for i := uint64(0); i < 3; i++ {
			_, err := s.write(msg)
			assert.Equal(t, err, nil)
		}
		assert.Equal(t, s.isFull(), true)
	})

	t.Run("segment is maxed by store", func(t *testing.T) {
		dir := t.TempDir()
		var cfg Config
		cfg.Segment.MaxStoreBytes = uint64(len(msg.Value) + 8) * 3
		cfg.Segment.MaxIndexBytes = 1024 

		s, err := newSegment(dir, baseOffset, cfg)
		assert.Equal(t, err, nil)

		for i := 0; i < 3; i++ {
			_, err := s.write(msg)
			assert.Equal(t, err, nil)
		}
		assert.Equal(t, s.isFull(), true)
	})

	t.Run("segment rebuilds state from existing files", func(t *testing.T) {
		dir := t.TempDir()
		var cfg Config
		cfg.Segment.MaxStoreBytes = 1024
		cfg.Segment.MaxIndexBytes = entryWidth * 10

		s, err := newSegment(dir, baseOffset, cfg)
		assert.Equal(t, err, nil)

		for i := uint64(0); i < 3; i++ {
			off, err := s.write(msg)
			assert.Equal(t, err, nil)
			assert.Equal(t, off, baseOffset+i)
		}
		err = s.close()
		assert.Equal(t, err, nil)

		s2, err := newSegment(dir, baseOffset, cfg)
		assert.Equal(t, err, nil)
		assert.Equal(t, s2.nextOffset, baseOffset+3)
		assert.Equal(t, s2.isFull(), false)

		got, err := s2.read(baseOffset)
		assert.Equal(t, err, nil)
		assert.Equal(t, got.Value, msg.Value)
	})

	t.Run("remove cleans up files", func(t *testing.T) {
		dir := t.TempDir()
		var cfg Config
		cfg.Segment.MaxStoreBytes = 1024
		cfg.Segment.MaxIndexBytes = entryWidth * 3

		s, err := newSegment(dir, baseOffset, cfg)
		assert.Equal(t, err, nil)

		_, err = s.write(msg)
		assert.Equal(t, err, nil)

		err = s.remove()
		assert.Equal(t, err, nil)

		s2, err := newSegment(dir, baseOffset, cfg)
		assert.Equal(t, err, nil)
		assert.Equal(t, s2.nextOffset, baseOffset)
		assert.Equal(t, s2.isFull(), false)
	})

	t.Run("read returns error for offset before base", func(t *testing.T) {
		dir := t.TempDir()
		var cfg Config
		cfg.Segment.MaxStoreBytes = 1024
		cfg.Segment.MaxIndexBytes = entryWidth * 3

		s, err := newSegment(dir, baseOffset, cfg)
		assert.Equal(t, err, nil)

		_, err = s.read(baseOffset - 1)
		assert.Equal(t, err != nil, true)
	})

	t.Run("write returns EOF when segment is full", func(t *testing.T) {
		dir := t.TempDir()
		var cfg Config
		cfg.Segment.MaxStoreBytes = 1024
		cfg.Segment.MaxIndexBytes = entryWidth * 3

		s, err := newSegment(dir, baseOffset, cfg)
		assert.Equal(t, err, nil)

		for i := 0; i < 3; i++ {
			_, err := s.write(msg)
			assert.Equal(t, err, nil)
		}

		_, err = s.write(msg)
		assert.Equal(t, err, io.EOF)
	})
}
