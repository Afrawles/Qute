package log

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Afrawles/Qute/internal/assert"
)

func TestIndex(t *testing.T) {
	var cfg Config
	cfg.Segment.MaxIndexBytes = 512

	t.Run("append and read multiple entries", func(t *testing.T) {
		dir := t.TempDir()
		f, err := os.OpenFile(filepath.Join(dir, "index_test"), os.O_CREATE|os.O_RDWR, 0644)
		assert.Equal(t, err, nil)
		defer f.Close()

		idx, err := newIndex(f, cfg)
		assert.Equal(t, err, nil)

		entries := []struct {
			offset uint32
			pos    uint64
		}{
			{0, 0},
			{1, 100},
			{2, 250},
			{3, 500},
		}

		for _, entry := range entries {
			err = idx.writeAt(entry.offset, entry.pos)
			assert.Equal(t, err, nil)
		}

		for i, entry := range entries {
			gotOffset, gotPos, err := idx.readAt(int64(i))
			assert.Equal(t, err, nil)
			assert.Equal(t, gotOffset, entry.offset)
			assert.Equal(t, gotPos, entry.pos)
		}
	})

	t.Run("read returns EOF when index is empty", func(t *testing.T) {
		dir := t.TempDir()
		f, err := os.OpenFile(filepath.Join(dir, "index_test"), os.O_CREATE|os.O_RDWR, 0644)
		assert.Equal(t, err, nil)
		defer f.Close()

		idx, err := newIndex(f, cfg)
		assert.Equal(t, err, nil)

		_, _, err = idx.readAt(0)
		assert.Equal(t, err, io.EOF)
	})

	t.Run("read returns EOF when reading beyond size", func(t *testing.T) {
		dir := t.TempDir()
		f, err := os.OpenFile(filepath.Join(dir, "index_test"), os.O_CREATE|os.O_RDWR, 0644)
		assert.Equal(t, err, nil)
		defer f.Close()

		idx, err := newIndex(f, cfg)
		assert.Equal(t, err, nil)

		err = idx.writeAt(0, 100)
		assert.Equal(t, err, nil)

		_, _, err = idx.readAt(1)
		assert.Equal(t, err, io.EOF)

		_, _, err = idx.readAt(5)
		assert.Equal(t, err, io.EOF)
	})

	t.Run("index rebuilds state from existing file", func(t *testing.T) {
		dir := t.TempDir()
		filepath := filepath.Join(dir, "index_test")

		f, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0644)
		assert.Equal(t, err, nil)

		idx, err := newIndex(f, cfg)
		assert.Equal(t, err, nil)

		err = idx.writeAt(0, 0)
		assert.Equal(t, err, nil)

		err = idx.writeAt(1, 200)
		assert.Equal(t, err, nil)

		err = idx.writeAt(2, 400)
		assert.Equal(t, err, nil)

		err = idx.close()
		assert.Equal(t, err, nil)

		f2, err := os.OpenFile(filepath, os.O_RDWR, 0644)
		assert.Equal(t, err, nil)
		defer f2.Close()

		idx2, err := newIndex(f2, cfg)
		assert.Equal(t, err, nil)

		assert.Equal(t, idx2.size, uint64(3*entryWidth))

		offset, pos, err := idx2.readAt(0)
		assert.Equal(t, err, nil)
		assert.Equal(t, offset, uint32(0))
		assert.Equal(t, pos, uint64(0))

		offset, pos, err = idx2.readAt(1)
		assert.Equal(t, err, nil)
		assert.Equal(t, offset, uint32(1))
		assert.Equal(t, pos, uint64(200))

		offset, pos, err = idx2.readAt(2)
		assert.Equal(t, err, nil)
		assert.Equal(t, offset, uint32(2))
		assert.Equal(t, pos, uint64(400))
	})
}
