package log

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Afrawles/Qute/internal/assert"
)

var (
	data = []byte("testing store")
	width = uint64(len(data)) + 8 
)

func setup(t testing.TB) (*store, string, func()) {
	t.Helper()

	dir := t.TempDir()
	fp := filepath.Join(dir, "store_test")
	f, err := os.OpenFile(fp, os.O_CREATE | os.O_RDWR, 0644)
	
	if err != nil {
		t.Fatal(err)
	}

	s, err := newStore(f)
	if err != nil {
		t.Fatal(err)
	}

	// t.Cleanup(func() {
	// 	if err := s.Close(); err != nil {
	// 		t.Fatal(err)
	// 	}
	// })
	
	cleanup := func ()  {
		s.Close()
	}

	return s, fp, cleanup

}


func testAppend(t testing.TB, s *store) {
	t.Helper()

	for i := uint64(1); i <= 5 ; i++ {
		n, pos, err := s.Append(data)
		assert.Equal(t, err, nil)
		assert.Equal(t, n, width)

		// width*i = expected total bytes after i records
		// pos + n = actual total bytes after appending this record
		assert.Equal(t, pos+n, width*i)
	}
}

func testRead(t testing.TB, s *store) {
	t.Helper()

	var pos uint64
	for i := uint64(1); i <= 5; i++ {
		p, err := s.Read(pos)
		assert.Equal(t, err, nil)
		assert.Equal(t, p, data)
		pos += width
	}
}

func testReadAt(t testing.TB, s *store) {
	t.Helper()
	for i, pos := uint64(1), uint64(0); i <= 5; i++ {
		prefixB := make([]byte, 8)
		n, err := s.ReadAt(prefixB, pos)
		assert.Equal(t, err, nil)
		assert.Equal(t, n, 8)

		pos += uint64(n)

		actualData := make([]byte, binary.BigEndian.Uint64(prefixB))
		n, err = s.ReadAt(actualData, pos)
		assert.Equal(t, err, nil)
		assert.Equal(t, binary.BigEndian.Uint64(prefixB), uint64(n))
		assert.Equal(t, actualData, data)

		pos += uint64(n)
	}
}


func TestAppendRead(t *testing.T) {
	s, _, cleanup := setup(t)
	defer cleanup()

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

}

func TestAppendCloseRead(t *testing.T) {
	s, fp, _ := setup(t)

	data1 := []byte("before close")

	_, pos1, err := s.Append(data1)
	assert.Equal(t, err, nil)
	assert.Equal(t, s.Close(), nil)

	f, err := os.OpenFile(fp, os.O_RDWR|os.O_APPEND, 0644)
	assert.Equal(t, err, nil)

	s2, err := newStore(f)
	assert.Equal(t, err, nil)
	
	defer s2.Close()

	got1, err := s2.Read(pos1)
	assert.Equal(t, err, nil)
	assert.Equal(t, got1, data1)

	data2 := []byte("after reopen")
	_, pos2, err := s2.Append(data2)
	assert.Equal(t, err, nil)

	got2, err := s2.Read(pos2)
	assert.Equal(t, err, nil)
	assert.Equal(t, got2, data2)

}
