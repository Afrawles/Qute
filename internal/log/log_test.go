package log

import (
	"errors"
	"io"
	"os"
	"testing"

	api "github.com/Afrawles/Qute/api/v1"
	"github.com/Afrawles/Qute/internal/assert"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	msg := &api.Message{Value: []byte("hello world")}

	for scenario, fn := range map[string]func(t *testing.T, log *messageLog){
		"append and read succeeds":      testAppendRead,
		"offset out of range error":     testOutOfRangeErr,
		"init with existing segments":   testInitExisting,
		"reader":                        testReader,
		"truncate":                      testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			var cfg Config
			cfg.Segment.MaxStoreBytes = uint64(len(msg.Value)+lenWidth + crcWidth) * 3
			cfg.Segment.MaxIndexBytes = entryWidth * 10

			log, err := NewMessageLog(dir, cfg)
			assert.Equal(t, err, nil)
			defer os.RemoveAll(dir)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *messageLog) {
	msg := &api.Message{Value: []byte("hello world")}

	off, err := log.Append(msg)
	assert.NoError(t, err)
	assert.Equal(t, off, uint64(0))

	got, err := log.Read(off)
	assert.NoError(t, err)
	assert.Equal(t, got.Value, msg.Value)
}

func testOutOfRangeErr(t *testing.T, log *messageLog) {
	read, err := log.Read(1)

	assert.Nil(t, read)
	assert.Error(t, err)

	var apiErr api.ErrOffsetOutOfRange
	assert.True(t, errors.As(err, &apiErr))

	assert.Equal(t, apiErr.Offset, uint64(1))
}

func testInitExisting(t *testing.T, log *messageLog) {
	msg := &api.Message{Value: []byte("hello world")}

	for i := 0; i < 3; i++ {
		_, err := log.Append(msg)
		assert.NoError(t, err)
	}

	err := log.Close()
	assert.NoError(t, err)

	low, err := log.LowestOffset()
	assert.NoError(t, err)
	assert.Equal(t, low, uint64(0))

	high, err := log.HighestOffset()
	assert.NoError(t, err)
	assert.Equal(t, high, uint64(2))

	n, err := NewMessageLog(log.dir, log.config)
	assert.NoError(t, err)

	low, err = n.LowestOffset()
	assert.NoError(t, err)
	assert.Equal(t, low, uint64(0))

	high, err = n.HighestOffset()
	assert.NoError(t, err)
	assert.Equal(t, high, uint64(2))
}

func testReader(t *testing.T, log *messageLog) {
	msg := &api.Message{Value: []byte("hello world")}

	off, err := log.Append(msg)
	assert.NoError(t, err)
	assert.Equal(t, off, uint64(0))

	reader := log.NewReader()
	b, err := io.ReadAll(reader)
	assert.NoError(t, err)

	got := &api.Message{}
	err = proto.Unmarshal(b[lenWidth+crcWidth:], got)
	assert.NoError(t, err)

	assert.Equal(t, got.Value, msg.Value)
}

func testTruncate(t *testing.T, log *messageLog) {
	msg := &api.Message{Value: []byte("hello world")}

	dir := t.TempDir()
	var cfg Config
	cfg.Segment.MaxStoreBytes = uint64(len(msg.Value) + lenWidth + crcWidth)
	cfg.Segment.MaxIndexBytes = entryWidth * 3

	l, err := NewMessageLog(dir, cfg)
	assert.NoError(t, err)

	for i := 0; i < 3; i++ {
		_, err := l.Append(msg)
		assert.NoError(t, err)
	}

	err = l.Truncate(1)
	assert.NoError(t, err)

	_, err = l.Read(0)
	assert.Error(t, err)
}
