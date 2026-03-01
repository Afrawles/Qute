package log

import (
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
			cfg.Segment.MaxStoreBytes = uint64(len(msg.Value)+8) * 3
			cfg.Segment.MaxIndexBytes = entryWidth * 10

			log, err := newMessageLog(dir, cfg)
			assert.Equal(t, err, nil)
			defer os.RemoveAll(dir)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *messageLog) {
	msg := &api.Message{Value: []byte("hello world")}

	off, err := log.Append(msg)
	assert.Equal(t, err, nil)
	assert.Equal(t, off, uint64(0))

	got, err := log.Read(off)
	assert.Equal(t, err, nil)
	assert.Equal(t, got.Value, msg.Value)
}

func testOutOfRangeErr(t *testing.T, log *messageLog) {
	_, err := log.Read(999)
	assert.Equal(t, err != nil, true)
}

func testInitExisting(t *testing.T, log *messageLog) {
	msg := &api.Message{Value: []byte("hello world")}

	for i := 0; i < 3; i++ {
		_, err := log.Append(msg)
		assert.Equal(t, err, nil)
	}

	err := log.Close()
	assert.Equal(t, err, nil)

	low, err := log.LowestOffset()
	assert.Equal(t, err, nil)
	assert.Equal(t, low, uint64(0))

	high, err := log.HighestOffset()
	assert.Equal(t, err, nil)
	assert.Equal(t, high, uint64(2))

	n, err := newMessageLog(log.dir, log.config)
	assert.Equal(t, err, nil)

	low, err = n.LowestOffset()
	assert.Equal(t, err, nil)
	assert.Equal(t, low, uint64(0))

	high, err = n.HighestOffset()
	assert.Equal(t, err, nil)
	assert.Equal(t, high, uint64(2))
}

func testReader(t *testing.T, log *messageLog) {
	msg := &api.Message{Value: []byte("hello world")}

	off, err := log.Append(msg)
	assert.Equal(t, err, nil)
	assert.Equal(t, off, uint64(0))

	reader := log.NewReader()
	b, err := io.ReadAll(reader)
	assert.Equal(t, err, nil)

	got := &api.Message{}
	err = proto.Unmarshal(b[8:], got)
	assert.Equal(t, err, nil)
	assert.Equal(t, got.Value, msg.Value)
}

func testTruncate(t *testing.T, log *messageLog) {
    msg := &api.Message{Value: []byte("hello world")}

    dir := t.TempDir()
    var cfg Config
    cfg.Segment.MaxStoreBytes = uint64(len(msg.Value) + 8)
    cfg.Segment.MaxIndexBytes = entryWidth * 3
    l, err := newMessageLog(dir, cfg)
    assert.Equal(t, err, nil)

    for i := 0; i < 3; i++ {
        _, err := l.Append(msg)
        assert.Equal(t, err, nil)
    }

    err = l.Truncate(1)
    assert.Equal(t, err, nil)

    _, err = l.Read(0)
    assert.Equal(t, err != nil, true)
}
