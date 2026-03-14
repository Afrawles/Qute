package log

import (
	"os"
	"testing"

	api "github.com/Afrawles/Qute/api/v1"
	"github.com/Afrawles/Qute/internal/assert"
)

func TestTopic(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, topic *Topic){
		"append and read succeeds":        testTopicAppendRead,
		"key based partition routing":     testTopicKeyRouting,
		"invalid partition error":         testTopicInvalidPartition,
		"round robin keyless messages":    testTopicRoundRobin,
		"multiple partitions independent": testTopicPartitionsIndependent,
		"close succeeds":                  testTopicClose,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			var cfg Config
			cfg.Segment.MaxStoreBytes = 100 * 1024 * 1024
			cfg.Segment.MaxIndexBytes = entryWidth * 10
			topic, err := NewTopic(dir, "test-topic", 3, cfg)
			assert.Equal(t, err, nil)
			defer os.RemoveAll(dir)
			fn(t, topic)
		})
	}
}

func testTopicAppendRead(t *testing.T, topic *Topic) {
	msg := &api.Message{Value: []byte("hello world"), Key: []byte("my-key")}
	off, err := topic.Append(msg)
	assert.Equal(t, err, nil)

	p := topic.partition(msg)
	got, err := topic.Read(p.id, off)
	assert.Equal(t, err, nil)
	assert.Equal(t, got.Value, msg.Value)
}

func testTopicKeyRouting(t *testing.T, topic *Topic) {
	msg := &api.Message{Value: []byte("hello"), Key: []byte("stable-key")}

	p1 := topic.partition(msg)
	p2 := topic.partition(msg)
	assert.Equal(t, p1.id, p2.id)
}

func testTopicInvalidPartition(t *testing.T, topic *Topic) {
	_, err := topic.Read(999, 0)
	assert.Equal(t, err != nil, true)
}

func testTopicRoundRobin(t *testing.T, topic *Topic) {
	numPartitions := uint32(len(topic.partitions))

	for i := uint32(0); i < numPartitions; i++ {
		msg := &api.Message{Value: []byte("no key")}
		p := topic.partition(msg)
		assert.Equal(t, p.id, i%numPartitions)
	}
}

func testTopicPartitionsIndependent(t *testing.T, topic *Topic) {
	msgA := &api.Message{Value: []byte("partition A"), Key: []byte("key-a")}
	msgB := &api.Message{Value: []byte("partition B"), Key: []byte("key-b")}

	pA := topic.partition(msgA)
	pB := topic.partition(msgB)

	offA, err := topic.Append(msgA)
	assert.Equal(t, err, nil)

	offB, err := topic.Append(msgB)
	assert.Equal(t, err, nil)

	gotA, err := topic.Read(pA.id, offA)
	assert.Equal(t, err, nil)
	assert.Equal(t, gotA.Value, msgA.Value)

	gotB, err := topic.Read(pB.id, offB)
	assert.Equal(t, err, nil)
	assert.Equal(t, gotB.Value, msgB.Value)
}

func testTopicClose(t *testing.T, topic *Topic) {
	msg := &api.Message{Value: []byte("hello"), Key: []byte("k")}
	_, err := topic.Append(msg)
	assert.Equal(t, err, nil)

	err = topic.Close()
	assert.Equal(t, err, nil)
}
