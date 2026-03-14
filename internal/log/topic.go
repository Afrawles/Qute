package log

import (
	"fmt"
	"hash/fnv"
	"path/filepath"
	"sync"

	api "github.com/Afrawles/Qute/api/v1"
)

type partition struct {
	id uint32
	log *messageLog
}

type Topic struct {
	partitions []*partition
	name string
	counter uint32 

	mu sync.RWMutex

}

func NewTopic(dir, name string, numPartitions int, cfg Config) (*Topic, error) {
	tpc := &Topic{
		name: name,
	}

	for i := 0; i < numPartitions; i++ {
		tpcDir := filepath.Join(dir, fmt.Sprintf("%s-%d", name, i))
		lg, err := newMessageLog(tpcDir, cfg)
		if err != nil {
			return nil, err
		}

		tpc.partitions = append(tpc.partitions, &partition{id: uint32(i), log: lg})

	}

	return tpc, nil
}

func (t *Topic) partition(msg *api.Message) *partition {
    if msg.Key == nil {
        t.mu.Lock()
        idx := t.counter % uint32(len(t.partitions))
        t.counter++
        t.mu.Unlock()
        return t.partitions[idx]
    }
    h := fnv.New32a()
    h.Write(msg.Key)
    return t.partitions[h.Sum32()%uint32(len(t.partitions))]
}

func (t *Topic) Read(paritionID uint32, off uint64) (*api.Message, error) {

	if int(paritionID) >= len(t.partitions) {
		return nil, fmt.Errorf("partion : %d does not exsit", paritionID)
	}

	return t.partitions[paritionID].log.Read(off)
}

func (t *Topic) Append(msg *api.Message) (uint64, error) {
	p := t.partition(msg)
	return p.log.Append(msg)
}


func (t *Topic) Close() error {
    for _, p := range t.partitions {
        if err := p.log.Close(); err != nil {
            return err
        }
    }
    return nil
}
