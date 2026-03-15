package server

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	api "github.com/Afrawles/Qute/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Afrawles/Qute/internal/assert"
	"github.com/Afrawles/Qute/internal/log"
)

// TestServer runs all gRPC integration tests in a table-driven manner.
func TestServer(t *testing.T) {
	tests := []struct {
		name string
		test func(t testing.TB, client api.LogClient, lg CommitLog)
	}{
		{"produce/consume works", testProduceConsume},
		{"produce/consume stream works", testProduceConsumeStream},
		{"consume past boundary fails", testConsumePastBoundary},
		{"consume stream stops on cancel", testConsumeStreamCancel},
		{"multiple clients concurrently", testMultipleClients},
		{"multiple clients concurrently (streamed)", testMultipleClientsStream},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, lg, teardown := setupTest(t)
			defer teardown()
			tt.test(t, client, lg)
		})
	}
}

// setupTest creates a listener, server, client, and teardown function
func setupTest(t testing.TB) (client api.LogClient, lg CommitLog, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	assert.Equal(t, nil, err)

	dir := t.TempDir()

	mlog, err := log.NewMessageLog(dir, log.Config{})
	assert.Equal(t, nil, err)

	grpcServer, err := NewGRPCServer(mlog)
	assert.NoError(t, err)

	go func() {
		if err := grpcServer.Serve(l); err != nil && !strings.Contains(err.Error(), "closed") {
			t.Error(err)
		}
	}()

	cc, err := grpc.NewClient(
		l.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	assert.Equal(t, nil, err)
	client = api.NewLogClient(cc)

	teardown = func() {
		grpcServer.Stop()
		cc.Close()
		l.Close()
		mlog.Remove()
	}

	return client, mlog, teardown
}

func testProduceConsume(t testing.TB, client api.LogClient, lg CommitLog) {
	t.Helper()
	ctx := context.Background()
	want := &api.Message{Value: []byte("hello world")}

	produceResp, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	assert.Equal(t, err, nil)

	consumeResp, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produceResp.Offset})
	assert.Equal(t, err, nil)

	assert.Equal(t, consumeResp.Record.Value, want.Value)
	assert.Equal(t, consumeResp.Record.Offset, produceResp.Offset)
}

func testConsumePastBoundary(t testing.TB, client api.LogClient, lg CommitLog) {
	t.Helper()
	ctx := context.Background()

	produceResp, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Message{Value: []byte("hello")},
	})
	assert.Equal(t, err, nil)

	consumeResp, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produceResp.Offset + 1})
	assert.Equal(t, consumeResp, (*api.ConsumeResponse)(nil))

	st, ok := status.FromError(err)
	assert.Equal(t, ok, true)
	assert.Equal(t, st.Code(), codes.NotFound)
}

func testProduceConsumeStream(t testing.TB, client api.LogClient, lg CommitLog) {
	t.Helper()
	ctx := context.Background()

	records := []*api.Message{
		{Value: []byte("first message")},
		{Value: []byte("second message")},
	}

	ps, err := client.ProduceStream(ctx)
	assert.Equal(t, err, nil)
	for i, record := range records {
		err = ps.Send(&api.ProduceRequest{Record: record})
		assert.Equal(t, err, nil)

		res, err := ps.Recv()
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Offset, uint64(i))
	}

	cs, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	assert.Equal(t, err, nil)
	for i, record := range records {
		res, err := cs.Recv()
		assert.Equal(t, err, nil)
		assert.Equal(t, res.Record.Value, record.Value)
		assert.Equal(t, res.Record.Offset, uint64(i))
	}
}

func testConsumeStreamCancel(t testing.TB, client api.LogClient, lg CommitLog) {
	t.Helper()
	ctx := context.Background()

	records := []*api.Message{
		{Value: []byte("msg1")},
		{Value: []byte("msg2")},
	}

	ps, err := client.ProduceStream(ctx)
	assert.Equal(t, err, nil)
	for _, r := range records {
		err = ps.Send(&api.ProduceRequest{Record: r})
		assert.Equal(t, err, nil)
		_, err := ps.Recv()
		assert.Equal(t, err, nil)
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cs, err := client.ConsumeStream(cancelCtx, &api.ConsumeRequest{Offset: 0})
	assert.Equal(t, err, nil)

	res, err := cs.Recv()
	assert.Equal(t, err, nil)
	assert.Equal(t, res.Record.Value, records[0].Value)

	cancel()
	_, err = cs.Recv()
	if err == nil {
		t.Fatal("expected error after context cancel, got nil")
	}
	st, ok := status.FromError(err)
	assert.Equal(t, ok, true)
	assert.Equal(t, st.Code(), codes.Canceled)
}

func testMultipleClients(t testing.TB, client api.LogClient, lg CommitLog) {
	t.Helper()
	ctx := context.Background()

	const (
		numProducers = 3
		numConsumers = 3
		numMessages  = 5
	)

	type produced struct {
		value  string
		offset uint64
	}
	producedCh := make(chan produced, numProducers*numMessages)

	for p := 0; p < numProducers; p++ {
		go func(pid int) {
			for m := 0; m < numMessages; m++ {
				msg := &api.Message{Value: []byte(fmt.Sprintf("producer%d-msg%d", pid, m))}
				resp, err := client.Produce(ctx, &api.ProduceRequest{Record: msg})
				assert.Equal(t, err, nil)
				producedCh <- produced{value: string(msg.Value), offset: resp.Offset}
			}
		}(p)
	}

	producedMessages := make(map[uint64]string)
	for i := 0; i < numProducers*numMessages; i++ {
		msg := <-producedCh
		producedMessages[msg.offset] = msg.value
	}

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func() {
			defer wg.Done()
			for offset := uint64(0); offset < uint64(numProducers*numMessages); offset++ {
				resp, err := client.Consume(ctx, &api.ConsumeRequest{Offset: offset})
				assert.Equal(t, err, nil)
				want := producedMessages[offset]
				assert.Equal(t, string(resp.Record.Value), want)
				assert.Equal(t, resp.Record.Offset, offset)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consumers did not finish in time")
	}
}

func testMultipleClientsStream(t testing.TB, client api.LogClient, lg CommitLog) {
	t.Helper()
	ctx := context.Background()

	const (
		numProducers = 3
		numConsumers = 3
		numMessages  = 5
	)

	type produced struct {
		offset uint64
		value  string
	}
	producedCh := make(chan produced, numProducers*numMessages)

	for p := 0; p < numProducers; p++ {
		go func(pid int) {
			ps, err := client.ProduceStream(ctx)
			assert.Equal(t, err, nil)

			for m := 0; m < numMessages; m++ {
				msg := &api.Message{Value: []byte(fmt.Sprintf("producer%d-msg%d", pid, m))}
				err := ps.Send(&api.ProduceRequest{Record: msg})
				assert.Equal(t, err, nil)

				res, err := ps.Recv()
				assert.Equal(t, err, nil)
				producedCh <- produced{offset: res.Offset, value: string(msg.Value)}
			}
		}(p)
	}

	producedMessages := make(map[uint64]string)
	for i := 0; i < numProducers*numMessages; i++ {
		msg := <-producedCh
		producedMessages[msg.offset] = msg.value
	}

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	for c := 0; c < numConsumers; c++ {
		go func() {
			defer wg.Done()
			cs, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
			assert.Equal(t, err, nil)

			for offset := uint64(0); offset < uint64(numProducers*numMessages); offset++ {
				res, err := cs.Recv()
				assert.Equal(t, err, nil)
				want := producedMessages[res.Record.Offset]
				assert.Equal(t, string(res.Record.Value), want)
				assert.Equal(t, res.Record.Offset, res.Record.Offset)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("consumers did not finish in time")
	}
}
