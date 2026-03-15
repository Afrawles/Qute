package server

import (
	"context"
	"errors"
	"io"
	"time"

	api "github.com/Afrawles/Qute/api/v1"
	"google.golang.org/grpc"
)

type CommitLog interface {
	Append(*api.Message) (uint64, error)
	Read(off uint64) (*api.Message, error)
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	CommitLog CommitLog
	api.UnimplementedLogServer
}

func NewGRPCServer(log CommitLog) (*grpc.Server, error) {
	gsrv := grpc.NewServer()

	srv := &grpcServer{
		CommitLog: log, 
	}

	api.RegisterLogServer(gsrv, srv)

	return gsrv, nil
}

func (g *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	off, err := g.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: off}, err
}

func (g *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	msg, err := g.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Record: msg}, nil
}

func (g *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		resp, err := g.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err = stream.Send(resp); err != nil {
			return err
		}
	}
}

func (g *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <- stream.Context().Done():
		return nil
		case <- time.After(10 * time.Millisecond):
			resp, err := g.Consume(stream.Context(), req)

			var outOfRange api.ErrOffsetOutOfRange

			if errors.As(err, &outOfRange) {
				continue
			}
			if err != nil {
				return err
			}
			
			if err := stream.Send(resp); err != nil {
				return err
			}

			req.Offset++

		}
	}
}
