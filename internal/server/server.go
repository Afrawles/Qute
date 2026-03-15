package server

import (
	"context"
	"errors"
	"io"
	"time"

	api "github.com/Afrawles/Qute/api/v1"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type CommitLog interface {
	Append(*api.Message) (uint64, error)
	Read(off uint64) (*api.Message, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	*Config
	api.UnimplementedLogServer
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
    // Prepend auth interceptors so they always run first, before any
    // caller-supplied opts. gRPC only allows one Unary and one Stream
    // interceptor, so prepending ensures auth can't be silently overwritten.
    opts = append([]grpc.ServerOption{
        grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
            grpc_auth.StreamServerInterceptor(authenticate),
        )),
        grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
            grpc_auth.UnaryServerInterceptor(authenticate),
        )),
    }, opts...)

    gsrv := grpc.NewServer(opts...)
    srv := &grpcServer{Config: config}
    api.RegisterLogServer(gsrv, srv)
    return gsrv, nil
}

func (g *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := g.Authorizer.Authorize(subject(ctx), objectWildcard, produceAction); err != nil {
		return nil, err
	}
	off, err := g.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: off}, err
}

func (g *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := g.Authorizer.Authorize(subject(ctx), objectWildcard, consumeAction); err != nil {
		return nil, err
	}
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

// authenticate is a gRPC interceptor that extracts the client's identity
// from the TLS certificate and stores it in the context for later use
// by the authorizer.
func authenticate(ctx context.Context) (context.Context, error) {
    // Extract peer info from the incoming request context
    p, ok := peer.FromContext(ctx)
    if !ok {
        return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
    }

    // If no auth info, store an empty subject (unauthenticated peer)
    if p.AuthInfo == nil {
        return context.WithValue(ctx, subjectContextKey{}, ""), nil
    }

    // Extract the Common Name from the first verified certificate chain,
    // which identifies the client (e.g. "root", "nobody")
    tlsInfo := p.AuthInfo.(credentials.TLSInfo)
    subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
    ctx = context.WithValue(ctx, subjectContextKey{}, subject)
    return ctx, nil
}

// subject extracts the client's certificate CN from the context,
// which is set by the gRPC TLS interceptor.
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
