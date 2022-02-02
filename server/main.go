package main

import (
	"context"
	"fmt"
	"log"
	"net"

	queuev1 "github.com/NikeNano/postrgres-queue/types/proto/go/types/v1"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	listenOn := "127.0.0.1:8080"
	listener, err := net.Listen("tcp", listenOn)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenOn, err)
	}

	server := grpc.NewServer()
	queuev1.RegisterQueueServiceServer(server, &queueServiceServer{})
	log.Println("Listening on", listenOn)
	if err := server.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve gRPC server: %w", err)
	}

	return nil
}

type queueServiceServer struct {
	queuev1.UnimplementedQueueServiceServer
}

func (q *queueServiceServer) GetValues(ctx context.Context, req *queuev1.Key) (*queuev1.Value, error) {
	name := req.GetKey()
	fmt.Println("The key is: ", name)

	return &queuev1.Value{
		Key:   "Hello world",
		Value: "yeees",
	}, nil
}
