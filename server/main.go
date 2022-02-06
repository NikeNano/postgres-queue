package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/NikeNano/postrgres-queue/lib/postgress"
	queuev1 "github.com/NikeNano/postrgres-queue/types/proto/go/types/v1"
	"google.golang.org/grpc"

	_ "github.com/lib/pq"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	listenOn := ":8080"
	listener, err := net.Listen("tcp", listenOn)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenOn, err)
	}

	db, err := postgress.Getdb()
	if err != nil {
		return err
	}
	server := grpc.NewServer()
	queuev1.RegisterQueueServiceServer(server, &queueServiceServer{dbService: postgress.NewService(db)})
	log.Println("Listening on", listenOn)
	if err := server.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve gRPC server: %w", err)
	}

	return nil
}

type queueServiceServer struct {
	queuev1.UnimplementedQueueServiceServer
	dbService postgress.Service
}

func (q *queueServiceServer) GetValues(ctx context.Context, req *queuev1.Key) (*queuev1.Value, error) {
	name := req.GetKey()
	fmt.Println("The key is: ", name)

	return &queuev1.Value{
		Key:   "Hello world",
		Value: "yeees",
	}, nil
}

func (q *queueServiceServer) GetValuesTx(srv queuev1.QueueService_GetValuesTxServer) error {
	fmt.Println("INSIDE THE TX")
	ctx := srv.Context()

	// We need to use a new context here if it should work....
	tx, err := q.dbService.GetTx(context.Background())
	if err != nil {
		return err
	}

	// Lock the rows in the database
	out, err := q.dbService.DeQueueLockTx(ctx, tx, 1)
	if err != nil {
		_ = q.dbService.RollBack(tx)
		return err
	}
	q.dbService.CommitTx(tx)
	// Send the rows to the client
	if err := srv.Send(&queuev1.Value{Key: out[0].Key, Value: out[0].Value}); err != nil {
		q.dbService.RollBack(tx)
		return err
	}

	// Listen for errors or done.
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		select {
		case <-ctx.Done(): // exit if context is done
			fmt.Println("Context done")
			return q.dbService.CommitTx(tx)
		default:
		}
	}
	if err := q.dbService.RollBack(tx); err != nil {
		return err
	}
	return fmt.Errorf("failed to get context done")
}
