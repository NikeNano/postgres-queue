package main

import (
	"context"
	"fmt"
	"time"

	queuev1 "github.com/NikeNano/postrgres-queue/types/proto/go/types/v1"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	connectTo := "127.0.0.1:8080"
	conn, err := grpc.Dial(connectTo, grpc.WithBlock(), grpc.WithInsecure(), grpc.WithTimeout(time.Second*10))
	if err != nil {
		return fmt.Errorf("failed to connect to PetStoreService on %s: %w", connectTo, err)
	}

	queue := queuev1.NewQueueServiceClient(conn)
	v, err := queue.GetValues(context.TODO(), &queuev1.Key{Key: "hello"})
	if err != nil {
		return err
	}
	fmt.Println("The value of v is: ", v.Key)
	ctx := context.Background()
	stream, err := queue.GetValuesTx(ctx)
	// user ticker to avoid hard spin CPU
	if err != nil {
		return err
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		res, err := stream.Recv()
		if err != nil {
			_ = stream.CloseSend()
			break
		}
		if res == nil {
			continue
		}
		fmt.Println("The result is: ", res.Value)
		ctx.Done()
		return nil
	}

	return err
}
