package main

import (
	"context"
	"fmt"
	"log"
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
	fmt.Println("START")
	conn, err := grpc.Dial(connectTo, grpc.WithBlock(), grpc.WithInsecure(), grpc.WithTimeout(time.Second*10))
	if err != nil {
		return fmt.Errorf("failed to connect to PetStoreService on %s: %w", connectTo, err)
	}
	fmt.Println("START")
	log.Println("Connected to", connectTo)
	fmt.Println("START")
	queue := queuev1.NewQueueServiceClient(conn)
	fmt.Println("START")
	v, err := queue.GetValues(context.TODO(), &queuev1.Key{Key: "hello"})
	if err != nil {
		return err
	}
	fmt.Println("START")
	fmt.Println("The value of v is: ", v.Key)
	return nil
}
