package main

import (
	"context"
	"fmt"
	"log"
	"time"

	disheapv1 "github.com/disheap/disheap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to the gRPC server
	conn, err := grpc.Dial("localhost:9999", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create client
	client := disheapv1.NewDisheapClient(conn)

	// Test Stats method
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("Testing Stats method...")
	statsReq := &disheapv1.StatsReq{}
	statsResp, err := client.Stats(ctx, statsReq)
	if err != nil {
		log.Fatalf("Stats failed: %v", err)
	}

	fmt.Printf("Stats successful: %+v\n", statsResp)
}
