package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func getFileFromArguments() string {
	if len(os.Args) > 1 {
		return os.Args[1]
	}
	return "test201712"
}

func splitFilePath(path string) string {
	return path[len(path)-6:]
}

//daily
//hourly
//precip
//station

func main() {
	// OPEN CONNECTION
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		fmt.Printf("PROBLEM : %v\n", fmt.Sprintf("connect err = %v", err))
		os.Exit(1)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		fmt.Printf("PROBLEM : %v\n", fmt.Sprintf("ping err = %v", err))
		os.Exit(1)
	}
	collection := client.Database("climate").Collection("stations")

	pathToFile := getFileFromArguments()
	d := splitFilePath(pathToFile)

	// STATIONS CSV
	stns := processStationsCSV(pathToFile, d)
	insertManyResult, err := collection.InsertMany(context.TODO(), stns)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("docs inserted: ", insertManyResult.InsertedIDs)

	// CLOSE CONNECTION
	err = client.Disconnect(context.TODO())

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection to MongoDB closed.")
}
