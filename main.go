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

func idWBANtoMap(wbans []string, ids []interface{}) map[string]string {
	if len(wbans) != len(ids) {
		fmt.Printf("strings: %v | ids: %v", len(wbans), len(ids))
		log.Fatal("arrays for object ids did not match")
	}

	m := make(map[string]string)
	for i := 0; i < len(wbans); i++ {
		str := fmt.Sprint(ids[i])
		m[wbans[i]] = str[10 : len(str)-2]
	}
	return m
}

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
	db := client.Database("climate")

	pathToFile := getFileFromArguments()
	d := splitFilePath(pathToFile)

	// STATIONS CSV
	stns, stnWBANs := processStationsCSV(pathToFile, d)
	insertManyResult, err := db.Collection("stations").InsertMany(context.TODO(), stns)
	if err != nil {
		log.Fatal(err)
	}

	// create wban / objectid map
	stationIDMap := idWBANtoMap(stnWBANs, insertManyResult.InsertedIDs)

	// unset slices
	stns = []interface{}{}
	stnWBANs = []string{}

	// PRECIP CSV
	prcps := processPrecipCSV(pathToFile, d, stationIDMap)
	_, err = db.Collection("precips").InsertMany(context.TODO(), prcps)
	if err != nil {
		log.Fatal(err)
	}

	// CLOSE CONNECTION
	err = client.Disconnect(context.TODO())

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection to MongoDB closed.")
}
