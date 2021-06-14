package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strings"

	"go-async-migrator/tasks"

	"github.com/hibiken/asynq"
)

func getTenants() []string{
	fmt.Println("Calling API...")
	client := &http.Client{}
	req, err := http.NewRequest("GET", "http://cb-consume-common-api.dev-consume:7080/common/v1/tenants", nil)
	if err != nil {
		fmt.Print(err.Error())
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Print(err.Error())
	}
	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Print(err.Error())
	}
	var responseObject []string
	json.Unmarshal(bodyBytes, &responseObject)
	fmt.Printf("API Response as struct %+v\n", responseObject)
	return responseObject
}

var client *mongo.Client
var collection *mongo.Collection
var ctx = context.TODO()

type Info struct {
	TenantId string
	Status string
	Migrated  string
}

func connect() *mongo.Client {
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB!")
	collection = client.Database("migration").Collection("info")
	return client
}

func create(tenantId string) {
	tenant := Info{tenantId, "PENDING", ""}
	res, err := collection.InsertOne(ctx, tenant)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Inserted document: ", res.InsertedID)
}


func checkMigrationStatus(tenantId string) string{
	var result Info
	filter := bson.D{{"tenantid", tenantId}}
	err := collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found a single document: %+v\n", result)
	return reflect.Indirect(reflect.ValueOf(result)).FieldByName("status").String()
}

func main() {
	// Create a new Redis connection for the client.
	redisConnection := asynq.RedisClientOpt{
		Addr: "localhost:6379", // Redis server address
	}

	// Create a new Asynq client.
	client := asynq.NewClient(redisConnection)
	defer client.Close()


	// Loop to create tasks as Asynq client.
	for i, s := range getTenants() {
		// Generate a tenant ID.
		fmt.Println(i, s)

		connect()
		create(s)
		status := checkMigrationStatus(s)
		fmt.Println(status)

		// Define tasks.
		task := tasks.NewMigratorTask(s)

		// Process the task immediately in critical queue.
		if _, err := client.Enqueue(
			task,                   // task payload
			asynq.Queue("critical"), // set queue for task
		); err != nil {
			log.Fatal(err)
		}

		if strings.Compare(status, "PENDING") == 0 || strings.Compare(status, "FAILED") == 0{
			// Define tasks.
			task := tasks.NewMigratorTask(s)

			// Process the task immediately in critical queue.
			if _, err := client.Enqueue(
				task,                   // task payload
				asynq.Queue("critical"), // set queue for task
			); err != nil {
				log.Fatal(err)
			}

		}
// 		task1 := tasks.NewWelcomeEmailTask(i)
//
// 		// Process the task immediately in critical queue.
// 		if _, err := client.Enqueue(
// 			task1,                   // task payload
// 			asynq.Queue("critical"), // set queue for task
// 		); err != nil {
// 			log.Fatal(err)
// 		}
	}
    tasks.StartServer()
}
