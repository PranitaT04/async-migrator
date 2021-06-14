package tasks

import (
	"context"
	"fmt"
	"github.com/hibiken/asynq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
)

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

func update(tenantId string) {

	filter := bson.D{{"tenantid", tenantId}}
	update := bson.D{
		{"$set", bson.D{
			{"status", "INPROGRESS"},
			{"migrated", "[FULFILMENT, ORDERS, WORKFLOW]"},
		}},
	}

	updateResult, err := collection.UpdateOne(context.TODO(), filter, update)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Updated documents: %+v\n", updateResult)
}

type Collection map[string]interface{}

type Feature struct {
	TenantId string `json:"tenant_id"`
	ServiceName    string `json:"service_name"`
	SourceDb string `json:"sourcedb"`
	TargetDb string `json:"targetdb"`
	CollectionMap Collection `json:"collection_map"`
}

func UpdateFeature(tenant string) []Feature{
	dataReceived := []Feature{
		Feature{TenantId: tenant, ServiceName: "fulfilment", SourceDb: "fulfilment", TargetDb: "action_fulfilment", CollectionMap: Collection{"order": "action_request"} },
		Feature{TenantId: tenant, ServiceName: "order", SourceDb: "orderds", TargetDb: "action_orderds", CollectionMap: Collection{"order": "action_request"} },
		Feature{TenantId: tenant, ServiceName: "workflow", SourceDb: "workflow", TargetDb: "action_workflow", CollectionMap: Collection{"order": "action_request"} },
	}
	fmt.Print(dataReceived)
	return dataReceived
}

// HandleMigratorTask handler for migration task.
func HandleMigratorTask(c context.Context, t *asynq.Task) error {
	fmt.Println("HandleMigratorTask")
	// Get tenant ID from given task.
	tenant, err := t.Payload.GetString("tenant_id")
	if err != nil {
		return err
	}

	// Message to the worker's output.
	fmt.Printf("Proceed with migration process for Tenant ID %d\n", tenant)
	if tenant != "" {
		fmt.Println(tenant)
		if isMcasEnabled(tenant) {
			serviceBlocked(tenant)
			DbMigrator(UpdateFeature(tenant))
			connect()
			update(tenant)
		}
	}
	return nil
}

