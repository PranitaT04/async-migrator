package tasks

import (
// 	"C"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
// 	"ibm.com/cloudbroker/cb-go-common/log/logger"
)

var defaultClient = NewHTTPClient(time.Second * 60)

//HTTPClient ...
type HTTPClient struct {
	client *http.Client
}

//export DbMigrator
func DbMigrator(requestData []Feature) {
// 	fmt.Println("hello called")
// 	request_string := C.GoString(request)
// 	var requestData []interface{}
// 	if err := json.Unmarshal([]byte(request_string), &requestData); err != nil {
// 		panic(err)
// 	}
	fmt.Println(requestData)
	for _, service := range requestData {
		c := make(chan int)
		ctx := context.Background()
		go migrateService(ctx, c, service)
		<-c
	}
}

// func get(ctx context.Context, url string) (map[string]interface{}, error) {
//  var result map[string]interface{}
//  logger.Infof("Url for configuration API call [%s]", url)
//  resp, err := http.Get(url)
//  if err != nil {
//      logger.Errorf("Error while fetching configuration from url: [%s], Exc: [%s]", url, err)
//      return result, err
//  }
//  if resp.StatusCode == 200 {
//      json.NewDecoder(resp.Body).Decode(&result)
//  }
//  return result, nil
// }

// HTTPGetWithHeaders send URL params along with the base URL
func (hc *HTTPClient) HTTPGetWithHeaders(url string, headers map[string]string) ([]interface{}, error) {
	var result []interface{}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
// 		logger.Error("Cannot construct the url for get: ", url, err)
		return nil, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
// 	logger.Info("Calling http get URL (with headers): ", req.URL.String())
	resp, err := hc.client.Do(req)
	if err != nil {
		print(err)
		return nil, err
	}
	if resp.StatusCode == 200 {
		json.NewDecoder(resp.Body).Decode(&result)
	}
	return result, err
}
func NewHTTPClient(timeout time.Duration) *HTTPClient {
	client := &http.Client{
		Timeout: timeout}
	return &HTTPClient{
		client: client,
	}
}
func MongoConnect(host, user, password, database string) (*mongo.Database, error) {
	uri := fmt.Sprintf("mongodb://%s:27017", host)
	opt := options.Client().ApplyURI(uri)
	client, err := mongo.NewClient(opt)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	var db = client.Database(database)
	return db, nil
}
func migrateService(ctx context.Context, c chan int, service Feature) {
	var serviceName = service.ServiceName
	var tenantId = service.TenantId
	var targetDb = service.TargetDb
	var sourceDb = service.SourceDb
	var collections = service.CollectionMap
// 	var serviceName = service.(map[string]interface{})["service_name"].(string)
// 	var tenantId = service.(map[string]interface{})["tenant_id"].(string)
// 	var targetDb = service.(map[string]interface{})["targetdb"].(string)
// 	var sourceDb = service.(map[string]interface{})["sourcedb"].(string)
// 	var collections = service.(map[string]interface{})["collection_map"]
	//var lock_url = service["lock_url"].(string)
	//var release_url = requestData["release_url"].(string)
	println(serviceName, tenantId, targetDb)
	println(collections)
	// resp, err := get(ctx, lock_url)
	// if err != nil {
	//  logger.Errorf("Error while locking the service [%s]", lock_url)
	// }
	//if resp != nil {
	//db := mt.GetTenantDB(ctx, tenantId, targetDb)
	for sourceCollection, targetCollection := range collections {
		fmt.Println(sourceCollection)
		fmt.Println(targetCollection)
		//time.Sleep(20 * time.Second)
		uri := fmt.Sprintf("http://cb-consume-common-api.dev-consume:7080/common/v1/export/%s/%s", sourceDb, sourceCollection)
		flag := true
		counter := int64(1)
		limit := int64(100)
		for flag {
			skip := (counter - 1) * limit
			base, err := url.Parse(uri)
			if err != nil {
				c <- 1
				updateMigrationDb(ctx, serviceName, "failed", tenantId)
				//return
			}
			// Query params
			params := url.Values{}
			params.Add("limit", strconv.FormatInt(limit, 10))
			params.Add("skip", strconv.FormatInt(skip, 10))
			base.RawQuery = params.Encode()
			headers := map[string]string{
				"tenant-uuid": tenantId,
			}
			resp, err := defaultClient.HTTPGetWithHeaders(base.String(), headers)
			if err != nil {
// 				logger.Error(ctx, "Error creating http GET request with headers: ", base.String(), err)
				c <- 1
				updateMigrationDb(ctx, serviceName, "failed", tenantId)
				//return

			}
			//print(resp)
			if len(resp) > 0 {
				var bulkDocs []mongo.WriteModel
				for _, value := range resp {
					insertModel := mongo.NewInsertOneModel()
					insertModel.SetDocument(value)
					bulkDocs = append(bulkDocs, insertModel)
				}
				db, _ := MongoConnect("mongodb.dev-actions", "", "", dbWithTennatId(tenantId, targetDb))
				result, err := db.Collection(targetCollection.(string)).BulkWrite(ctx, bulkDocs)
				fmt.Println(result)
				if err != nil {
					c <- 1
					updateMigrationDb(ctx, serviceName, "failed", tenantId)
					//return
				}
			} else {
				flag = false
			}
			counter++
		}
	}
	updateMigrationDb(ctx, serviceName, "success", tenantId)
	c <- 0
}

func updateMigrationDb(ctx context.Context, serviceName, status, tenantId string) {
	dbName := "migration"
	db, _ := MongoConnect("mongodb.dev-actions", "", "", dbName)
	update := bson.M{
		"$addToSet": bson.M{"migrated": serviceName},
		"$set":      bson.M{"status": status},
	}
	result := db.Collection("info").FindOneAndUpdate(ctx, bson.M{"tenant_id": tenantId}, update)
	fmt.Println(result)

}

func dbWithTennatId(tenantId, dbName string) string {
	if tenantId == "" {
		return dbName
	}
	return tenantId + "_" + dbName
}

func main() {
}
