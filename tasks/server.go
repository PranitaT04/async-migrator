package tasks

import (
	"log"

	"github.com/hibiken/asynq"
)

func StartServer() {
	// Create and configuring Redis connection.
	redisConnection := asynq.RedisClientOpt{
		Addr: "redis://redis.dev-actions:6379", // Redis server address
	}

	// Create and configuring Asynq worker server.
	worker := asynq.NewServer(redisConnection, asynq.Config{
		// Specify how many concurrent workers to use.
		Concurrency: 10,
		// Specify multiple queues with different priority.
		Queues: map[string]int{
			"critical": 6, // processed 60% of the time
			"default":  3, // processed 30% of the time
			"low":      1, // processed 10% of the time
		},
	})

	// Create a new task's mux instance.
	mux := asynq.NewServeMux()

	// Define a task handler for the migrator task.
	mux.HandleFunc(
		TypeMigratorPayload,       // task type
		HandleMigratorTask, // handler function
	)

	// Run worker server.
	if err := worker.Run(mux); err != nil {
		log.Fatal(err)
	}
}
