package tasks

import (
	"fmt"
	"github.com/hibiken/asynq"
)

const (
	// TypeMigratorPayload is a name of the task type
	// for sending a reminder email.
	TypeMigratorPayload = "migrate:tenant"
)

// NewMigratorTask task payload for migration.
func NewMigratorTask(id string) *asynq.Task {
	fmt.Println("NewMigratorTask")
	// Specify task payload.
	payload := map[string]interface{}{
		"tenant_id": id, // set tenant ID
	}

	// Return a new task with given type and payload.
	fmt.Println(payload)
	return asynq.NewTask(TypeMigratorPayload, payload)
}
