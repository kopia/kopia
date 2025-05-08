package server

import (
	"context"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/uitask"
)

func handleTaskList(_ context.Context, rc requestContext) (interface{}, *apiError) {
	tasks := rc.srv.taskManager().ListTasks()
	if tasks == nil {
		tasks = []uitask.Info{}
	}

	return serverapi.TaskListResponse{
		Tasks: tasks,
	}, nil
}

func handleTaskInfo(_ context.Context, rc requestContext) (interface{}, *apiError) {
	taskID := rc.muxVar("taskID")

	t, ok := rc.srv.taskManager().GetTask(taskID)
	if !ok {
		return nil, notFoundError("task not found")
	}

	return t, nil
}

func handleTaskSummary(_ context.Context, rc requestContext) (interface{}, *apiError) {
	return rc.srv.taskManager().TaskSummary(), nil
}

func handleTaskLogs(_ context.Context, rc requestContext) (interface{}, *apiError) {
	taskID := rc.muxVar("taskID")

	return serverapi.TaskLogResponse{
		Logs: rc.srv.taskManager().TaskLog(taskID),
	}, nil
}

func handleTaskCancel(_ context.Context, rc requestContext) (interface{}, *apiError) {
	rc.srv.taskManager().CancelTask(rc.muxVar("taskID"))

	return &serverapi.Empty{}, nil
}
