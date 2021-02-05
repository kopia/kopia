package server

import (
	"context"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/kopia/kopia/internal/serverapi"
	"github.com/kopia/kopia/internal/uitask"
)

func (s *Server) handleTaskList(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	tasks := s.taskmgr.ListTasks()
	if tasks == nil {
		tasks = []uitask.Info{}
	}

	return serverapi.TaskListResponse{
		Tasks: tasks,
	}, nil
}

func (s *Server) handleTaskInfo(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	taskID := mux.Vars(r)["taskID"]

	t, ok := s.taskmgr.GetTask(taskID)
	if !ok {
		return nil, notFoundError("task not found")
	}

	return t, nil
}

func (s *Server) handleTaskSummary(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	return s.taskmgr.TaskSummary(), nil
}

func (s *Server) handleTaskLogs(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	taskID := mux.Vars(r)["taskID"]

	return serverapi.TaskLogResponse{
		Logs: s.taskmgr.TaskLog(taskID),
	}, nil
}

func (s *Server) handleTaskCancel(ctx context.Context, r *http.Request, body []byte) (interface{}, *apiError) {
	s.taskmgr.CancelTask(mux.Vars(r)["taskID"])

	return &serverapi.Empty{}, nil
}
