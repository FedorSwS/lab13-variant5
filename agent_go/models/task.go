package models

import (
	"encoding/json"
	"time"
)

type Metric struct {
	ID         string    `json:"id"`
	Service    string    `json:"service"`
	MetricType string    `json:"metric_type"`
	Value      float64   `json:"value"`
	Timestamp  time.Time `json:"timestamp"`
}

type Task struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
	TraceID   string          `json:"trace_id"`
}

type CollectTaskPayload struct {
	Services []string `json:"services"`
}

type AnalyzeTaskPayload struct {
	MetricID   string  `json:"metric_id"`
	Service    string  `json:"service"`
	Value      float64 `json:"value"`
	Thresholds struct {
		Warning  float64 `json:"warning"`
		Critical float64 `json:"critical"`
	} `json:"thresholds"`
}

type AlertTaskPayload struct {
	Service  string `json:"service"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
	MetricID string `json:"metric_id"`
}

type RecoverTaskPayload struct {
	Service  string `json:"service"`
	Issue    string `json:"issue"`
	Attempts int    `json:"attempts"`
}

type Result struct {
	TaskID    string      `json:"task_id"`
	Success   bool        `json:"success"`
	Output    interface{} `json:"output"`
	Error     string      `json:"error,omitempty"`
	TraceID   string      `json:"trace_id"`
	Timestamp time.Time   `json:"timestamp"`
}
