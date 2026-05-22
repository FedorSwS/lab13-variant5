package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Task struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
	TraceID   string          `json:"trace_id"`
}

type AnalyzePayload struct {
	MetricID   string  `json:"metric_id"`
	Service    string  `json:"service"`
	Value      float64 `json:"value"`
	Thresholds struct {
		Warning  float64 `json:"warning"`
		Critical float64 `json:"critical"`
	} `json:"thresholds"`
}

type AlertPayload struct {
	Service  string `json:"service"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
	MetricID string `json:"metric_id"`
}

type Result struct {
	TaskID    string      `json:"task_id"`
	Success   bool        `json:"success"`
	Output    interface{} `json:"output"`
	Error     string      `json:"error,omitempty"`
	TraceID   string      `json:"trace_id"`
	Timestamp time.Time   `json:"timestamp"`
}

var tracer trace.Tracer

func initTracer() func() {
	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "localhost:4317",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	exporter, _ := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	res, _ := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String("analyzer-agent")))
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("analyzer-agent")
	return func() { tp.Shutdown(ctx) }
}

func main() {
	shutdown := initTracer()
	defer shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	_, err := nc.Subscribe("tasks.analyze", func(msg *nats.Msg) {
		ctx, span := tracer.Start(context.Background(), "AnalyzeTask")
		defer span.End()

		var task Task
		json.Unmarshal(msg.Data, &task)
		span.SetAttributes(attribute.String("task.id", task.ID))

		var payload AnalyzePayload
		json.Unmarshal(task.Payload, &payload)

		severity := "normal"
		if payload.Value >= payload.Thresholds.Critical {
			severity = "critical"
		} else if payload.Value >= payload.Thresholds.Warning {
			severity = "warning"
		}

		span.SetAttributes(attribute.String("severity", severity))
		log.Printf("Analyzer: service=%s value=%.2f severity=%s", payload.Service, payload.Value, severity)

		alertNeeded := severity != "normal"

		result := Result{
			TaskID:  task.ID,
			Success: true,
			Output: map[string]interface{}{
				"severity":     severity,
				"alert_needed": alertNeeded,
				"metric_id":    payload.MetricID,
				"service":      payload.Service,
				"value":        payload.Value,
			},
			TraceID:   task.TraceID,
			Timestamp: time.Now(),
		}
		respData, _ := json.Marshal(result)
		nc.Publish("tasks.analyze.completed", respData)

		if alertNeeded && severity == "critical" {
			alertPayload := AlertPayload{
				Service:  payload.Service,
				Severity: severity,
				Message:  "Critical threshold exceeded: " + payload.Service,
				MetricID: payload.MetricID,
			}
			alertData, _ := json.Marshal(alertPayload)
			nc.Publish("tasks.alert", alertData)
		}
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Analyzer agent started")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
