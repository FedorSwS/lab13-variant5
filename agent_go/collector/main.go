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
	"github.com/redis/go-redis/v9"
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

type CollectPayload struct {
	Services []string `json:"services"`
}

type Metric struct {
	ID         string    `json:"id"`
	Service    string    `json:"service"`
	MetricType string    `json:"metric_type"`
	Value      float64   `json:"value"`
	Timestamp  time.Time `json:"timestamp"`
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
	conn, err := grpc.DialContext(ctx, "localhost:4317",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatal("Failed to create gRPC connection:", err)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		log.Fatal("Failed to create exporter:", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("collector-agent"),
		),
	)
	if err != nil {
		log.Fatal("Failed to create resource:", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("collector-agent")

	return func() {
		tp.Shutdown(ctx)
	}
}

func main() {
	shutdown := initTracer()
	defer shutdown()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal("Failed to connect to NATS:", err)
	}
	defer nc.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Printf("Redis not available, continuing without state: %v", err)
	}

	counterKey := "collector:task_count"
	var taskCount int64
	if val, err := rdb.Get(ctx, counterKey).Int64(); err == nil {
		taskCount = val
		log.Printf("Restored state: processed %d tasks", taskCount)
	}

	_, err = nc.Subscribe("tasks.collect", func(msg *nats.Msg) {
		ctx, span := tracer.Start(context.Background(), "CollectTask")
		defer span.End()

		var task Task
		if err := json.Unmarshal(msg.Data, &task); err != nil {
			log.Printf("Failed to unmarshal task: %v", err)
			return
		}
		span.SetAttributes(attribute.String("task.id", task.ID))

		var payload CollectPayload
		if err := json.Unmarshal(task.Payload, &payload); err != nil {
			log.Printf("Failed to unmarshal payload: %v", err)
			return
		}

		log.Printf("Collector: collecting metrics for services: %v", payload.Services)

		metrics := []Metric{}
		for _, svc := range payload.Services {
			metric := Metric{
				ID:         task.ID + "_" + svc,
				Service:    svc,
				MetricType: "cpu_usage",
				Value:      float64(40 + time.Now().UnixNano()%50),
				Timestamp:  time.Now(),
			}
			metrics = append(metrics, metric)

			span.AddEvent("metric_collected", trace.WithAttributes(
				attribute.String("service", svc),
				attribute.Float64("value", metric.Value),
			))
		}

		taskCount++
		rdb.Set(ctx, counterKey, taskCount, 0)
		rdb.Set(ctx, "collector:last_"+task.ID, time.Now().Unix(), 24*time.Hour)

		result := Result{
			TaskID:    task.ID,
			Success:   true,
			Output:    metrics,
			TraceID:   task.TraceID,
			Timestamp: time.Now(),
		}
		respData, _ := json.Marshal(result)
		nc.Publish("tasks.collect.completed", respData)
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Collector agent started, waiting for tasks...")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("Shutting down collector agent")
}
