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

type RecoverPayload struct {
	Service  string `json:"service"`
	Issue    string `json:"issue"`
	Attempts int    `json:"attempts"`
}

type Result struct {
	TaskID    string      `json:"task_id"`
	Success   bool        `json:"success"`
	Output    interface{} `json:"output"`
	Timestamp time.Time   `json:"timestamp"`
}

var tracer trace.Tracer

func initTracer() func() {
	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "localhost:4317", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	exporter, _ := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	res, _ := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String("recovery-agent")))
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("recovery-agent")
	return func() { tp.Shutdown(ctx) }
}

func main() {
	shutdown := initTracer()
	defer shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	_, err := nc.Subscribe("tasks.recover", func(msg *nats.Msg) {
		ctx, span := tracer.Start(context.Background(), "RecoverTask")
		defer span.End()

		var recover RecoverPayload
		json.Unmarshal(msg.Data, &recover)

		span.SetAttributes(
			attribute.String("service", recover.Service),
			attribute.Int("attempts", recover.Attempts),
		)

		action := "restart_service"
		if recover.Attempts > 2 {
			action = "escalate_to_oncall"
		}

		log.Printf("Recovery: service=%s issue=%s action=%s", recover.Service, recover.Issue, action)

		result := Result{
			TaskID:  recover.Service + "_recovery",
			Success: true,
			Output:  map[string]string{"action_taken": action, "status": "recovery_initiated"},
			Timestamp: time.Now(),
		}
		respData, _ := json.Marshal(result)
		nc.Publish("tasks.recover.completed", respData)
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Recovery agent started")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
