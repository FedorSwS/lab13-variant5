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
	Timestamp time.Time   `json:"timestamp"`
}

var tracer trace.Tracer

func initTracer() func() {
	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "localhost:4317", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	exporter, _ := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	res, _ := resource.New(ctx, resource.WithAttributes(semconv.ServiceNameKey.String("alerter-agent")))
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	tracer = tp.Tracer("alerter-agent")
	return func() { tp.Shutdown(ctx) }
}

func main() {
	shutdown := initTracer()
	defer shutdown()

	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	_, err := nc.Subscribe("tasks.alert", func(msg *nats.Msg) {
		ctx, span := tracer.Start(context.Background(), "AlertTask")
		defer span.End()

		var alert AlertPayload
		json.Unmarshal(msg.Data, &alert)

		span.SetAttributes(
			attribute.String("service", alert.Service),
			attribute.String("severity", alert.Severity),
		)

		log.Printf("ALERT [%s] %s: %s", alert.Severity, alert.Service, alert.Message)

		result := Result{
			TaskID:    alert.MetricID + "_alert",
			Success:   true,
			Output:    map[string]string{"notification_sent": "true", "channel": "slack"},
			Timestamp: time.Now(),
		}
		respData, _ := json.Marshal(result)
		nc.Publish("tasks.alert.completed", respData)
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Alerter agent started")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
