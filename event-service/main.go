package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// --- Models ---
type PostEvent struct {
	PostID string `json:"post_id"`
	Author string `json:"author"`
	Action string `json:"action"`
}

type APIError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details any    `json:"details"`
	} `json:"error"`
}

// --- Configuration ---
const (
	kafkaBroker = "kafka:9092"
	topic       = "post-events"
	groupID     = "scalebreeze-consumers"
	port        = ":8081"
)

// --- Kafka Interface ---
type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// --- Kafka Producer ---
var writer MessageWriter

func connectWithRetry(ctx context.Context) (MessageWriter, error) {
	var lastErr error
	backoff := 1 * time.Second

	for i := 1; i <= 5; i++ {
		log.Printf("Attempt %d: Connecting to Kafka at %s...", i, kafkaBroker)

		// Create a writer to test connection
		w := &kafka.Writer{
			Addr:                   kafka.TCP(kafkaBroker),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}

		// Try to fetch metadata to verify connection
		conn, err := kafka.DialContext(ctx, "tcp", kafkaBroker)
		if err == nil {
			conn.Close()
			log.Println("Successfully connected to Kafka")
			return w, nil
		}

		lastErr = err
		log.Printf("Connection failed: %v. Retrying in %v...", err, backoff)

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("failed to connect to Kafka after 5 attempts: %w", lastErr)
}

// --- Kafka Consumer ---
func startConsumer(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		GroupID: groupID,
		Topic:   topic,
	})
	defer r.Close()

	log.Printf("[CONSUMER] Joined group %s, reading from %s", groupID, topic)

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[CONSUMER] Error reading message: %v", err)
			continue
		}
		log.Printf("[CONSUMER] Received at offset %d: %s", m.Offset, string(m.Value))
	}
}

// --- Error Helper ---
func sendError(w http.ResponseWriter, code string, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	errResp := APIError{}
	errResp.Error.Code = code
	errResp.Error.Message = message
	errResp.Error.Details = map[string]string{}
	json.NewEncoder(w).Encode(errResp)
}

// --- Handlers ---
func handleEvents(w http.ResponseWriter, r *http.Request) {
	// Security: Basic CORS
	w.Header().Set("Access-Control-Allow-Origin", "https://localhost:8889")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.Method != http.MethodPost {
		sendError(w, "METHOD_NOT_ALLOWED", "Only POST is allowed", http.StatusMethodNotAllowed)
		return
	}

	var event PostEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		sendError(w, "BAD_REQUEST", "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Basic validation
	if event.PostID == "" || event.Author == "" || event.Action == "" {
		sendError(w, "VALIDATION_ERROR", "Missing required fields: post_id, author, action", http.StatusUnprocessableEntity)
		return
	}

	payload, err := json.Marshal(event)
	if err != nil {
		sendError(w, "INTERNAL_ERROR", "Failed to marshal event", http.StatusInternalServerError)
		return
	}

	msg := kafka.Message{
		Value: payload,
	}

	err = writer.WriteMessages(r.Context(), msg)
	if err != nil {
		log.Printf("Failed to publish message: %v", err)
		sendError(w, "KAFKA_ERROR", "Failed to publish event to message queue", http.StatusServiceUnavailable)
		return
	}

	log.Printf("Published event: %s | Action: %s", event.PostID, event.Action)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"status": "event_published",
		"data":   event,
	})
}

func main() {
	// Create context that listens for interrupt signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Connect to Kafka
	var err error
	writer, err = connectWithRetry(ctx)
	if err != nil {
		log.Fatalf("Critical error: %v", err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("Error closing Kafka writer: %v", err)
		}
	}()

	// Start Background Consumer
	go startConsumer(ctx)

	// HTTP Server Configuration
	mux := http.NewServeMux()
	mux.HandleFunc("/events", handleEvents)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:    port,
		Handler: mux,
	}

	// Run server in a goroutine
	go func() {
		log.Printf("Event service starting on %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	<-ctx.Done()
	log.Println("Shutting down gracefully...")

	// Give the server 5 seconds to shut down
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	log.Println("Graceful shutdown complete")
}
