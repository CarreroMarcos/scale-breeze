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

// --- Configuration ---
const (
	kafkaBroker = "kafka:9092"
	topic       = "post-events"
	groupID     = "scalebreeze-consumers"
	port        = ":8081"
)

// --- Kafka Producer ---
var writer *kafka.Writer

func connectWithRetry(ctx context.Context) (*kafka.Writer, error) {
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

// --- Handlers ---
func handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event PostEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	payload, err := json.Marshal(event)
	if err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	msg := kafka.Message{
		Value: payload,
	}

	err = writer.WriteMessages(r.Context(), msg)
	if err != nil {
		log.Printf("Failed to publish message: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	log.Printf("Published event: %s | Action: %s", event.PostID, event.Action)
	
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{"status": "event_published"})
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to Kafka
	var err error
	writer, err = connectWithRetry(ctx)
	if err != nil {
		log.Fatalf("Critical error: %v", err)
	}
	defer writer.Close()

	// Start Background Consumer
	go startConsumer(ctx)

	// HTTP Routes
	http.HandleFunc("/events", handleEvents)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	log.Printf("Event service starting on %s", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
StatusOK)
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
