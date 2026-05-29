package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

// --- Models ---
type PostEvent struct {
	PostID    string `json:"post_id"`
	Author    string `json:"author"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
	RequestID string `json:"request_id"`
}

type APIError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details any    `json:"details"`
	} `json:"error"`
}

// --- Configuration ---
type Config struct {
	KafkaBroker string
	RedisAddr   string
	Topic       string
	GroupID     string
	Port        string
	JWTSecret   string
}

func loadConfig() Config {
	return Config{
		KafkaBroker: getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
		RedisAddr:   getEnv("REDIS_URL", "redis:6379"),
		Topic:       getEnv("KAFKA_TOPIC", "post-events"),
		GroupID:     getEnv("KAFKA_GROUP_ID", "scalebreeze-consumers"),
		Port:        getEnv("PORT", ":8081"),
		JWTSecret:   getEnv("JWT_SECRET", "super-secret-key"),
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		if strings.HasPrefix(value, "redis://") {
			value = strings.TrimPrefix(value, "redis://")
			if idx := strings.Index(value, "/"); idx != -1 {
				value = value[:idx]
			}
		}
		return value
	}
	return fallback
}

// --- Kafka Interface ---
type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// --- Global Clients ---
var writer MessageWriter
var rdb *redis.Client
var cfg Config

func init() {
	// Configure zerolog for JSON output to stdout
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
}

func connectKafkaWithRetry(ctx context.Context) (MessageWriter, error) {
	var lastErr error
	backoff := 1 * time.Second

	for i := 1; i <= 5; i++ {
		log.Info().Msgf("Attempt %d: Connecting to Kafka at %s...", i, cfg.KafkaBroker)

		w := &kafka.Writer{
			Addr:                   kafka.TCP(cfg.KafkaBroker),
			Topic:                  cfg.Topic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		}

		conn, err := kafka.DialContext(ctx, "tcp", cfg.KafkaBroker)
		if err == nil {
			conn.Close()
			log.Info().Msg("Successfully connected to Kafka")
			return w, nil
		}

		lastErr = err
		log.Warn().Err(err).Msgf("Connection failed. Retrying in %v...", backoff)

		select {
		case <-time.After(backoff):
			backoff *= 2
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("failed to connect to Kafka after 5 attempts: %w", lastErr)
}

func connectRedisWithRetry(ctx context.Context) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})

	var lastErr error
	backoff := 1 * time.Second
	for i := 1; i <= 5; i++ {
		if err := client.Ping(ctx).Err(); err == nil {
			log.Info().Msg("Successfully connected to Redis")
			return client, nil
		} else {
			lastErr = err
			log.Warn().Err(err).Msgf("Redis connection failed. Retrying in %v...", backoff)
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return nil, fmt.Errorf("failed to connect to Redis after 5 attempts: %w", lastErr)
}

// --- Auth Middleware ---
func validateJWT(tokenString string) (string, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(cfg.JWTSecret), nil
	})

	if err != nil {
		return "", err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		if sub, ok := claims["sub"].(string); ok {
			return sub, nil
		}
	}
	return "", fmt.Errorf("invalid token claims")
}

// --- Kafka Consumer (Fan-out Engine) ---
func startConsumer(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.KafkaBroker},
		GroupID: cfg.GroupID,
		Topic:   cfg.Topic,
	})
	defer r.Close()

	log.Info().Msgf("[CONSUMER] Joined group %s, reading from %s", cfg.GroupID, cfg.Topic)

	for {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error().Err(err).Msg("[CONSUMER] Error reading message")
			continue
		}

		var event PostEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Error().Err(err).Msg("[CONSUMER] Error unmarshaling")
			continue
		}

		if event.Action == "created" {
			go fanOutPost(ctx, event)
		}
	}
}

func fanOutPost(ctx context.Context, event PostEvent) {
	l := log.With().Str("request_id", event.RequestID).Str("post_id", event.PostID).Logger()
	
	followers := []string{"follower-1", "follower-2", "follower-3"}

	for _, followerID := range followers {
		key := fmt.Sprintf("user:%s:feed", followerID)
		err := rdb.LPush(ctx, key, event.PostID).Err()
		if err != nil {
			l.Error().Err(err).Str("follower_id", followerID).Msg("[FAN-OUT] Error pushing to feed")
		} else {
			rdb.LTrim(ctx, key, 0, 99)
		}
	}
	l.Info().Int("follower_count", len(followers)).Msg("[FAN-OUT] Processed post successfully")
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
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = "gen-" + uuid.NewString()
	}
	l := log.With().Str("request_id", requestID).Str("method", r.Method).Str("path", r.URL.Path).Logger()

	w.Header().Set("Access-Control-Allow-Origin", "https://localhost:8889")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Request-ID")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// JWT Authentication
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, "Bearer ") {
		l.Warn().Msg("Missing or invalid authorization header")
		sendError(w, "UNAUTHORIZED", "Missing or invalid token", http.StatusUnauthorized)
		return
	}
	token := strings.TrimPrefix(authHeader, "Bearer ")
	_, err := validateJWT(token)
	if err != nil {
		l.Warn().Err(err).Msg("JWT validation failed")
		sendError(w, "UNAUTHORIZED", "Invalid token", http.StatusUnauthorized)
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
	event.RequestID = requestID

	if event.PostID == "" || event.Author == "" || event.Action == "" {
		sendError(w, "VALIDATION_ERROR", "Missing required fields", http.StatusUnprocessableEntity)
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
		l.Error().Err(err).Msg("Failed to publish to Kafka")
		sendError(w, "KAFKA_ERROR", "Failed to publish event", http.StatusServiceUnavailable)
		return
	}

	l.Info().Str("post_id", event.PostID).Msg("Published event successfully")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"status": "event_published",
		"data":   event,
	})
}

func main() {
	cfg = loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Connect to Redis
	var err error
	rdb, err = connectRedisWithRetry(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Critical Redis error")
	}
	defer rdb.Close()

	// Connect to Kafka
	writer, err = connectKafkaWithRetry(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Critical Kafka error")
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Error().Err(err).Msg("Error closing Kafka writer")
		}
	}()

	go startConsumer(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/events", handleEvents)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:    cfg.Port,
		Handler: mux,
	}

	go func() {
		log.Info().Msgf("Event service starting on %s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("Server failed")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("Shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("HTTP shutdown error")
	}
	log.Info().Msg("Graceful shutdown complete")
}
