package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

// --- Helpers ---
func createTestToken(userID string) string {
	claims := jwt.MapClaims{
		"sub": userID,
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString([]byte(cfg.JWTSecret))
	return tokenString
}

func TestHealthHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/health", nil)
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "OK", rr.Body.String())
}

func TestPostEventModel(t *testing.T) {
	event := PostEvent{
		PostID: "87892ac7-e796-4b66-9f95-cd6e1ffc72ba",
		Author: "mars",
		Action: "created",
	}

	data, err := json.Marshal(event)
	assert.NoError(t, err)

	var decoded PostEvent
	err = json.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, event.PostID, decoded.PostID)
	assert.Equal(t, event.Author, decoded.Author)
}

func TestHandleEventsMethodNotAllowed(t *testing.T) {
	cfg = Config{JWTSecret: "test-secret"}
	token := createTestToken("test-user")
	req, err := http.NewRequest("GET", "/events", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleEvents)

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
	
	var apiErr APIError
	err = json.NewDecoder(rr.Body).Decode(&apiErr)
	assert.NoError(t, err)
	assert.Equal(t, "METHOD_NOT_ALLOWED", apiErr.Error.Code)
}

func TestHandleEventsInvalidJSON(t *testing.T) {
	cfg = Config{JWTSecret: "test-secret"}
	token := createTestToken("test-user")
	body := bytes.NewBufferString(`{"post_id": "missing-bracket"`)
	req, err := http.NewRequest("POST", "/events", body)
	req.Header.Set("Authorization", "Bearer "+token)
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleEvents)

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	
	var apiErr APIError
	err = json.NewDecoder(rr.Body).Decode(&apiErr)
	assert.NoError(t, err)
	assert.Equal(t, "BAD_REQUEST", apiErr.Error.Code)
}

func TestHandleEventsValidationFailure(t *testing.T) {
	cfg = Config{JWTSecret: "test-secret"}
	token := createTestToken("test-user")
	body := bytes.NewBufferString(`{"post_id": "missing-fields"}`)
	req, err := http.NewRequest("POST", "/events", body)
	req.Header.Set("Authorization", "Bearer "+token)
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleEvents)

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnprocessableEntity, rr.Code)
	
	var apiErr APIError
	err = json.NewDecoder(rr.Body).Decode(&apiErr)
	assert.NoError(t, err)
	assert.Equal(t, "VALIDATION_ERROR", apiErr.Error.Code)
}

type mockWriter struct {
	messages []kafka.Message
}

func (m *mockWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockWriter) Close() error {
	return nil
}

func TestHandleEventsSuccess(t *testing.T) {
	cfg = Config{JWTSecret: "test-secret"}
	token := createTestToken("test-user")
	
	// Setup mock writer
	mw := &mockWriter{}
	writer = mw

	event := PostEvent{
		PostID: "abc-123",
		Author: "mars",
		Action: "created",
	}
	body, _ := json.Marshal(event)
	req, err := http.NewRequest("POST", "/events", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+token)
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleEvents)

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusAccepted, rr.Code)
	assert.Equal(t, 1, len(mw.messages))
	
	var resp map[string]any
	json.NewDecoder(rr.Body).Decode(&resp)
	assert.Equal(t, "event_published", resp["status"])
	assert.NotNil(t, resp["data"])
}

func TestFanOutPost(t *testing.T) {
	// Setup miniredis
	s := miniredis.RunT(t)
	rdb = redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	event := PostEvent{
		PostID: "post-789",
		Author: "celebrity",
		Action: "created",
	}

	fanOutPost(context.Background(), event)

	// Verify that the post ID was pushed to followers' feeds
	followers := []string{"follower-1", "follower-2", "follower-3"}
	for _, f := range followers {
		key := "user:" + f + ":feed"
		val, err := rdb.LPop(context.Background(), key).Result()
		assert.NoError(t, err)
		assert.Equal(t, "post-789", val)
	}
}
