package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
	req, err := http.NewRequest("GET", "/events", nil)
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleEvents)

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func TestHandleEventsInvalidJSON(t *testing.T) {
	body := bytes.NewBufferString(`{"post_id": "missing-bracket"`)
	req, err := http.NewRequest("POST", "/events", body)
	assert.NoError(t, err)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handleEvents)

	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}
