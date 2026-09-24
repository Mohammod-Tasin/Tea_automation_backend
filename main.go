package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var validDrinks = map[string]bool{"coffee": true, "tea": true, "hotwater": true}

var unlockSeconds = 25
var frontendOrigin = "*"

type machineState struct {
	mu           sync.Mutex
	lastSeen     time.Time
	busyUntil    time.Time
	busyDrink    string
	pendingDrink string
	notify       chan struct{}
}

func newMachineState() *machineState {
	return &machineState{notify: make(chan struct{})}
}

func (s *machineState) online() bool {
	return time.Since(s.lastSeen) <= 60*time.Second
}

var state = newMachineState()
var nextID int64

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	state.mu.Lock()
	online := state.online()
	busy := time.Now().Before(state.busyUntil)
	drink := ""
	secondsLeft := 0
	if busy {
		drink = state.busyDrink
		secondsLeft = int(time.Until(state.busyUntil).Seconds())
	}
	state.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"online":      online,
		"busy":        busy,
		"drink":       drink,
		"secondsLeft": secondsLeft,
	})
}

func unlockHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Drink string `json:"drink"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || !validDrinks[body.Drink] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid drink"})
		return
	}

	state.mu.Lock()
	if !state.online() {
		state.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "machine offline"})
		return
	}
	if state.pendingDrink != "" || time.Now().Before(state.busyUntil) {
		state.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "machine busy"})
		return
	}
	state.pendingDrink = body.Drink
	close(state.notify)
	state.notify = make(chan struct{})
	state.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"drink":   body.Drink,
		"seconds": unlockSeconds,
	})
}

func pollHandler(w http.ResponseWriter, r *http.Request) {
	wait := 25
	if v := r.URL.Query().Get("wait"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			wait = n
		}
	}
	deadline := time.Now().Add(time.Duration(wait) * time.Second)

	state.mu.Lock()
	state.lastSeen = time.Now()
	state.mu.Unlock()

	for {
		state.mu.Lock()
		if state.pendingDrink != "" {
			drink := state.pendingDrink
			state.pendingDrink = ""
			state.busyUntil = time.Now().Add(time.Duration(unlockSeconds) * time.Second)
			state.busyDrink = drink
			state.mu.Unlock()

			id := atomic.AddInt64(&nextID, 1)
			writeJSON(w, http.StatusOK, map[string]any{
				"id":      id,
				"drink":   drink,
				"seconds": unlockSeconds,
			})
			return
		}
		ch := state.notify
		state.mu.Unlock()

		remaining := time.Until(deadline)
		if remaining <= 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ch:
			timer.Stop()
			continue
		case <-timer.C:
			w.WriteHeader(http.StatusNoContent)
			return
		case <-r.Context().Done():
			timer.Stop()
			return
		}
	}
}

func withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", frontendOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	port := getenv("PORT", "8080")
	if v := os.Getenv("UNLOCK_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			unlockSeconds = n
		}
	}
	frontendOrigin = getenv("FRONTEND_ORIGIN", "*")

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", withCORS(healthzHandler))
	mux.HandleFunc("/api/status", withCORS(statusHandler))
	mux.HandleFunc("/api/unlock", withCORS(unlockHandler))
	mux.HandleFunc("/api/device/poll", withCORS(pollHandler))

	log.Printf("listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
