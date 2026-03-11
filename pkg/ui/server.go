package ui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"log/slog"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/models"
	"github.com/foden/cdc/pkg/wal"
)

type Server struct {
	config *config.UIConfig
	queue  *wal.Manager[*models.Event]
	server *http.Server
}

func NewServer(cfg *config.UIConfig, queue *wal.Manager[*models.Event]) *Server {
	return &Server{
		config: cfg,
		queue:  queue,
	}
}

func (s *Server) Start() error {
	if s.config == nil || !s.config.Enabled {
		slog.Info("UI server is disabled")
		return nil
	}

	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/inspect", s.handleInspect)

	addr := fmt.Sprintf(":%d", s.config.Port)
	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	slog.Info("Starting HTTP API server", "port", s.config.Port)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("API server failed", "err", err)
		}
	}()
	return nil
}

func (s *Server) Stop() {
	if s.server != nil {
		s.server.Close()
	}
}

func enableCors(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	enableCors(&w)
	if s.queue == nil {
		http.Error(w, "Queue is nil", http.StatusInternalServerError)
		return
	}
	stats := s.queue.GetTotalStats()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleInspect(w http.ResponseWriter, r *http.Request) {
	enableCors(&w)
	if s.queue == nil {
		http.Error(w, "Queue is nil", http.StatusInternalServerError)
		return
	}
	events, err := s.queue.InspectRaw(0, 100) // Top 100 from partition 0
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}
