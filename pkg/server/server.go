package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"

	cdcpb "github.com/foden/cdc/api/proto/v1"
	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/interfaces"
	"github.com/foden/cdc/pkg/nats"
)

// ServerConfig holds configuration for the dual gRPC + REST server.
type ServerConfig struct {
	GRPCPort int
	HTTPPort int
}

// AppServer manages both gRPC and the grpc-gateway REST proxy.
type AppServer struct {
	cfg        ServerConfig
	appCfg     *config.Config
	natsClient *nats.Client

	grpcServer *grpc.Server
	httpServer *http.Server
	service    *GRPCService
}

// NewAppServer creates a new combined gRPC + REST server.
func NewAppServer(cfg ServerConfig, appCfg *config.Config, natsClient *nats.Client, engine interfaces.PipelineEngine) *AppServer {
	return &AppServer{
		cfg:        cfg,
		appCfg:     appCfg,
		natsClient: natsClient,
		service:    NewGRPCService(appCfg, natsClient, engine),
	}
}

// Start launches the gRPC server and the grpc-gateway REST proxy.
func (s *AppServer) Start() error {
	// 1. gRPC server
	s.grpcServer = grpc.NewServer()
	cdcpb.RegisterCDCServiceServer(s.grpcServer, s.service)
	reflection.Register(s.grpcServer) // grpcurl / grpc-web reflection

	grpcAddr := fmt.Sprintf(":%d", s.cfg.GRPCPort)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("gRPC listen on %s: %w", grpcAddr, err)
	}

	go func() {
		slog.Info("gRPC server started", "port", s.cfg.GRPCPort)
		if err := s.grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server failed", "err", err)
		}
	}()
	// 2. grpc-gateway REST proxy
	ctx := context.Background()
	gwMux := runtime.NewServeMux()

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if err := cdcpb.RegisterCDCServiceHandlerFromEndpoint(ctx, gwMux, grpcAddr, opts); err != nil {
		return fmt.Errorf("grpc-gateway register: %w", err)
	}

	// Expose prometheus metrics and health check endpoint
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"up"}`))
	})
	mux.Handle("/", gwMux)

	// Wrap with CORS middleware for Next.js frontend
	handler := corsMiddleware(mux)

	httpAddr := fmt.Sprintf(":%d", s.cfg.HTTPPort)
	s.httpServer = &http.Server{
		Addr:         httpAddr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		slog.Info("REST gateway (grpc-gateway) started", "port", s.cfg.HTTPPort)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("REST gateway failed", "err", err)
		}
	}()

	return nil
}

// Stop gracefully shuts down both servers.
func (s *AppServer) Stop() {
	slog.Info("shutting down gRPC + REST servers")

	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.httpServer.Shutdown(ctx)
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	slog.Info("servers stopped")
}

// CORS middleware
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
