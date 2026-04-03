package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/jackstoller/p2p-messaging/internal/monitor"
)

func main() {
	listenAddr := flag.String("listen", ":8080", "HTTP listen address for the monitor UI")
	projectDir := flag.String("project-dir", ".", "path to the docker compose project")
	composeFile := flag.String("compose-file", "docker-compose.yml", "compose file to follow for node logs")
	tailLines := flag.Int("tail", 2000, "number of existing log lines to load before following")
	nodeHTTPPort := flag.Int("node-http-port", 8081, "HTTP API port exposed by each mesh node inside the docker network")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app := monitor.New(monitor.Config{
		ProjectDir:   *projectDir,
		ComposeFile:  *composeFile,
		TailLines:    *tailLines,
		NodeHTTPPort: *nodeHTTPPort,
	})
	app.Start(ctx)

	server := &http.Server{
		Addr:    *listenAddr,
		Handler: app.Handler(),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), monitor.DefaultShutdownTimeout)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("monitor listening on %s", *listenAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("monitor failed: %v", err)
	}
}
