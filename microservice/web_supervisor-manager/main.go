package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/totooicu/web_supervisor-manager/actions"
	managerapp "github.com/totooicu/web_supervisor-manager/app"
)

type managerConfigFile struct {
	API    managerAPIConfig `json:"api"`
	Custom map[string]any   `json:"custom"`
}
type managerAPIConfig struct {
	ListenAddr       string `json:"listen_addr"`
	DataDir          string `json:"data_dir"`
	GatewayURL       string `json:"gateway_url"`
	GatewayTimeoutMs int64  `json:"gateway_timeout_ms"`
}

func main() {
	configPath := flag.String("config_path", "config.json", "manager configuration path")
	legacy := flag.Bool("legacy", false, "run the historical Redis-stream jobs runner")
	flag.Parse()
	if *legacy {
		initClient()
		log.Println("legacy client started")
		actions.Action()
		return
	}
	path, err := filepath.Abs(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	cfg := managerConfigFile{}
	if data, readErr := os.ReadFile(path); readErr == nil {
		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Fatalf("parse config: %v", err)
		}
	} else if !os.IsNotExist(readErr) {
		log.Fatal(readErr)
	}
	dataDir := cfg.API.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(filepath.Dir(path), "data", "workflow-manager")
	} else if !filepath.IsAbs(dataDir) {
		dataDir = filepath.Join(filepath.Dir(path), dataDir)
	}
	server, err := managerapp.NewServer(managerapp.Config{ListenAddr: cfg.API.ListenAddr, DataDir: dataDir, GatewayURL: cfg.API.GatewayURL, GatewayTimeoutMs: cfg.API.GatewayTimeoutMs, Custom: cfg.Custom})
	if err != nil {
		log.Fatal(fmt.Errorf("create manager: %w", err))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.Serve(ctx); err != nil {
		log.Fatal(err)
	}
}
