package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/totooicu/go-mytool/stream"
)

func main() {
	initService()

	gw := &gateway{send: stream.Send}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /rpc", gw.handleRPC)

	addr := customString("http_addr", "127.0.0.1:8080")
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Println("http gateway listening on", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("http server error:", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	_ = srv.Close()
}
