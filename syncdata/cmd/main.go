package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"syncdata/operation"
)

func main() {
	c := flag.String("c", "cfg.json", "config file")
	flag.Parse()
	config, err := operation.Load(*c)
	if err != nil {
		fmt.Println(err)
		return
	}
	srv, err := operation.StartServer(config)
	if err != nil {
		fmt.Println(err)
		return
	}

	if config.Sim {
		operation.StartSimulateErrorServer(config)
	}

	shutdown := make(chan os.Signal)
	signal.Notify(shutdown, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-shutdown
	fmt.Println("shutting down upload server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		fmt.Println("upload server shut down failed: ", err)
	}
	fmt.Println("upload server exiting")
}
