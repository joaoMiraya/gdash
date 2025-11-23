package main

import (
	"os"
	"os/signal"
	"syscall"

	"worker/api"
	"worker/config"
	"worker/queue"
	"worker/utils"
)

func main() {
	// Inicializa componentes
	logger := utils.NewLogger()
	cfg := config.Load()

	logger.Info("worker_starting", map[string]interface{}{
		"queue":       cfg.QueueName,
		"api_url":     cfg.APIBaseURL,
		"max_retries": cfg.MaxRetries,
	})

	// Cliente da API
	apiClient := api.NewClient(cfg, logger)

	// Consumer RabbitMQ
	consumer := queue.NewConsumer(cfg, apiClient, logger)

	// Conecta ao RabbitMQ
	if err := consumer.Connect(); err != nil {
		logger.Error("connection_failed", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("shutdown_signal_received", map[string]interface{}{
			"signal": sig.String(),
		})
		consumer.Stop()
	}()

	// Inicia consumo
	if err := consumer.Start(); err != nil {
		logger.Error("consumer_error", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	logger.Info("worker_stopped", nil)
}