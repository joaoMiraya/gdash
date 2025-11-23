package config

import (
	"os"
	"strconv"
)

type Config struct {
	RabbitMQURL string
	QueueName   string
	APIBaseURL  string
	MaxRetries  int
}

func Load() *Config {
	return &Config{
		RabbitMQURL: getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672"),
		QueueName:   getEnv("QUEUE_NAME", "weather_queue"),
		APIBaseURL:  getEnv("API_BASE_URL", "http://localhost:4000"),
		MaxRetries:  getEnvInt("MAX_RETRIES", 3),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return fallback
}