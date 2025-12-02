package queue

import (
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"worker/api"
	"worker/config"
	"worker/models"
	"worker/utils"
)

type Consumer struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	cfg       *config.Config
	apiClient *api.Client
	logger    *utils.Logger
	done      chan bool
}

func NewConsumer(cfg *config.Config, apiClient *api.Client, logger *utils.Logger) *Consumer {
	return &Consumer{
		cfg:       cfg,
		apiClient: apiClient,
		logger:    logger,
		done:      make(chan bool),
	}
}

func (c *Consumer) Connect() error {
	var err error

	c.logger.Info("connecting_to_rabbitmq", map[string]interface{}{
		"url": c.cfg.RabbitMQURL,
	})

	c.conn, err = amqp.Dial(c.cfg.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}

	_, err = c.channel.QueueDeclare(
		c.cfg.QueueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Prefetch: processa uma mensagem por vez
	err = c.channel.Qos(1, 0, false)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	c.logger.Info("rabbitmq_connected", map[string]interface{}{
		"queue": c.cfg.QueueName,
	})

	return nil
}

func (c *Consumer) Start() error {
	msgs, err := c.channel.Consume(
		c.cfg.QueueName,
		"",    // consumer tag
		false, // auto-ack (false = manual ack)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	c.logger.Info("consumer_started", map[string]interface{}{
		"queue": c.cfg.QueueName,
	})

	go func() {
		for msg := range msgs {
			c.processMessage(msg)
		}
	}()

	<-c.done
	return nil
}

func (c *Consumer) processMessage(msg amqp.Delivery) {
	c.logger.Info("message_received", map[string]interface{}{
		"delivery_tag": msg.DeliveryTag,
		"size":         len(msg.Body),
	})

	// Parse JSON
	var weather models.WeatherData
	if err := json.Unmarshal(msg.Body, &weather); err != nil {
		c.logger.Error("json_parse_error", map[string]interface{}{
			"error": err.Error(),
		})
		msg.Nack(false, false)
		return
	}

	if errors := weather.Validate(); len(errors) > 0 {
		c.logger.Error("validation_error", map[string]interface{}{
			"errors": errors,
			"city":   weather.City,
		})
		msg.Nack(false, false)
		return
	}

	if err := c.sendWithRetry(&weather); err != nil {
		c.logger.Error("send_failed_after_retries", map[string]interface{}{
			"error": err.Error(),
			"city":  weather.City,
		})
		msg.Nack(false, true)
		return
	}

	// Sucesso
	c.logger.Info("message_processed", map[string]interface{}{
		"city":        weather.City,
		"temperature": weather.Temperature,
	})
	msg.Ack(false)
}

func (c *Consumer) sendWithRetry(data *models.WeatherData) error {
	var lastErr error

	for attempt := 1; attempt <= c.cfg.MaxRetries; attempt++ {
		err := c.apiClient.SendWeatherLog(data)
		if err == nil {
			return nil
		}

		lastErr = err
		c.logger.Warn("retry_attempt", map[string]interface{}{
			"attempt":     attempt,
			"max_retries": c.cfg.MaxRetries,
			"error":       err.Error(),
		})

		// Backoff exponencial: 1s, 2s, 4s...
		time.Sleep(time.Duration(1<<uint(attempt-1)) * time.Second)
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

func (c *Consumer) Stop() {
	c.logger.Info("consumer_stopping", nil)
	close(c.done)

	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
