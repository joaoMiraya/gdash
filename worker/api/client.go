package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"worker/config"
	"worker/models"
	"worker/utils"
)

type Client struct {
	httpClient *http.Client
	baseURL    string
	logger     *utils.Logger
}

func NewClient(cfg *config.Config, logger *utils.Logger) *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURL:    cfg.APIBaseURL,
		logger:     logger,
	}
}

func (c *Client) SendWeatherLog(data *models.WeatherData) error {
	url := fmt.Sprintf("%s/api/weather/logs", c.baseURL)

	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	c.logger.Info("sending_to_api", map[string]interface{}{
		"url":  url,
		"city": data.City,
	})

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("api returned status %d", resp.StatusCode)
	}

	var apiResp models.APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		// Não falha se não conseguir decodificar, status 2xx é suficiente
		c.logger.Warn("could_not_decode_response", map[string]interface{}{
			"error": err.Error(),
		})
	}

	c.logger.Info("api_response", map[string]interface{}{
		"status":  resp.StatusCode,
		"success": apiResp.Success,
		"id":      apiResp.ID,
	})

	return nil
}