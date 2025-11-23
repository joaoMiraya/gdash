package models

import "time"

type WeatherData struct {
	City                 string    `json:"city"`
	Country              string    `json:"country"`
	Temperature          float64   `json:"temperature"`
	FeelsLike            float64   `json:"feels_like"`
	Humidity             int       `json:"humidity"`
	Pressure             int       `json:"pressure"`
	WindSpeed            float64   `json:"wind_speed"`
	WindDirection        int       `json:"wind_direction"`
	Clouds               int       `json:"clouds"`
	Visibility           int       `json:"visibility"`
	Condition            string    `json:"condition"`
	ConditionDescription string    `json:"condition_description"`
	Icon                 string    `json:"icon"`
	Rain1h               *float64  `json:"rain_1h"`
	Snow1h               *float64  `json:"snow_1h"`
	Sunrise              time.Time `json:"sunrise"`
	Sunset               time.Time `json:"sunset"`
	CollectedAt          time.Time `json:"collected_at"`
}

// Validate verifica se os dados obrigatórios estão presentes
func (w *WeatherData) Validate() []string {
	var errors []string

	if w.City == "" {
		errors = append(errors, "city is required")
	}
	if w.Country == "" {
		errors = append(errors, "country is required")
	}
	if w.Temperature < -100 || w.Temperature > 60 {
		errors = append(errors, "temperature out of valid range (-100 to 60)")
	}
	if w.Humidity < 0 || w.Humidity > 100 {
		errors = append(errors, "humidity must be between 0 and 100")
	}
	if w.CollectedAt.IsZero() {
		errors = append(errors, "collected_at is required")
	}

	return errors
}

// APIResponse representa a resposta da API NestJS
type APIResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	ID      string `json:"id,omitempty"`
}