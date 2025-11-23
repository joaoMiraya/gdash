import httpx
from datetime import datetime, timezone
from typing import Optional

from ..config.settings import get_settings
from ..models.weather_data import WeatherData
from ..utils.logger import get_logger

logger = get_logger(__name__)


class WeatherAPIClient:
    def __init__(self):
        self.settings = get_settings()
        self.client = httpx.Client(timeout=30.0)
    
    def fetch_weather(self) -> Optional[WeatherData]:
        """Busca dados do clima na OpenWeatherMap API"""
        try:
            params = {
                "q": self.settings.weather_city,
                "appid": self.settings.weather_api_key,
                "units": "metric",  # Celsius
                "lang": "pt_br"
            }
            
            logger.info("fetching_weather", city=self.settings.weather_city)
            
            response = self.client.get(self.settings.weather_api_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            weather = self._parse_response(data)
            
            logger.info(
                "weather_fetched",
                city=weather.city,
                temperature=weather.temperature,
                condition=weather.condition
            )
            
            return weather
            
        except httpx.HTTPStatusError as e:
            logger.error("weather_api_error", status=e.response.status_code, detail=str(e))
            return None
        except Exception as e:
            logger.error("weather_fetch_failed", error=str(e))
            return None
    
    def _parse_response(self, data: dict) -> WeatherData:
        """Normaliza a resposta da API para WeatherData"""
        main = data["main"]
        wind = data["wind"]
        weather = data["weather"][0]
        sys = data["sys"]
        
        return WeatherData(
            city=data["name"],
            country=sys["country"],
            temperature=main["temp"],
            feels_like=main["feels_like"],
            humidity=main["humidity"],
            pressure=main["pressure"],
            wind_speed=wind.get("speed", 0),
            wind_direction=wind.get("deg", 0),
            clouds=data.get("clouds", {}).get("all", 0),
            visibility=data.get("visibility", 10000),
            condition=weather["main"],
            condition_description=weather["description"],
            icon=weather["icon"],
            rain_1h=data.get("rain", {}).get("1h"),
            snow_1h=data.get("snow", {}).get("1h"),
            sunrise=datetime.fromtimestamp(sys["sunrise"], tz=timezone.utc),
            sunset=datetime.fromtimestamp(sys["sunset"], tz=timezone.utc),
            collected_at=datetime.now(timezone.utc)
        )
    
    def close(self):
        self.client.close()