from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Weather API
    weather_api_key: str
    weather_city: str = "Presidente Prudente"
    weather_api_url: str = "https://api.openweathermap.org/data/2.5/weather"
    
    # RabbitMQ
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672"
    queue_name: str = "weather_queue"
    
    # Scheduler
    collect_interval_seconds: int = 3600  # 1 hora
    
    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()