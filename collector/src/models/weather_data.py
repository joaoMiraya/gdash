from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class WeatherData(BaseModel):
    city: str
    country: str
    temperature: float          # Celsius
    feels_like: float           # Celsius
    humidity: int               # %
    pressure: int               # hPa
    wind_speed: float           # m/s
    wind_direction: int         # degrees
    clouds: int                 # %
    visibility: int             # metros
    condition: str              # ex: "Clear", "Rain", "Clouds"
    condition_description: str  # ex: "clear sky", "light rain"
    icon: str                   # código do ícone
    rain_1h: Optional[float] = None  # mm (última hora)
    snow_1h: Optional[float] = None  # mm (última hora)
    sunrise: datetime
    sunset: datetime
    collected_at: datetime
    
    def to_queue_message(self) -> dict:
        """Converte para formato JSON da fila"""
        return self.model_dump(mode="json")