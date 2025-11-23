import json
import pika
from pika.exceptions import AMQPConnectionError
from typing import Optional

from ..config.settings import get_settings
from ..models.weather_data import WeatherData
from ..utils.logger import get_logger

logger = get_logger(__name__)


class QueueProducer:
    def __init__(self):
        self.settings = get_settings()
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
    
    def connect(self) -> bool:
        """Estabelece conexão com RabbitMQ"""
        try:
            params = pika.URLParameters(self.settings.rabbitmq_url)
            self.connection = pika.BlockingConnection(params)
            self.channel = self.connection.channel()
            
            # Declara a fila (cria se não existir)
            self.channel.queue_declare(
                queue=self.settings.queue_name,
                durable=True  # Sobrevive a restart do RabbitMQ
            )
            
            logger.info("rabbitmq_connected", queue=self.settings.queue_name)
            return True
            
        except AMQPConnectionError as e:
            logger.error("rabbitmq_connection_failed", error=str(e))
            return False
    
    def publish(self, weather: WeatherData) -> bool:
        """Publica dados do clima na fila"""
        if not self.channel:
            if not self.connect():
                return False
        
        try:
            message = json.dumps(weather.to_queue_message())
            
            self.channel.basic_publish(
                exchange="",
                routing_key=self.settings.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensagem persistente
                    content_type="application/json"
                )
            )
            
            logger.info(
                "message_published",
                queue=self.settings.queue_name,
                city=weather.city,
                temperature=weather.temperature
            )
            return True
            
        except Exception as e:
            logger.error("publish_failed", error=str(e))
            self.connection = None
            self.channel = None
            return False
    
    def close(self):
        """Fecha conexão com RabbitMQ"""
        if self.connection and self.connection.is_open:
            self.connection.close()
            logger.info("rabbitmq_disconnected")