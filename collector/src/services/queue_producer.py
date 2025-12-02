import json
import pika
from pika.exceptions import AMQPConnectionError, AMQPError
from typing import Optional
import time

from ..config.settings import get_settings
from ..models.weather_data import WeatherData
from ..utils.logger import get_logger

logger = get_logger(__name__)


class QueueProducer:
    def __init__(self):
        self.settings = get_settings()
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.max_retries = 3
        self.retry_delay = 5
    
    def connect(self) -> bool:
        """Estabelece conexão com RabbitMQ com retry e heartbeat"""
        for attempt in range(self.max_retries):
            try:
                params = pika.URLParameters(self.settings.rabbitmq_url)
                
                # Configurações de heartbeat e timeouts
                params.heartbeat = 600
                params.blocked_connection_timeout = 300
                params.connection_attempts = 3
                params.retry_delay = 2
                params.socket_timeout = 10
                
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                
                self.channel.queue_declare(
                    queue=self.settings.queue_name,
                    durable=True
                )
                
                # Confirmar delivery mode
                self.channel.confirm_delivery()
                
                logger.info("rabbitmq_connected", 
                           queue=self.settings.queue_name,
                           attempt=attempt + 1)
                return True
                
            except AMQPConnectionError as e:
                logger.error("rabbitmq_connection_failed", 
                           error=str(e),
                           attempt=attempt + 1,
                           max_retries=self.max_retries)
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    return False
            except Exception as e:
                logger.error("unexpected_connection_error", error=str(e))
                return False
        
        return False
    
    def _ensure_connection(self) -> bool:
        """Verifica e restabelece conexão se necessário"""
        if not self.connection or self.connection.is_closed:
            logger.warning("connection_lost_reconnecting")
            return self.connect()
        
        if not self.channel or self.channel.is_closed:
            logger.warning("channel_lost_reopening")
            try:
                self.channel = self.connection.channel()
                self.channel.confirm_delivery()
                return True
            except Exception as e:
                logger.error("channel_reopen_failed", error=str(e))
                return self.connect()
        
        return True
    
    def publish(self, weather: WeatherData) -> bool:
        """Publica mensagem com retry automático"""
        if not self._ensure_connection():
            return False
        
        for attempt in range(self.max_retries):
            try:
                message = json.dumps(weather.to_queue_message())
                
                self.channel.basic_publish(
                    exchange="",
                    routing_key=self.settings.queue_name,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_type="application/json",
                        timestamp=int(time.time())
                    ),
                    mandatory=True
                )
                
                logger.info(
                    "message_published",
                    queue=self.settings.queue_name,
                    city=weather.city,
                    temperature=weather.temperature,
                    attempt=attempt + 1
                )
                return True
                
            except (AMQPConnectionError, AMQPError) as e:
                logger.error("publish_failed_amqp_error", 
                           error=str(e),
                           attempt=attempt + 1)
                
                # Tentar reconectar
                self.connection = None
                self.channel = None
                
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    if not self._ensure_connection():
                        continue
                else:
                    return False
                    
            except Exception as e:
                logger.error("publish_failed_unexpected", error=str(e))
                return False
        
        return False
    
    def close(self):
        """Fecha conexão de forma segura"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
            
            if self.connection and not self.connection.is_closed:
                self.connection.close()
            
            logger.info("rabbitmq_disconnected")
        except Exception as e:
            logger.warning("close_error", error=str(e))
        finally:
            self.connection = None
            self.channel = None
    
    def __enter__(self):
        """Context manager support"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.close()