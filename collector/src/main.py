import signal
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config.settings import get_settings
from services.weather_api import WeatherAPIClient
from services.queue_producer import QueueProducer
from utils.logger import setup_logger, get_logger

# Setup
setup_logger()
logger = get_logger(__name__)
settings = get_settings()

# Instâncias globais
weather_client = WeatherAPIClient()
queue_producer = QueueProducer()
scheduler = BlockingScheduler()


def collect_and_publish():
    """Job principal: coleta dados e publica na fila"""
    logger.info("job_started", job="collect_weather")
    
    # Busca dados do clima
    weather = weather_client.fetch_weather()
    if not weather:
        logger.warning("job_skipped", reason="failed_to_fetch_weather")
        return
    
    # Publica na fila
    success = queue_producer.publish(weather)
    if success:
        logger.info("job_completed", job="collect_weather", city=weather.city)
    else:
        logger.error("job_failed", job="collect_weather", reason="publish_failed")


def shutdown(signum, frame):
    """Graceful shutdown"""
    logger.info("shutdown_signal_received", signal=signum)
    scheduler.shutdown(wait=False)
    weather_client.close()
    queue_producer.close()
    sys.exit(0)


def main():
    # Registra handlers de shutdown
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    
    logger.info(
        "collector_starting",
        city=settings.weather_city,
        interval_seconds=settings.collect_interval_seconds
    )
    
    # Conecta ao RabbitMQ
    if not queue_producer.connect():
        logger.error("startup_failed", reason="rabbitmq_connection")
        sys.exit(1)
    
    # Executa imediatamente na inicialização
    collect_and_publish()
    
    # Agenda execuções periódicas
    scheduler.add_job(
        collect_and_publish,
        trigger=IntervalTrigger(seconds=settings.collect_interval_seconds),
        id="weather_collector",
        name="Collect Weather Data",
        replace_existing=True
    )
    
    logger.info("scheduler_started")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        shutdown(None, None)


if __name__ == "__main__":
    main()