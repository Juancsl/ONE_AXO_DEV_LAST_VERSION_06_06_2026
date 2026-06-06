# Archivo: GENERIC_ETL_PIPELINE/oneaxo_cfg_pkg/sources/kafka_source.py
import logging
import json
from confluent_kafka import Consumer, KafkaException

class KafkaSourceHandler:
    def __init__(self, config: dict):
        self.config = config
        self.kafka_config = config['kafka_config']
        self.topic = config['topic']
        self.max_messages = config.get('max_messages_per_run', 100)
        self.timeout = config.get('poll_timeout_seconds', 5.0)

    def discover(self) -> list[dict]:
        consumer = Consumer(self.kafka_config)
        consumer.subscribe([self.topic])
        jobs = []
        
        try:
            messages_consumed = 0
            while messages_consumed < self.max_messages:
                msg = consumer.poll(timeout=self.timeout)
                if msg is None:
                    # No hay más mensajes en este momento, salimos del bucle
                    break
                if msg.error():
                    raise KafkaException(msg.error())
                
                # Cada mensaje se convierte en un "job"
                # Es crucial guardar el offset y la partición para el "staging"
                jobs.append({
                    "payload_bytes": msg.value(),
                    "kafka_metadata": {
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "key": msg.key().decode('utf-8') if msg.key() else None
                    }
                })
                messages_consumed += 1
        
        except Exception as e:
            logging.error(f"Error consumiendo de Kafka: {e}")
            # Es importante cerrar el consumidor en caso de error
            consumer.close()
            raise
            
        finally:
            logging.info(f"Consumidos {len(jobs)} mensajes de Kafka.")
            # Cerramos la conexión al terminar
            consumer.close()
            
        return jobs