"""Load Kafka settings from YAML without reading environment variables."""

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    security_protocol: str
    sasl_mechanism: str
    sasl_jaas_config: str
    username: str
    password: str
    topic: str

    def to_client_config(self) -> dict[str, str]:
        """Return properties for confluent-kafka; topic is used separately."""
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
        }
        if self.security_protocol.startswith("SASL_"):
            config.update({
                "sasl.mechanism": self.sasl_mechanism,
                "sasl.username": self.username,
                "sasl.password": self.password,
            })
        return config


@dataclass(frozen=True)
class Settings:
    kafka: KafkaSettings


def load_settings(path: str | Path | None = None) -> Settings:
    """Read and validate a file. Call once at application startup.

    The default is config.yml beside this module, independent of the current
    working directory. The example file is never loaded automatically.
    """
    config_path = Path(path) if path is not None else Path(__file__).with_name("config.yml")
    if not config_path.is_file():
        raise FileNotFoundError(
            f"Config file not found: {config_path}. Copy config.example.yml to config.yml first."
        )

    try:
        with config_path.open(encoding="utf-8") as stream:
            data = yaml.safe_load(stream)
    except yaml.YAMLError:
        # Parser errors may contain a source line with credentials.
        raise ValueError("Invalid YAML in config file.") from None

    if not isinstance(data, dict) or not isinstance(data.get("kafka"), dict):
        raise ValueError("Config must contain a 'kafka' mapping.")

    kafka = data["kafka"]

    def read_string(key: str, *, allow_empty: bool = False) -> str:
        value = kafka.get(key)
        if not isinstance(value, str) or (not allow_empty and not value.strip()):
            raise ValueError(f"kafka.{key} must be a {'string' if allow_empty else 'non-empty string'}.")
        return value

    protocol = read_string("security.protocol")
    if protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
        raise ValueError("Unsupported kafka.security.protocol.")

    uses_sasl = protocol.startswith("SASL_")
    return Settings(kafka=KafkaSettings(
        bootstrap_servers=read_string("bootstrap.servers"),
        security_protocol=protocol,
        sasl_mechanism=read_string("sasl.mechanism", allow_empty=not uses_sasl),
        sasl_jaas_config=read_string("sasl.jaas.config", allow_empty=True),
        username=read_string("username", allow_empty=not uses_sasl),
        password=read_string("password", allow_empty=not uses_sasl),
        topic=read_string("topic"),
    ))
