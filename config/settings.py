"""Load Kafka settings from YAML without reading environment variables."""

from dataclasses import dataclass
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
class MirrorSettings:
    checkpoint_topic: str
    consumer_count: int
    batch_max_records: int

@dataclass(frozen=True)
class Settings:
    source: KafkaSettings
    destination: KafkaSettings
    mirror: MirrorSettings



def load_settings(path: str | Path | None = None) -> Settings:
    """Read and validate a file. Call once at application startup.
    """

    if path is None:
        raise FileNotFoundError(f"Config file not found: {path}")

    config_path = Path(path)
    try:
        with config_path.open(encoding="utf-8") as stream:
            data = yaml.safe_load(stream)
    except yaml.YAMLError:
        # Parser errors may contain a source line with credentials.
        raise ValueError("Invalid YAML in config file.") from None
    config_obj_dict = parse_config(data)
    return Settings(
        source = config_obj_dict['source'],
        destination = config_obj_dict['destination'],
        mirror = config_obj_dict['mirror']
    )


def parse_config(config: dict) -> dict[str, KafkaSettings | MirrorSettings]:
    """Build the three settings objects from the YAML sections."""
    if not isinstance(config, dict):
        raise ValueError("Config must be a mapping.")

    try:
        source_config = config["source"]
        destination_config = config["destination"]
        mirror_config = config["mirror"]
    except KeyError as e:
        raise ValueError(f"Missing key in config: {e}") from None
    
    try:
        source = KafkaSettings(**source_config)
        destination = KafkaSettings(**destination_config)
        mirror = MirrorSettings(**mirror_config)
    except TypeError as e:
        raise ValueError(f"Invalid config: {e}") from None
    
    return {"source": source, "destination": destination, "mirror": mirror}

if __name__ == "__main__":
    config = load_settings("config.yml")
    print(config)