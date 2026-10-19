from dataclasses import dataclass


@dataclass(frozen=True)
class Origin:
    cluster_id: str
    topic: str
    partition: int
    offset: int


@dataclass
class MirrorState:
    manifest: dict
    next_offsets: dict[int, int]


@dataclass
class PreparedBatch:
    records: list[dict]
    next_offsets: dict[int, int]
