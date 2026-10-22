"""Runtime configuration, sourced only from environment variables.

Everything environment-specific (bucket, catalog, thresholds) arrives through
the RayJob's pod env, so one immutable image is promoted unchanged from the
local demo to kind to EKS. Secrets are *not* read here: LLM_API_KEY is read
only inside the EntityResolver actor, on the pods that are allowed to see it.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Settings:
    input_uri: str = field(default_factory=lambda: _env("INPUT_URI", "./data/raw"))
    artifact_uri: str = field(default_factory=lambda: _env("ARTIFACT_URI", "./data/artifacts"))
    # PyIceberg resolves the catalog from PYICEBERG_CATALOG__REGISTRY__* env vars
    # (type=glue on AWS, rest on local Kubernetes, sql in the Docker demo), so the code is catalog-agnostic.
    catalog_name: str = field(default_factory=lambda: _env("ICEBERG_CATALOG", "registry"))
    namespace: str = field(default_factory=lambda: _env("ICEBERG_NAMESPACE", "corporate_registry"))
    companies_table: str = field(default_factory=lambda: _env("ICEBERG_TABLE", "companies"))
    predictions_table: str = field(default_factory=lambda: _env("ICEBERG_PREDICTIONS_TABLE", "revenue_predictions"))
    # Scheduling: the Ray node label that only the resolver worker group carries.
    resolver_pool_label: str = field(default_factory=lambda: _env("RESOLVER_POOL_LABEL", "resolver"))
    resolver_concurrency: int = field(default_factory=lambda: int(_env("RESOLVER_CONCURRENCY", "2")))
    # Ray Data's hash shuffle defaults to 200 partitions with matching aggregator actors; on a small
    # cluster that requests more CPU than exists and the groupby stalls. Size it to the data.
    shuffle_partitions: int = field(default_factory=lambda: int(_env("SHUFFLE_PARTITIONS", "8")))
    # Hash shuffle runs long-lived aggregator actors that each reserve ~1 GiB; on small workers
    # they can reserve all memory and deadlock the shuffle's own map tasks. Sort-based pull
    # shuffle uses plain tasks - right for groupbys of this size. Use hash_shuffle for TB-scale.
    shuffle_strategy: str = field(default_factory=lambda: _env("SHUFFLE_STRATEGY", "sort_shuffle_pull_based"))
    train_workers: int = field(default_factory=lambda: int(_env("TRAIN_WORKERS", "2")))
    # Quality gates: the run fails rather than publishing bad data or a bad model.
    min_rows_per_source: int = field(default_factory=lambda: int(_env("MIN_ROWS_PER_SOURCE", "1000")))
    min_er_precision: float = field(default_factory=lambda: float(_env("MIN_ER_PRECISION", "0.95")))
    min_model_r2: float = field(default_factory=lambda: float(_env("MIN_MODEL_R2", "0.5")))
    commit_retries: int = field(default_factory=lambda: int(_env("ICEBERG_COMMIT_RETRIES", "5")))
