"""Iceberg writes: transactional upsert of golden records, idempotent prediction publish.

Guarantees:
* Each write is one Iceberg commit (one new snapshot) - readers see all of it or none.
* Optimistic concurrency: a concurrent writer causes CommitFailedException; we reload the
  table metadata and retry with backoff instead of overwriting someone else's commit.
* Idempotency: canonical_id is deterministic and upsert skips unchanged rows, so replaying
  the same input produces zero updates. Predictions overwrite only their own run_id slice.
* Lineage: every snapshot carries run_id in its summary, so `table.history()` maps
  snapshots back to RayJob runs (and time travel can roll back a bad one).
"""
from __future__ import annotations

import logging
import random
import time

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import (CommitFailedException, CommitStateUnknownException, NamespaceAlreadyExistsError,
                                  ValidationException)
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DoubleType, IntegerType, ListType, NestedField, StringType, TimestamptzType

LOG = logging.getLogger(__name__)

COMPANIES_SCHEMA = Schema(
    NestedField(1, "canonical_id", StringType(), required=True),
    NestedField(2, "canonical_name", StringType()),
    NestedField(3, "normalized_name", StringType()),
    NestedField(4, "country", StringType(), required=True),
    NestedField(5, "city", StringType()),
    NestedField(6, "industry", StringType()),
    NestedField(7, "domain", StringType()),
    NestedField(8, "ticker", StringType()),
    NestedField(9, "employee_count", DoubleType()),
    NestedField(10, "active_contracts", DoubleType()),
    NestedField(11, "annual_spend_usd", DoubleType()),
    NestedField(12, "founded_year", DoubleType()),
    NestedField(13, "annual_revenue_usd", DoubleType()),
    NestedField(14, "revenue_fiscal_year", DoubleType()),
    NestedField(15, "operating_margin", DoubleType()),
    NestedField(16, "source_record_ids", ListType(17, StringType(), element_required=True)),
    NestedField(18, "source_count", IntegerType()),
    NestedField(19, "match_rules", StringType()),
    identifier_field_ids=[1],
)

PREDICTIONS_SCHEMA = Schema(
    NestedField(1, "run_id", StringType(), required=True),
    NestedField(2, "canonical_id", StringType(), required=True),
    NestedField(3, "canonical_name", StringType()),
    NestedField(4, "predicted_revenue_usd", DoubleType()),
    NestedField(5, "model_version", StringType()),
    NestedField(6, "predicted_at", TimestamptzType()),
)


def catalog(name: str, **properties: str) -> Catalog:
    # Connection details come from PYICEBERG_CATALOG__<NAME>__* env vars (glue | rest | sql).
    return load_catalog(name, **properties)


def _ensure_table(cat: Catalog, namespace: str, table: str, schema: Schema, spec: PartitionSpec) -> Table:
    # Check first: on Glue the database is owned by Terraform and the job role deliberately
    # lacks glue:CreateDatabase, so a blind create would fail with AccessDenied.
    if not cat.namespace_exists(namespace):
        try:
            cat.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass
    return cat.create_table_if_not_exists(
        f"{namespace}.{table}", schema=schema, partition_spec=spec,
        properties={"format-version": "2", "write.parquet.compression-codec": "zstd"})


# PyIceberg already retries a bare conflict internally by rebuilding the commit on fresh metadata.
# What reaches us:
#   CommitFailedException  - retries exhausted; the commit did not land.
#   ValidationException    - a concurrent writer rewrote files this commit would replace; did not land.
#   CommitStateUnknown     - (REST) unknown if it landed. Normally unsafe to retry, but our writes are
#                            idempotent: if it landed, the re-run upsert diffs to zero changes.
# In every case: re-run the whole operation against a refreshed table, which recomputes the diff.
RETRYABLE = (CommitFailedException, ValidationException, CommitStateUnknownException)


def _with_commit_retry(fn, attempts: int):
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except RETRYABLE:
            if attempt == attempts:
                raise
            delay = min(30.0, 0.5 * 2 ** attempt) * (0.5 + random.random())
            LOG.warning("iceberg commit conflict (attempt %d/%d), retrying in %.1fs", attempt, attempts, delay)
            time.sleep(delay)


def upsert_companies(cat: Catalog, namespace: str, table: str, golden: pd.DataFrame, run_id: str,
                     attempts: int = 5) -> dict:
    spec = PartitionSpec(PartitionField(source_id=4, field_id=1000, transform=IdentityTransform(), name="country"))
    tbl = _ensure_table(cat, namespace, table, COMPANIES_SCHEMA, spec)
    data = pa.Table.from_pandas(golden[[f.name for f in COMPANIES_SCHEMA.fields]], preserve_index=False)
    data = data.cast(tbl.schema().as_arrow())  # catalog may reassign field ids on create

    def commit():
        tbl.refresh()  # pick up any snapshot committed by a concurrent writer before retrying
        return tbl.upsert(data, join_cols=["canonical_id"],
                          snapshot_properties={"run_id": run_id, "writer": "corpreg-pipeline"})

    result = _with_commit_retry(commit, attempts)
    snap = tbl.current_snapshot()
    out = {"table": f"{namespace}.{table}", "rows_inserted": result.rows_inserted,
           "rows_updated": result.rows_updated, "snapshot_id": snap.snapshot_id if snap else None,
           "snapshots_total": len(tbl.history())}
    LOG.info("iceberg upsert committed %s", out)
    return out


def publish_predictions(cat: Catalog, namespace: str, table: str, preds: pd.DataFrame, run_id: str,
                        attempts: int = 5) -> dict:
    spec = PartitionSpec(PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="run_id"))
    tbl = _ensure_table(cat, namespace, table, PREDICTIONS_SCHEMA, spec)
    data = pa.Table.from_pandas(preds[[f.name for f in PREDICTIONS_SCHEMA.fields]], preserve_index=False)
    data = data.cast(tbl.schema().as_arrow())

    def commit():
        tbl.refresh()
        # Replace only this run's slice: re-running the same RUN_ID is idempotent.
        tbl.overwrite(data, overwrite_filter=EqualTo("run_id", run_id), snapshot_properties={"run_id": run_id})

    _with_commit_retry(commit, attempts)
    LOG.info("iceberg predictions published table=%s.%s rows=%d", namespace, table, len(preds))
    return {"table": f"{namespace}.{table}", "rows": len(preds)}
