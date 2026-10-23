"""Corporate registry pipeline on Ray.

    ingest (Ray Data, parallel CSV read + standardize)
      -> entity resolution (EntityResolver actor pool, pinned to the resolver node group)
      -> quality gate (row counts, resolution precision)
      -> Iceberg upsert (one atomic commit, idempotent)
      -> distributed training (Ray Train XGBoost on Spot workers, checkpointed)
      -> model gate -> predictions published to Iceberg

Runs unchanged on a laptop (`--local`), on kind, and on EKS: everything environment-specific
comes from env vars set on the RayJob. Exit code is non-zero if any gate fails, so the RayJob
is marked FAILED and CI/alerting see it.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid

import pandas as pd
import ray
import ray.data as rd
from ray.data.context import ShuffleStrategy

from corpreg import datagen, iceberg_sink, storage, training
from corpreg.resolution import EntityResolver, pairwise_quality, standardize
from corpreg.settings import Settings

LOG = logging.getLogger("corpreg")
SOURCES = {"supply_chain": "supply_chain.csv", "financial": "financial.csv"}
POOL_LABEL_KEY = "corpreg.io/pool"


class QualityGateError(RuntimeError):
    """Raised when data or model quality is below the publish threshold."""


def ingest_data(cfg: Settings, source: str) -> rd.Dataset:
    """Distributed read + standardization of one source with Ray Data (streaming, block-parallel)."""
    fs, path = storage.resolve(f"{cfg.input_uri.rstrip('/')}/{SOURCES[source]}")
    ds = rd.read_csv(path, filesystem=fs)
    return ds.map_batches(standardize, fn_kwargs={"source": source}, batch_format="pandas")


def resolve_entities(cfg: Settings, unified: rd.Dataset) -> pd.DataFrame:
    # Block on country: candidates are only compared within a block, so work is parallel
    # across blocks and each comparison set stays small. The actor pool is placed only on
    # Ray nodes labelled corpreg.io/pool=resolver (the resolver worker group, which in turn
    # is pinned to the resolver Karpenter NodePool by nodeSelector + toleration).
    resolved = unified.groupby("country", num_partitions=cfg.shuffle_partitions).map_groups(
        EntityResolver, batch_format="pandas", compute=rd.ActorPoolStrategy(size=cfg.resolver_concurrency), num_cpus=1,
        label_selector={POOL_LABEL_KEY: cfg.resolver_pool_label}, max_restarts=3, max_task_retries=3)
    # Golden records are O(entities), bounded and small enough to collect on the driver.
    return resolved.to_pandas()


def run_pipeline(cfg: Settings, run_id: str, generate_sources: bool) -> dict:
    started = time.time()
    summary: dict = {"run_id": run_id, "status": "RUNNING"}
    LOG.info("pipeline start run_id=%s cluster_resources=%s", run_id, ray.cluster_resources())
    ctx = rd.DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy(cfg.shuffle_strategy)
    ctx.max_hash_shuffle_aggregators = cfg.shuffle_partitions
    ctx.enable_progress_bars = False  # terminal control codes are noise in CloudWatch; logs keep progress

    # 0. Sources. In production these land from upstream systems; the flag seeds the demo.
    if generate_sources and not storage.exists(f"{cfg.input_uri.rstrip('/')}/{SOURCES['financial']}"):
        datagen.write(cfg.input_uri)
        LOG.info("generated synthetic sources at %s", cfg.input_uri)

    # 1. Distributed ingestion.
    datasets = {name: ingest_data(cfg, name).materialize() for name in SOURCES}
    counts = {name: ds.count() for name, ds in datasets.items()}
    summary["source_rows"] = counts
    if any(c < cfg.min_rows_per_source for c in counts.values()):
        raise QualityGateError(f"source below {cfg.min_rows_per_source} rows: {counts}")

    # 2. Entity resolution on the resolver node group.
    golden = resolve_entities(cfg, datasets["supply_chain"].union(datasets["financial"]))
    summary["resolution"] = {"golden_records": len(golden),
                             "multi_source_entities": int((golden.source_count > 1).sum()),
                             "match_rules": golden.match_rules.value_counts().to_dict()}

    truth_uri = f"{cfg.input_uri.rstrip('/')}/ground_truth.csv"
    if storage.exists(truth_uri):  # only synthetic data has ground truth
        from io import StringIO
        quality = pairwise_quality(golden, pd.read_csv(StringIO(storage.read_text(truth_uri))))
        summary["resolution"]["quality"] = quality
        if quality["pairwise_precision"] < cfg.min_er_precision:
            # A false merge corrupts revenue labels downstream: refuse to publish.
            raise QualityGateError(f"resolution precision {quality['pairwise_precision']} < {cfg.min_er_precision}")

    # 3. Transactional, idempotent upsert into the main Iceberg table.
    cat = iceberg_sink.catalog(cfg.catalog_name)
    summary["iceberg"] = iceberg_sink.upsert_companies(
        cat, cfg.namespace, cfg.companies_table, golden, run_id, attempts=cfg.commit_retries)

    # 4. Distributed training on the harmonized data (Ray Train, Spot workers, checkpointed).
    run_artifacts = f"{cfg.artifact_uri.rstrip('/')}/runs/{run_id}"
    booster, metrics = training.train_and_evaluate(golden, run_id, f"{cfg.artifact_uri.rstrip('/')}/ray-train",
                                                   cfg.train_workers)
    summary["model"] = metrics
    storage.write_text(f"{run_artifacts}/model.json", booster.save_raw("json").decode())
    storage.write_text(f"{run_artifacts}/metrics.json", json.dumps(metrics, indent=2))

    # 5. Model gate: data was published above; a weak model only blocks promotion + predictions.
    if metrics["r2_log_revenue"] < cfg.min_model_r2:
        raise QualityGateError(f"model r2 {metrics['r2_log_revenue']} < {cfg.min_model_r2}; not promoted")
    storage.write_text(f"{cfg.artifact_uri.rstrip('/')}/models/latest.json",
                       json.dumps({"run_id": run_id, "uri": f"{run_artifacts}/model.json", **metrics}))
    preds = training.predict_missing(booster, golden, run_id, model_version=run_id)
    summary["predictions"] = iceberg_sink.publish_predictions(
        cat, cfg.namespace, cfg.predictions_table, preds, run_id, attempts=cfg.commit_retries)

    summary.update(status="SUCCEEDED", duration_s=round(time.time() - started, 1))
    storage.write_text(f"{run_artifacts}/summary.json", json.dumps(summary, indent=2, default=str))
    return summary


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--run-id", default=os.getenv("RUN_ID") or f"manual-{uuid.uuid4().hex[:8]}")
    p.add_argument("--generate-sources", action="store_true", default=os.getenv("GENERATE_SOURCES") == "true",
                   help="seed synthetic CSVs at INPUT_URI if absent (demo only)")
    p.add_argument("--local", action="store_true",
                   help="start an in-process Ray cluster whose node carries the resolver label")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format=f'{{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s",'
                               f'"run_id":"{args.run_id}","msg":"%(message)s"}}')

    if args.local:
        ray.init(labels={POOL_LABEL_KEY: os.getenv("RESOLVER_POOL_LABEL", "resolver")})
    else:
        ray.init()  # inside a RayJob, RAY_ADDRESS points at the cluster's head

    try:
        summary = run_pipeline(Settings(), args.run_id, args.generate_sources)
    except QualityGateError as exc:
        LOG.error("quality gate failed: %s", exc)
        print(json.dumps({"run_id": args.run_id, "status": "FAILED", "reason": str(exc)}))
        return 2
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
