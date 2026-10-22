"""Revenue model: distributed XGBoost on Ray Train, trained on the harmonized registry.

Spot resilience: workers checkpoint to durable storage every CHECKPOINT_EVERY rounds.
If a Spot node is reclaimed, Ray Train restarts the worker group (FailureConfig) and the
training loop resumes from the latest checkpoint instead of starting over.
"""
from __future__ import annotations

import json
import logging

import numpy as np
import pandas as pd

LOG = logging.getLogger(__name__)

INDUSTRIES = ["software", "manufacturing", "healthcare", "logistics", "retail", "energy"]
COUNTRIES = ["US", "GB", "DE", "IN", "SG"]
NUM_ROUNDS = 300
CHECKPOINT_EVERY = 25
REFERENCE_YEAR = 2025


def features(df: pd.DataFrame) -> pd.DataFrame:
    """Deterministic feature matrix (fixed one-hot vocabulary so train/predict columns match)."""
    x = pd.DataFrame(index=df.index)
    x["log_employees"] = np.log1p(df.employee_count.astype(float))
    x["log_contracts"] = np.log1p(df.active_contracts.astype(float))
    x["log_spend"] = np.log1p(df.annual_spend_usd.astype(float))
    x["company_age"] = REFERENCE_YEAR - df.founded_year.astype(float)
    x["operating_margin"] = df.operating_margin.astype(float)
    for v in INDUSTRIES:
        x[f"industry_{v}"] = (df.industry == v).astype(float)
    for v in COUNTRIES:
        x[f"country_{v}"] = (df.country == v).astype(float)
    return x  # NaNs are fine: XGBoost learns a default direction for missing values


def _train_loop(config: dict) -> None:
    # Runs on every Ray Train worker (Spot worker pool). Each gets a shard of "train".
    import ray.train
    import xgboost
    from ray.train.xgboost import RayTrainReportCallback

    train = ray.train.get_dataset_shard("train").materialize().to_pandas()
    valid = ray.train.get_dataset_shard("valid").materialize().to_pandas()
    dtrain = xgboost.DMatrix(train.drop(columns=["label"]), label=train["label"])
    dvalid = xgboost.DMatrix(valid.drop(columns=["label"]), label=valid["label"])

    class CheckpointEveryN(RayTrainReportCallback):
        # Report only with checkpoints. Per-round reports are synchronous calls to the
        # Train controller; at ~170 rounds they dominated runtime (6 min vs seconds).
        def _report_metrics(self, report_dict):
            pass

    booster, done = None, 0
    checkpoint = ray.train.get_checkpoint()
    if checkpoint:  # resuming after a worker/node failure (e.g. Spot reclaim)
        booster = RayTrainReportCallback.get_model(checkpoint)
        done = booster.num_boosted_rounds()
        print(f"[corpreg] resuming training from checkpoint at round {done}", flush=True)

    xgboost.train(
        config["params"], dtrain, num_boost_round=max(0, NUM_ROUNDS - done), xgb_model=booster,
        evals=[(dtrain, "train"), (dvalid, "valid")], early_stopping_rounds=30, verbose_eval=False,
        callbacks=[CheckpointEveryN(metrics={"valid_rmse": "valid-rmse"}, frequency=CHECKPOINT_EVERY)])


def train_and_evaluate(golden: pd.DataFrame, run_id: str, storage_uri: str, num_workers: int) -> tuple:
    """Fit on labelled entities, evaluate on a held-out split. Returns (booster, metrics)."""
    import ray.data
    import xgboost
    from ray.train import CheckpointConfig, FailureConfig, RunConfig, ScalingConfig
    from ray.train.xgboost import RayTrainReportCallback, XGBoostTrainer
    from corpreg.storage import resolve

    labelled = golden[golden.annual_revenue_usd.notna() & (golden.annual_revenue_usd > 0)]
    x, y = features(labelled), np.log(labelled.annual_revenue_usd.astype(float))  # log target: revenue is heavy-tailed
    rng = np.random.default_rng(42)
    test_mask = rng.random(len(x)) < 0.2
    trainval = x[~test_mask].assign(label=y[~test_mask].values)
    valid_mask = rng.random(len(trainval)) < 0.15

    fs, path = resolve(storage_uri)
    trainer = XGBoostTrainer(
        _train_loop,
        train_loop_config={"params": {"objective": "reg:squarederror", "eval_metric": "rmse", "tree_method": "hist",
                                      "max_depth": 5, "eta": 0.05, "subsample": 0.9, "seed": 42}},
        scaling_config=ScalingConfig(num_workers=num_workers, resources_per_worker={"CPU": 1}),
        datasets={"train": ray.data.from_pandas(trainval[~valid_mask]),
                  "valid": ray.data.from_pandas(trainval[valid_mask])},
        run_config=RunConfig(
            name=f"revenue-model-{run_id}", storage_path=path, storage_filesystem=fs,
            checkpoint_config=CheckpointConfig(num_to_keep=2),
            failure_config=FailureConfig(max_failures=3)),
    )
    result = trainer.fit()
    booster = RayTrainReportCallback.get_model(result.checkpoint)

    pred = booster.predict(xgboost.DMatrix(x[test_mask]))
    actual, predicted = np.exp(y[test_mask].values), np.exp(pred)
    ss_res = float(np.sum((y[test_mask].values - pred) ** 2))
    ss_tot = float(np.sum((y[test_mask].values - y[test_mask].values.mean()) ** 2))
    metrics = {
        "run_id": run_id, "labelled_entities": int(len(labelled)), "test_rows": int(test_mask.sum()),
        "train_workers": num_workers, "boosted_rounds": int(booster.num_boosted_rounds()),
        "r2_log_revenue": round(1 - ss_res / ss_tot, 4),
        "mape": round(float(np.median(np.abs(predicted - actual) / actual)), 4),
        "checkpoint_path": result.checkpoint.path,
    }
    LOG.info("model metrics %s", json.dumps(metrics))
    return booster, metrics


def predict_missing(booster, golden: pd.DataFrame, run_id: str, model_version: str) -> pd.DataFrame:
    """Predict revenue for entities that no source reported revenue for."""
    import xgboost
    target = golden[golden.annual_revenue_usd.isna()]
    if target.empty:
        return pd.DataFrame(columns=["run_id", "canonical_id", "canonical_name", "predicted_revenue_usd",
                                     "model_version", "predicted_at"])
    pred = np.exp(booster.predict(xgboost.DMatrix(features(target))))
    return pd.DataFrame({"run_id": run_id, "canonical_id": target.canonical_id.values,
                         "canonical_name": target.canonical_name.values, "predicted_revenue_usd": pred.round(2),
                         "model_version": model_version, "predicted_at": pd.Timestamp.now(tz="UTC")})
