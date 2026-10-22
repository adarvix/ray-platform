#!/usr/bin/env python3
"""Render the pipeline RayJob / RayCronJob for an environment.

    python k8s/render.py --env aws   --image <repo>:<sha> --run-id <id>              > rayjob.yaml
    python k8s/render.py --env aws   --image <repo>:<sha> --cron "0 2 * * *"         > raycronjob.yaml
    python k8s/render.py --env local --image corpreg:dev  --run-id local-1           > rayjob.yaml

One spec (k8s/pipeline/rayjob.yaml) + per-env values (k8s/envs/<env>.yaml). Fails loudly on
any unresolved ${VAR} so a half-configured manifest never reaches the cluster.
"""
from __future__ import annotations

import argparse
import copy
import os
import re
import string
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).parent
RAY_VERSION = "2.59.0"
CONTAINER = {"head": "ray-head", "cpu-workers": "ray-worker", "resolver-workers": "ray-worker"}


def _expand(value: str) -> str:
    out = os.path.expandvars(value)
    missing = re.findall(r"\$\{?([A-Z_][A-Z0-9_]*)\}?", out)
    if missing:
        raise SystemExit(f"render: unresolved environment variables: {sorted(set(missing))}")
    return out


def _env_list(mapping: dict) -> list[dict]:
    return [{"name": k, "value": _expand(str(v))} for k, v in mapping.items()]


def _pods(spec: dict):
    """Yield (group, pod_spec) for the head and every worker group."""
    yield "head", spec["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]
    for g in spec["rayClusterSpec"]["workerGroupSpecs"]:
        yield g["groupName"], g["template"]["spec"]


def render(env: str, image: str, run_id: str | None, cron: str | None, timezone: str) -> dict:
    values = yaml.safe_load((ROOT / "envs" / f"{env}.yaml").read_text())
    job_name = "corpreg-" + re.sub(r"[^a-z0-9-]", "-", (run_id or "scheduled").lower())[:40].strip("-")
    text = string.Template((ROOT / "pipeline" / "rayjob.yaml").read_text()).substitute(
        JOB_NAME=job_name, NAMESPACE=values["namespace"], IMAGE=image, IMAGE_TAG=image.rsplit(":", 1)[-1][:63],
        RUN_ID=run_id or "scheduled", RAY_VERSION=RAY_VERSION)
    doc = yaml.safe_load(text)
    spec = doc["spec"]

    common_env = _env_list(values.get("env", {}))
    for group, pod in _pods(spec):
        placement = values.get("placement", {}).get(group, {})
        if placement.get("nodeSelector"):
            pod["nodeSelector"] = placement["nodeSelector"]
        if placement.get("tolerations"):
            pod["tolerations"] = placement["tolerations"]
        container = next(c for c in pod["containers"] if c["name"] == CONTAINER[group])
        extra = _env_list(values.get("headEnv", {})) if group == "head" else []
        container["env"] = common_env + extra + container.get("env", [])
        if values.get("envFromSecret"):
            container["envFrom"] = [{"secretRef": {"name": values["envFromSecret"]}}]
        res = values.get("resources", {}).get("ray-head" if group == "head" else group)
        if res:
            container["resources"] = res

    for g in spec["rayClusterSpec"]["workerGroupSpecs"]:
        g.update(values.get("replicas", {}).get(g["groupName"], {}))
    if "submitter" in values.get("resources", {}):
        spec["submitterPodTemplate"]["spec"]["containers"][0]["resources"] = values["resources"]["submitter"]
    if "autoscaler" in values.get("resources", {}):
        spec["rayClusterSpec"]["autoscalerOptions"]["resources"] = values["resources"]["autoscaler"]
    if values.get("objectStoreMemory"):
        for g in spec["rayClusterSpec"]["workerGroupSpecs"]:
            if "object-store-memory" in g["rayStartParams"]:
                g["rayStartParams"]["object-store-memory"] = values["objectStoreMemory"]
    if values.get("envFromSecret"):  # the submitter also needs to reach the lake in local mode
        spec["submitterPodTemplate"]["spec"]["containers"][0]["envFrom"] = [
            {"secretRef": {"name": values["envFromSecret"]}}]

    if cron is None:
        return doc

    # RayCronJob: KubeRay creates a uniquely named RayJob per firing; RUN_ID comes from the
    # RayJob's own name via the downward API so each scheduled run has its own lineage id.
    job_spec = copy.deepcopy(spec)
    job_spec["entrypoint"] = "python /home/ray/app/ray_pipeline.py"
    head = job_spec["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]["containers"][0]
    head["env"].append({"name": "RUN_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.labels['ray.io/cluster']"}}})
    return {
        "apiVersion": "ray.io/v1",
        "kind": "RayCronJob",
        "metadata": {"name": "corpreg-nightly", "namespace": values["namespace"],
                     "labels": {"app.kubernetes.io/name": "corpreg-pipeline"}},
        "spec": {"schedule": cron, "timeZone": timezone, "jobTemplate": job_spec},
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--env", choices=["aws", "local"], required=True)
    p.add_argument("--image", required=True)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--run-id")
    group.add_argument("--cron", help="cron expression -> RayCronJob")
    p.add_argument("--timezone", default="Asia/Kolkata")
    a = p.parse_args()
    yaml.safe_dump(render(a.env, a.image, a.run_id, a.cron, a.timezone), sys.stdout, sort_keys=False)


if __name__ == "__main__":
    main()
