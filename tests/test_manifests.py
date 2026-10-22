"""Policy tests on the rendered RayJob: security and scheduling invariants that must not regress."""
import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("render", Path(__file__).parents[1] / "k8s" / "render.py")
render_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(render_mod)


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "ap-south-1")
    monkeypatch.setenv("LAKE_BUCKET", "lake-bucket")
    monkeypatch.setenv("GLUE_DATABASE", "corporate_registry")


def pods(job):
    spec = job["spec"]
    yield "head", spec["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]
    for g in spec["rayClusterSpec"]["workerGroupSpecs"]:
        yield g["groupName"], g["template"]["spec"]
    yield "submitter", spec["submitterPodTemplate"]["spec"]


@pytest.mark.parametrize("env", ["aws", "local"])
def test_every_pod_meets_restricted_pod_security(env, aws_env):
    job = render_mod.render(env, "img:sha", "run-1", None, "UTC")
    for name, pod in pods(job):
        assert pod["securityContext"]["runAsNonRoot"] is True, name
        assert pod["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault", name
        for c in pod["containers"]:
            assert c["securityContext"]["allowPrivilegeEscalation"] is False, (name, c["name"])
            assert c["securityContext"]["capabilities"]["drop"] == ["ALL"], (name, c["name"])
    assert job["spec"]["rayClusterSpec"]["autoscalerOptions"]["securityContext"]["capabilities"]["drop"] == ["ALL"]


def test_resolver_group_is_the_only_resolver_capable_and_pinned_on_aws(aws_env):
    job = render_mod.render("aws", "img:sha", "run-1", None, "UTC")
    groups = {g["groupName"]: g for g in job["spec"]["rayClusterSpec"]["workerGroupSpecs"]}
    labelled = [n for n, g in groups.items() if g.get("labels", {}).get("corpreg.io/pool") == "resolver"]
    assert labelled == ["resolver-workers"]
    pod = groups["resolver-workers"]["template"]["spec"]
    assert pod["nodeSelector"] == {"corpreg.io/pool": "resolver"}
    assert pod["tolerations"][0]["value"] == "resolver"
    head = job["spec"]["rayClusterSpec"]["headGroupSpec"]
    assert head["rayStartParams"]["num-cpus"] == "0"
    assert head["template"]["spec"]["nodeSelector"] == {"corpreg.io/pool": "system"}  # never Spot


def test_llm_secret_reaches_only_resolver_pods(aws_env):
    job = render_mod.render("aws", "img:sha", "run-1", None, "UTC")
    for name, pod in pods(job):
        names = {e["name"] for c in pod["containers"] for e in c.get("env", [])}
        assert ("LLM_API_KEY" in names) == (name == "resolver-workers"), name


def test_no_static_aws_credentials_on_aws(aws_env):
    job = render_mod.render("aws", "img:sha", "run-1", None, "UTC")
    for name, pod in pods(job):
        assert pod.get("serviceAccountName", "ray-job") == "ray-job" or name == "submitter"
        for c in pod["containers"]:
            assert "envFrom" not in c, f"{name}: AWS access must come from IRSA, not injected secrets"
            assert not {e["name"] for e in c.get("env", [])} & {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"}


def test_cron_variant_wraps_the_same_spec(aws_env):
    cron = render_mod.render("aws", "img:sha", None, "0 2 * * *", "Asia/Kolkata")
    assert cron["kind"] == "RayCronJob" and cron["spec"]["schedule"] == "0 2 * * *"
    assert cron["spec"]["jobTemplate"]["rayClusterSpec"]["rayVersion"] == render_mod.RAY_VERSION


def test_unresolved_variables_fail_closed(monkeypatch):
    monkeypatch.delenv("LAKE_BUCKET", raising=False)
    monkeypatch.setenv("AWS_REGION", "x")
    monkeypatch.setenv("GLUE_DATABASE", "x")
    with pytest.raises(SystemExit, match="LAKE_BUCKET"):
        render_mod.render("aws", "img:sha", "run-1", None, "UTC")
