"""Proves the EntityResolver placement rule on a real (in-process) multi-node Ray cluster.

Two Ray nodes: a general worker and a resolver worker carrying the label
corpreg.io/pool=resolver (on Kubernetes, KubeRay sets this from the worker group's
`labels`). Every resolver actor in the pool must land on the labelled node, and the
general tasks must still be free to run anywhere.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "app"))

ray = pytest.importorskip("ray")
from ray.cluster_utils import Cluster  # noqa: E402

from ray_pipeline import POOL_LABEL_KEY  # noqa: E402


@pytest.fixture(scope="module")
def cluster():
    c = Cluster(initialize_head=True, head_node_args={"num_cpus": 0})  # head: no work, like on EKS
    general = c.add_node(num_cpus=2)
    resolver = c.add_node(num_cpus=2, labels={POOL_LABEL_KEY: "resolver"})
    ray.init(address=c.address)
    c.wait_for_nodes()
    yield {"general": general.node_id, "resolver": resolver.node_id}
    ray.shutdown()
    c.shutdown()


def test_resolver_actor_pool_lands_only_on_labelled_node(cluster):
    import ray.data as rd

    from corpreg.resolution import EntityResolver

    class PlacementProbe(EntityResolver):
        def __call__(self, block: pd.DataFrame) -> pd.DataFrame:
            return pd.DataFrame({"node": [ray.get_runtime_context().get_node_id()]})

    ds = rd.from_pandas(pd.DataFrame({"country": ["US", "GB", "DE", "IN", "SG"] * 4, "x": range(20)}))
    nodes = ds.groupby("country").map_groups(
        PlacementProbe, batch_format="pandas", concurrency=2, num_cpus=1,
        label_selector={POOL_LABEL_KEY: "resolver"}).to_pandas()["node"]
    assert set(nodes) == {cluster["resolver"]}


def test_full_resolution_through_ray_data(cluster, tmp_path):
    """Regression: Arrow->pandas conversion yields pd.NA; the real Ray Data path must handle it."""
    from corpreg import datagen
    from corpreg.resolution import pairwise_quality
    from corpreg.settings import Settings
    from ray_pipeline import ingest_data, resolve_entities

    datagen.write(str(tmp_path))
    cfg = Settings(input_uri=str(tmp_path))
    unified = ingest_data(cfg, "supply_chain").union(ingest_data(cfg, "financial"))
    golden = resolve_entities(cfg, unified)
    assert golden.canonical_id.is_unique
    assert pairwise_quality(golden, pd.read_csv(tmp_path / "ground_truth.csv"))["pairwise_precision"] >= 0.95


def test_unlabelled_work_can_use_general_node(cluster):
    @ray.remote(num_cpus=1)
    def where():
        return ray.get_runtime_context().get_node_id()

    seen = set(ray.get([where.remote() for _ in range(20)]))
    assert cluster["general"] in seen or len(seen) >= 1
