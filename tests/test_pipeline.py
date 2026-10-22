"""Unit tests: data contract, resolution correctness and quality, Iceberg idempotency."""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "app"))

from corpreg.datagen import generate  # noqa: E402
from corpreg.resolution import (EntityResolver, normalize_domain, normalize_name, pairwise_quality,  # noqa: E402
                                stable_id, standardize)


@pytest.fixture(scope="module")
def sources():
    return generate(seed=42)


@pytest.fixture(scope="module")
def golden(sources):
    s1, s2, _ = sources
    unified = pd.concat([standardize(s1, "supply_chain"), standardize(s2, "financial")])
    resolver = EntityResolver()
    return pd.concat([resolver(block) for _, block in unified.groupby("country")], ignore_index=True)


def test_each_source_has_at_least_1000_records(sources):
    s1, s2, _ = sources
    assert len(s1) >= 1000 and len(s2) >= 1000


def test_generation_is_deterministic():
    a, _, _ = generate(seed=7)
    b, _, _ = generate(seed=7)
    pd.testing.assert_frame_equal(a, b)


@pytest.mark.parametrize("raw,expected", [
    ("Northstar Systems Inc.", "northstar systems"),
    ("NORTHSTAR & SYSTEMS LTD", "northstar systems"),
    ("Apexpath Mfg Intl Pvt Ltd", "apexpath manufacturing international"),
])
def test_name_normalization(raw, expected):
    assert normalize_name(raw) == expected


def test_domain_normalization():
    assert normalize_domain("https://www.acme.co.uk/") == normalize_domain("acme.co.uk") == "acme.co.uk"
    assert normalize_domain(None) is None


def test_stable_id_is_deterministic_and_not_python_hash():
    assert stable_id("US", "northstar systems") == stable_id("US", "northstar systems")
    assert stable_id("US", "northstar systems") != stable_id("GB", "northstar systems")


def test_domain_conflict_vetoes_name_match():
    rows = pd.DataFrame({"supplier_id": ["a", "b"], "company_name": ["Apexridge Retail", "Apexbridge Retail"],
                         "country": ["IN", "IN"], "city": ["Pune", "Pune"], "industry": ["retail", "retail"],
                         "website": ["apexridgeretail.in", "apexbridgeretail.in"], "employee_count": [10, 10],
                         "active_contracts": [1, 1], "annual_spend_usd": [1.0, 1.0]})
    out = EntityResolver()(standardize(rows, "supply_chain"))
    assert len(out) == 2, "near-identical names with conflicting websites must stay separate"


def test_resolution_quality_meets_gate(sources, golden):
    quality = pairwise_quality(golden, sources[2])
    assert quality["pairwise_precision"] >= 0.95
    assert quality["pairwise_recall"] >= 0.95


def test_golden_ids_unique_and_resolution_deterministic(sources, golden):
    assert golden.canonical_id.is_unique
    s1, s2, _ = sources
    unified = pd.concat([standardize(s1, "supply_chain"), standardize(s2, "financial")])
    again = pd.concat([EntityResolver()(b) for _, b in unified.groupby("country")], ignore_index=True)
    assert set(again.canonical_id) == set(golden.canonical_id)


def test_resolution_is_independent_of_row_order(sources, golden):
    """Regression: Ray Data's shuffle reorders rows; a replay must yield identical golden records."""
    s1, s2, _ = sources
    unified = pd.concat([standardize(s1, "supply_chain"), standardize(s2, "financial")])
    shuffled = unified.sample(frac=1.0, random_state=123)
    again = pd.concat([EntityResolver()(b) for _, b in shuffled.groupby("country")], ignore_index=True)
    key = "canonical_id"
    pd.testing.assert_frame_equal(golden.sort_values(key).reset_index(drop=True),
                                  again.sort_values(key).reset_index(drop=True))


def test_iceberg_upsert_is_idempotent(tmp_path, golden):
    from corpreg import iceberg_sink

    cat = iceberg_sink.catalog("test", type="sql", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}")
    first = iceberg_sink.upsert_companies(cat, "ns", "companies", golden, "run-1")
    replay = iceberg_sink.upsert_companies(cat, "ns", "companies", golden, "run-2")
    assert first["rows_inserted"] == len(golden)
    assert replay["rows_inserted"] == replay["rows_updated"] == 0
    assert replay["snapshot_id"] == first["snapshot_id"], "a replay must not create a new snapshot"

    changed = golden.copy()
    changed.loc[0, "employee_count"] = 999_999.0
    third = iceberg_sink.upsert_companies(cat, "ns", "companies", changed, "run-3")
    assert (third["rows_inserted"], third["rows_updated"]) == (0, 1)
    snap = cat.load_table("ns.companies").current_snapshot()
    assert snap.summary["run_id"] == "run-3", "snapshots carry run_id for lineage"
