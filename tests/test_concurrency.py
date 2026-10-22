"""Concurrent Iceberg writers: a stale commit must be rejected, retried, and lose nobody's changes.

Deterministic interleaving: writer B loads the table, writer A commits, then B commits with
stale metadata. Both writers are incremental (each upserts the entities it changed), which is
how concurrent writers to a shared registry behave.
"""
import pandas as pd
from pyiceberg.table import Table

from corpreg import iceberg_sink
from corpreg.datagen import generate
from corpreg.resolution import EntityResolver, standardize


def _golden() -> pd.DataFrame:
    s1, s2, _ = generate(seed=42)
    unified = pd.concat([standardize(s1, "supply_chain"), standardize(s2, "financial")])
    return pd.concat([EntityResolver()(b) for _, b in unified.groupby("country")], ignore_index=True)


def test_concurrent_writer_conflict_is_retried_without_losing_changes(tmp_path, monkeypatch):
    # Same catalog name = same catalog (the SQL catalog scopes tables by name); two instances
    # stand in for two independent pipeline processes.
    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/catalog.db", "warehouse": f"file://{tmp_path}"}
    cat_a, cat_b = iceberg_sink.catalog("shared", **props), iceberg_sink.catalog("shared", **props)
    golden = _golden()
    iceberg_sink.upsert_companies(cat_a, "ns", "companies", golden, "seed")

    # Two rows in the same partition (same data file), so B's rewrite really conflicts with A's.
    country = golden.country.iloc[0]
    rows = golden[golden.country == country].head(2)
    change_a = rows.head(1).assign(employee_count=111.0)
    change_b = rows.tail(1).assign(employee_count=222.0)

    original_upsert, original_refresh = Table.upsert, Table.refresh
    state = {"calls": 0, "conflicts": 0}

    def stale_first_refresh(self):
        return self if state["calls"] == 0 else original_refresh(self)  # B's 1st attempt stays stale

    def upsert_with_interleaved_writer(self, *args, **kwargs):
        state["calls"] += 1
        if state["calls"] == 1:  # A commits after B loaded metadata, before B commits
            monkeypatch.setattr(Table, "upsert", original_upsert)
            iceberg_sink.upsert_companies(cat_a, "ns", "companies", change_a, "run-a")
            monkeypatch.setattr(Table, "upsert", upsert_with_interleaved_writer)
        try:
            return original_upsert(self, *args, **kwargs)
        except iceberg_sink.RETRYABLE:
            state["conflicts"] += 1
            raise

    monkeypatch.setattr(Table, "refresh", stale_first_refresh)
    monkeypatch.setattr(Table, "upsert", upsert_with_interleaved_writer)
    monkeypatch.setattr(iceberg_sink.time, "sleep", lambda _: None)
    result = iceberg_sink.upsert_companies(cat_b, "ns", "companies", change_b, "run-b")
    monkeypatch.undo()

    assert state["conflicts"] == 1, "B's stale commit must be rejected, then retried"
    assert result["rows_updated"] == 1
    final = cat_a.load_table("ns.companies").scan().to_pandas().set_index("canonical_id")
    assert final.loc[change_a.canonical_id.iloc[0], "employee_count"] == 111.0, "A's change survived"
    assert final.loc[change_b.canonical_id.iloc[0], "employee_count"] == 222.0, "B's change landed on retry"
    assert len(final) == len(golden), "no rows lost or duplicated"
    runs = {s.summary["run_id"] for s in cat_a.load_table("ns.companies").snapshots() if "run_id" in s.summary}
    assert {"run-a", "run-b"} <= runs
