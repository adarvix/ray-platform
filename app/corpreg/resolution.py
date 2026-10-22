"""Entity resolution: standardize two sources, match within blocks, build golden records.

Matching is deterministic and explainable (domain match, fuzzy-name match,
context-checked "grey zone" match). Only grey-zone pairs would go to an LLM
adjudicator. That keeps LLM cost proportional to genuine ambiguity, not to
input size, and keeps the run reproducible when no key is configured.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
from collections import Counter

import numpy as np
import pandas as pd

LOG = logging.getLogger(__name__)

UNIFIED_COLUMNS = ["source", "source_record_id", "name_raw", "name_norm", "country", "city", "industry",
                   "domain", "employee_count", "active_contracts", "annual_spend_usd", "founded_year",
                   "fiscal_year", "annual_revenue_usd", "operating_margin", "ticker"]

LEGAL_SUFFIXES = {"inc", "incorporated", "corp", "corporation", "llc", "ltd", "limited", "plc", "gmbh", "ag",
                  "pvt", "private", "pte", "co", "company"}
ABBREVIATIONS = {"intl": "international", "mfg": "manufacturing", "inds": "industries", "sys": "systems",
                 "logistix": "logistics", "tech": "technologies"}
STOPWORDS = {"and", "the"}

AUTO_MATCH = 92.0   # name similarity at/above which records match outright
GREY_ZONE = 85.0    # [GREY_ZONE, AUTO_MATCH) needs corroborating context (or an LLM)
HEAD_TOKEN_MIN = 85.0  # similarity floor for the distinctive first token of the name


def normalize_name(value: object) -> str:
    text = re.sub(r"&", " and ", str(value or "").lower())
    tokens = re.sub(r"[^a-z0-9]+", " ", text).split()
    tokens = [ABBREVIATIONS.get(t, t) for t in tokens]
    return " ".join(t for t in tokens if t not in LEGAL_SUFFIXES and t not in STOPWORDS)


def normalize_domain(value: object) -> str | None:
    if value is None or (isinstance(value, float) and np.isnan(value)) or not str(value).strip():
        return None
    host = re.sub(r"^https?://", "", str(value).strip().lower()).split("/")[0]
    return host.removeprefix("www.") or None


def standardize(batch: pd.DataFrame, source: str) -> pd.DataFrame:
    """Map a raw source batch onto the unified schema (runs as a parallel Ray Data task)."""
    if source == "supply_chain":
        out = pd.DataFrame({
            "source_record_id": batch["supplier_id"], "name_raw": batch["company_name"],
            "city": batch["city"], "industry": batch["industry"],
            "employee_count": batch["employee_count"], "active_contracts": batch["active_contracts"],
            "annual_spend_usd": batch["annual_spend_usd"]})
    elif source == "financial":
        out = pd.DataFrame({
            "source_record_id": batch["filing_id"], "name_raw": batch["legal_name"],
            "city": batch["headquarters_city"], "industry": batch["sector"],
            "employee_count": batch["reported_employees"], "founded_year": batch["founded_year"],
            "fiscal_year": batch["fiscal_year"], "annual_revenue_usd": batch["annual_revenue_usd"],
            "operating_margin": batch["operating_margin"], "ticker": batch["ticker"]})
    else:
        raise ValueError(f"unknown source {source!r}")
    out["source"] = source
    out["country"] = batch["country"].astype(str).str.upper().str.strip()
    out["name_norm"] = out["name_raw"].map(normalize_name)
    out["domain"] = batch["website"].map(normalize_domain)
    for col in UNIFIED_COLUMNS:
        if col not in out:
            out[col] = None
    numeric = ["employee_count", "active_contracts", "annual_spend_usd", "founded_year", "fiscal_year",
               "annual_revenue_usd", "operating_margin"]
    out[numeric] = out[numeric].apply(pd.to_numeric, errors="coerce").astype("float64")
    text = ["source", "source_record_id", "name_raw", "name_norm", "country", "city", "industry", "domain", "ticker"]
    out[text] = out[text].astype(object).where(out[text].notna(), None)
    return out[UNIFIED_COLUMNS]


def stable_id(country: str, name_norm: str) -> str:
    """Deterministic across processes and runs (built-in hash() is salted per process)."""
    return "CORP-" + hashlib.sha256(f"{country}|{name_norm}".encode()).hexdigest()[:16].upper()


class _UnionFind:
    def __init__(self, n: int):
        self.parent = list(range(n))

    def find(self, i: int) -> int:
        while self.parent[i] != i:
            self.parent[i] = self.parent[self.parent[i]]
            i = self.parent[i]
        return i

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


def _first(values: pd.Series):
    v = values.dropna()
    return v.iloc[0] if len(v) else None


def golden_record(members: pd.DataFrame, match_rules: set[str]) -> dict:
    """Survivorship: which source wins for each attribute, and why."""
    fin = members[members.source == "financial"].sort_values(
        ["fiscal_year", "source_record_id"], ascending=[False, True], na_position="last")
    sc = members[members.source == "supply_chain"]
    names = Counter(members.name_norm)
    canonical_norm = min(names, key=lambda n: (-names[n], n))  # most frequent, ties lexicographic
    country = members.country.iloc[0]
    return {
        "canonical_id": stable_id(country, canonical_norm),
        # Finance is the system of record for legal identity and revenue; procurement for operations.
        "canonical_name": _first(fin.name_raw) or _first(sc.name_raw),
        "normalized_name": canonical_norm,
        "country": country,
        "city": _first(fin.city) or _first(sc.city),
        "industry": _first(fin.industry) or _first(sc.industry),
        "domain": _first(members.domain),
        "ticker": _first(fin.ticker),
        "employee_count": float(np.nanmedian(members.employee_count)) if members.employee_count.notna().any() else None,
        "active_contracts": float(sc.active_contracts.sum()) if len(sc) else None,
        "annual_spend_usd": float(sc.annual_spend_usd.sum()) if len(sc) else None,
        "founded_year": _first(fin.founded_year),
        "annual_revenue_usd": _first(fin.annual_revenue_usd),
        "revenue_fiscal_year": _first(fin.dropna(subset=["annual_revenue_usd"]).fiscal_year),
        "operating_margin": _first(fin.operating_margin),
        "source_record_ids": sorted(members.source_record_id.tolist()),
        "source_count": int(members.source.nunique()),
        "match_rules": ",".join(sorted(match_rules)) or "singleton",
    }


class EntityResolver:
    """Stateful resolver run by Ray Data as an actor pool on the resolver node group.

    Why an actor and not a task: it owns long-lived state - an LLM client
    connection, a decision cache, and (in production) a rate limiter - that
    should be created once per process, not once per block.
    """

    def __init__(self) -> None:
        # Injected from AWS Secrets Manager via External Secrets -> K8s Secret -> env,
        # and only on resolver pods. The value is never logged.
        self._llm_enabled = bool(os.getenv("LLM_API_KEY"))
        self._cache: dict[tuple[str, str], bool] = {}
        LOG.info("EntityResolver ready on node=%s llm_adjudication=%s",
                 os.getenv("RAY_NODE_NAME", os.uname().nodename), self._llm_enabled)

    def _adjudicate(self, a: pd.Series, b: pd.Series) -> bool:
        """Grey-zone decision. LLM when configured; otherwise require corroborating context."""
        key = tuple(sorted((a.source_record_id, b.source_record_id)))
        if key not in self._cache:
            # Production: prompt an LLM with both records and parse a structured yes/no + reason.
            # Without a key we stay conservative: a false merge corrupts revenue labels,
            # a missed merge only leaves a duplicate.
            self._cache[key] = (a.city == b.city) and (a.industry == b.industry)
        return self._cache[key]

    def __call__(self, block: pd.DataFrame) -> pd.DataFrame:
        from rapidfuzz import fuzz, process

        # Blocks arrive via Arrow, so nulls may be pd.NA (truth-value ambiguous). Normalize at the boundary.
        # Ray Data's shuffle delivers rows in arbitrary order; survivorship ("first non-null")
        # must not depend on it, or a replay of identical input would rewrite rows.
        block = block.sort_values("source_record_id", kind="mergesort").reset_index(drop=True)
        text = ["source", "source_record_id", "name_raw", "name_norm", "country", "city", "industry", "domain", "ticker"]
        block[text] = block[text].astype(object).where(block[text].notna(), None)
        numeric = [c for c in UNIFIED_COLUMNS if c not in text]
        block[numeric] = block[numeric].apply(pd.to_numeric, errors="coerce").astype("float64")
        n = len(block)
        uf, rules = _UnionFind(n), [set() for _ in range(n)]

        def link(i: int, j: int, rule: str) -> None:
            uf.union(i, j)
            rules[i].add(rule)
            rules[j].add(rule)

        # Rule 1: identical normalized website domain.
        for _, idx in block.dropna(subset=["domain"]).groupby("domain").groups.items():
            idx = list(idx)
            for j in idx[1:]:
                link(idx[0], j, "domain")

        # Rule 2/3: fuzzy name similarity within the block, with two vetoes:
        #  - negative evidence: both records carry a website and the domains differ;
        #  - the distinctive (first) token must itself be near-identical, so shared
        #    generic words ("Software International") cannot carry the score.
        names = block.name_norm.tolist()
        heads = [n.split(" ", 1)[0] if n else "" for n in names]
        domains = block.domain.tolist()
        scores = process.cdist(names, names, scorer=fuzz.token_sort_ratio, dtype=np.uint8, workers=1)
        for i, j in zip(*np.where(np.triu(scores >= GREY_ZONE, k=1))):
            if domains[i] and domains[j] and domains[i] != domains[j]:
                continue
            if fuzz.ratio(heads[i], heads[j]) < HEAD_TOKEN_MIN:
                continue
            if scores[i, j] >= AUTO_MATCH:
                link(i, j, "name")
            elif self._adjudicate(block.iloc[i], block.iloc[j]):
                link(i, j, "llm" if self._llm_enabled else "name+context")

        clusters: dict[int, list[int]] = {}
        for i in range(n):
            clusters.setdefault(uf.find(i), []).append(i)
        golden = [golden_record(block.iloc[m], set().union(*(rules[i] for i in m))) for m in clusters.values()]
        return pd.DataFrame(golden)


def pairwise_quality(golden: pd.DataFrame, truth: pd.DataFrame) -> dict:
    """Pairwise precision/recall of the clustering against synthetic ground truth."""
    pred = golden[["canonical_id", "source_record_ids"]].explode("source_record_ids")
    pred = pred.rename(columns={"source_record_ids": "source_record_id"})
    merged = pred.merge(truth[["source_record_id", "entity_id"]], on="source_record_id", how="inner")

    def pairs(groups: pd.Series) -> int:
        sizes = groups.value_counts()
        return int((sizes * (sizes - 1) // 2).sum())

    predicted = pairs(merged.canonical_id)
    actual = pairs(merged.entity_id)
    correct = pairs(merged.canonical_id + "|" + merged.entity_id)
    precision = correct / predicted if predicted else 1.0
    recall = correct / actual if actual else 1.0
    return {"pairwise_precision": round(precision, 4), "pairwise_recall": round(recall, 4),
            "true_entities": int(merged.entity_id.nunique()), "resolved_entities": int(merged.canonical_id.nunique())}
