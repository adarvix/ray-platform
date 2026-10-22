"""Synthetic corporate datasets for the entity-resolution + revenue pipeline.

Two sources describe an overlapping population of companies, the way two
departments would in real life:

* ``supply_chain.csv`` (S1) - procurement's supplier master: operational facts
  (employees, contracts, spend) and no revenue.
* ``financial.csv``   (S2) - finance's filings feed: revenue, margin, founding
  year, ticker. Not every supplier files, and not every filer is a supplier.

There is deliberately **no shared key**. Names drift between sources (legal
suffixes, casing, abbreviations, typos, "&" vs "and"), websites are sometimes
missing or formatted differently, and each source contains a few internal
duplicates. A third file, ``ground_truth.csv``, maps every source record to
its true entity so resolution quality can be measured (precision / recall)
rather than assumed. It is never read by the pipeline's matching logic.
"""
from __future__ import annotations

import argparse
import random
from dataclasses import dataclass

import numpy as np
import pandas as pd

COUNTRIES = {"US": ["New York", "Austin", "Chicago"], "GB": ["London", "Manchester"],
             "DE": ["Berlin", "Munich"], "IN": ["Bengaluru", "Pune", "Mumbai"], "SG": ["Singapore"]}
# Revenue per employee (USD) by industry: gives the model a learnable signal.
INDUSTRIES = {"software": 310_000, "manufacturing": 190_000, "healthcare": 230_000,
              "logistics": 160_000, "retail": 120_000, "energy": 420_000}
PREFIXES = ["North", "Blue", "Silver", "Apex", "Vertex", "Quantum", "Summit", "Harbor", "Cedar", "Iron",
            "Nova", "Atlas", "Crescent", "Pioneer", "Lumen", "Granite", "Orion", "Sterling", "Maple", "Falcon"]
STEMS = ["star", "ridge", "field", "wave", "point", "gate", "stone", "bridge", "peak", "line",
         "works", "path", "light", "core", "forge", "dale", "port", "view", "crest", "vale"]
SECTOR_WORDS = {"software": ["Systems", "Software", "Labs"], "manufacturing": ["Industries", "Manufacturing"],
                "healthcare": ["Health", "Medical", "Biotech"], "logistics": ["Logistics", "Freight"],
                "retail": ["Retail", "Stores"], "energy": ["Energy", "Power"]}
SUFFIXES = {"US": ["Inc", "Inc.", "Incorporated", "Corp", "Corporation", "LLC"],
            "GB": ["Ltd", "Limited", "PLC"], "DE": ["GmbH", "AG"], "IN": ["Pvt Ltd", "Private Limited", "Ltd"],
            "SG": ["Pte Ltd", "Pte. Ltd."]}
ABBREVIATIONS = {"International": "Intl", "Manufacturing": "Mfg", "Industries": "Inds", "Systems": "Sys",
                 "Logistics": "Logistix", "Technologies": "Tech"}


@dataclass(frozen=True)
class Entity:
    entity_id: str
    base_name: str
    country: str
    city: str
    industry: str
    domain: str
    employees: int
    founded_year: int
    revenue: float
    operating_margin: float


def _make_entities(n: int, rng: np.random.Generator, prng: random.Random) -> list[Entity]:
    entities, seen = [], set()
    while len(entities) < n:
        industry = prng.choice(list(INDUSTRIES))
        core = prng.choice(PREFIXES) + prng.choice(STEMS)
        name = f"{core} {prng.choice(SECTOR_WORDS[industry])}"
        if prng.random() < 0.15:
            name += " International"
        if name in seen:
            continue
        seen.add(name)
        country = prng.choice(list(COUNTRIES))
        employees = int(np.clip(rng.lognormal(mean=6.0, sigma=1.3), 5, 250_000))
        founded = int(prng.randint(1950, 2022))
        # Revenue ~ employees x sector productivity x maturity x noise.
        maturity = 0.7 + 0.6 * min(1.0, (2025 - founded) / 40)
        revenue = employees * INDUSTRIES[industry] * maturity * float(rng.lognormal(0, 0.25))
        entities.append(Entity(
            entity_id=f"E{len(entities):05d}", base_name=name, country=country,
            city=prng.choice(COUNTRIES[country]), industry=industry,
            domain="".join(ch for ch in name.lower() if ch.isalnum()) + {"US": ".com", "GB": ".co.uk", "DE": ".de", "IN": ".in", "SG": ".sg"}[country],
            employees=employees, founded_year=founded, revenue=round(revenue, 2),
            operating_margin=round(float(rng.normal(0.12, 0.08)), 4)))
    return entities


def _name_variant(e: Entity, prng: random.Random) -> str:
    """Render the entity name the way a messy upstream system might."""
    name = e.base_name
    roll = prng.random()
    if roll < 0.25:
        for full, short in ABBREVIATIONS.items():
            name = name.replace(full, short)
    elif roll < 0.35:
        name = name.replace(" ", " & ", 1) if " " in name else name
    if prng.random() < 0.7:
        name = f"{name} {prng.choice(SUFFIXES[e.country])}"
    style = prng.random()
    if style < 0.2:
        name = name.upper()
    elif style < 0.3:
        name = name.lower()
    if prng.random() < 0.08 and len(name) > 6:  # single-character typo
        i = prng.randrange(1, len(name) - 1)
        name = name[:i] + name[i + 1] + name[i] + name[i + 2:]
    return name


def _website(e: Entity, prng: random.Random, missing_rate: float) -> str | None:
    if prng.random() < missing_rate:
        return None
    return prng.choice([e.domain, f"www.{e.domain}", f"https://{e.domain}", f"https://www.{e.domain}/"])


def generate(n_entities: int = 1500, rows_per_source: int = 1200, seed: int = 42
             ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return (supply_chain, financial, ground_truth) frames. Deterministic for a given seed."""
    if rows_per_source < 1000:
        raise ValueError("each source must contain at least 1,000 records")
    rng, prng = np.random.default_rng(seed), random.Random(seed)
    entities = _make_entities(n_entities, rng, prng)

    # ~55% of entities appear in both sources: enough overlap to make resolution matter.
    idx = list(range(n_entities))
    prng.shuffle(idx)
    shared = idx[: int(n_entities * 0.55)]
    s1_pool = shared + idx[int(n_entities * 0.55): int(n_entities * 0.80)]
    s2_pool = shared + idx[int(n_entities * 0.80):]

    def pick(pool: list[int], n: int) -> list[Entity]:
        # Every entity in the pool once, then ~4% repeats (internal duplicates), trimmed to n.
        chosen = list(pool)
        chosen += prng.sample(pool, k=max(0, n - len(chosen))) if n > len(chosen) else []
        chosen = chosen[:n] if len(chosen) >= n else chosen
        dupes = prng.sample(chosen, k=int(n * 0.04))
        rows = (chosen + dupes)[:n]
        prng.shuffle(rows)
        return [entities[i] for i in rows]

    s1_rows, s2_rows, truth = [], [], []
    for i, e in enumerate(pick(s1_pool, rows_per_source)):
        rid = f"SC-{i + 1:06d}"
        contracts = max(1, int(e.employees / 400 * rng.lognormal(0, 0.4)))
        s1_rows.append({
            "supplier_id": rid, "company_name": _name_variant(e, prng), "country": e.country,
            "city": e.city, "industry": e.industry, "website": _website(e, prng, 0.25),
            "employee_count": max(1, int(e.employees * rng.normal(1.0, 0.05))),
            "active_contracts": contracts,
            "annual_spend_usd": round(contracts * float(rng.lognormal(11.5, 0.6)), 2),
            "onboarded_date": f"{prng.randint(2012, 2025)}-{prng.randint(1, 12):02d}-01"})
        truth.append({"source": "supply_chain", "source_record_id": rid, "entity_id": e.entity_id})

    for i, e in enumerate(pick(s2_pool, rows_per_source)):
        rid = f"FN-{i + 1:06d}"
        s2_rows.append({
            "filing_id": rid, "legal_name": _name_variant(e, prng), "country": e.country,
            "headquarters_city": e.city, "sector": e.industry, "website": _website(e, prng, 0.35),
            "ticker": (e.base_name[:4].upper().replace(" ", "")) if prng.random() < 0.3 else None,
            "fiscal_year": prng.choice([2023, 2024, 2024, 2025]),
            "founded_year": e.founded_year,
            "reported_employees": max(1, int(e.employees * rng.normal(1.0, 0.08))),
            # ~10% of filings omit revenue: those are what the model predicts.
            "annual_revenue_usd": None if prng.random() < 0.10 else e.revenue,
            "operating_margin": e.operating_margin})
        truth.append({"source": "financial", "source_record_id": rid, "entity_id": e.entity_id})

    return pd.DataFrame(s1_rows), pd.DataFrame(s2_rows), pd.DataFrame(truth)


def write(uri: str, n_entities: int = 1500, rows_per_source: int = 1200, seed: int = 42) -> None:
    """Write the three CSVs under ``uri`` (local path or s3://)."""
    from corpreg.storage import write_text
    s1, s2, truth = generate(n_entities, rows_per_source, seed)
    for name, frame in (("supply_chain.csv", s1), ("financial.csv", s2), ("ground_truth.csv", truth)):
        write_text(f"{uri.rstrip('/')}/{name}", frame.to_csv(index=False))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Generate the two synthetic corporate source datasets")
    p.add_argument("--out", required=True, help="local directory or s3://bucket/prefix")
    p.add_argument("--rows", type=int, default=1200)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    write(a.out, rows_per_source=a.rows, seed=a.seed)
    print(f"wrote supply_chain.csv, financial.csv, ground_truth.csv to {a.out}")
