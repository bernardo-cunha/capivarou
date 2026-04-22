"""
Scoring engine para o Capivarou.

Três dimensões (0–10 cada):
  produtividade  → presença em sessões + uso do CEAP vs teto
  entregas       → taxa de aprovação de projetos de lei vs média do cargo
  ficha          → ausência de processos ativos + variação patrimonial

Dados dos parlamentares já estão no Supabase; este engine apenas agrega e pontua.

Todas as funções calc_* aceitam (politician_id, sb) para uso standalone.
run_scoring_all() pré-carrega os dados em bulk para evitar N×M queries.
"""

import os
import uuid
from statistics import mean
from typing import Any, Optional

from dotenv import load_dotenv
from supabase import create_client

# ──────────────────────────────────────────────────────────────
# Constantes
# ──────────────────────────────────────────────────────────────

SCORE_VERSION = 1

CEAP_TETO: dict[str, float] = {
    "deputado": 45_600.0,
    "senador":  44_600.0,
}

_UUID_NS = uuid.UUID("abcdef12-1234-5678-1234-567812345678")

# Número máximo de linhas por página nas queries paginadas
_PAGE = 1_000


def _score_id(politician_id: str, version: int) -> str:
    return str(uuid.uuid5(_UUID_NS, f"score:{politician_id}:v{version}"))


def _clamp(v: float, lo: float = 0.0, hi: float = 10.0) -> float:
    return max(lo, min(hi, v))


def _safe_mean(values: list[float]) -> float:
    return mean(values) if values else 5.0


# ──────────────────────────────────────────────────────────────
# Bulk data loaders (paginados, retornam índices em memória)
# ──────────────────────────────────────────────────────────────

def _paginate_table(sb, table: str, select: str = "*") -> list[dict]:
    """Carrega toda a tabela em páginas de _PAGE linhas."""
    rows: list[dict] = []
    offset = 0
    while True:
        r = (
            sb.table(table)
            .select(select)
            .range(offset, offset + _PAGE - 1)
            .execute()
        )
        batch = r.data or []
        rows.extend(batch)
        if len(batch) < _PAGE:
            break
        offset += _PAGE
    return rows


def _load_expenses(sb) -> dict[str, dict[tuple[int, int], float]]:
    """{ politician_id: { (ano, mes): total_valor } }"""
    print("  Carregando despesas CEAP...", flush=True)
    rows = _paginate_table(sb, "expenses", "politician_id,ano,mes,valor")
    index: dict[str, dict[tuple[int, int], float]] = {}
    for r in rows:
        pid = r["politician_id"]
        key = (r.get("ano") or 0, r.get("mes") or 0)
        index.setdefault(pid, {})[key] = (
            index.get(pid, {}).get(key, 0.0) + (r.get("valor") or 0.0)
        )
    print(f"  {len(rows)} despesas, {len(index)} parlamentares", flush=True)
    return index


def _load_attendances(sb) -> dict[str, float]:
    """{ politician_id: media_percentual }"""
    rows = _paginate_table(sb, "attendances", "politician_id,percentual")
    index: dict[str, list[float]] = {}
    for r in rows:
        index.setdefault(r["politician_id"], []).append(r.get("percentual") or 0.0)
    return {pid: mean(vals) for pid, vals in index.items()}


def _load_bills(sb) -> dict[str, dict[str, int]]:
    """{ politician_id: { 'total': n, 'aprovados': n } }"""
    rows = _paginate_table(sb, "bills", "politician_id,status")
    index: dict[str, dict[str, int]] = {}
    for r in rows:
        pid = r["politician_id"]
        entry = index.setdefault(pid, {"total": 0, "aprovados": 0})
        entry["total"] += 1
        status = (r.get("status") or "").lower()
        if "aprovad" in status or "transform" in status:
            entry["aprovados"] += 1
    return index


def _load_lawsuits(sb) -> dict[str, dict[str, int]]:
    """{ politician_id: { 'ativos': n, 'total': n } }"""
    rows = _paginate_table(sb, "lawsuits", "politician_id,status")
    index: dict[str, dict[str, int]] = {}
    for r in rows:
        pid = r["politician_id"]
        entry = index.setdefault(pid, {"ativos": 0, "total": 0})
        entry["total"] += 1
        if (r.get("status") or "").lower() in ("ativo", "em andamento", "tramitando"):
            entry["ativos"] += 1
    return index


def _load_assets(sb) -> dict[str, dict[int, float]]:
    """{ politician_id: { eleicao_ano: total_valor } }"""
    rows = _paginate_table(sb, "assets", "politician_id,eleicao_ano,valor")
    index: dict[str, dict[int, float]] = {}
    for r in rows:
        pid = r["politician_id"]
        ano = r.get("eleicao_ano") or 0
        index.setdefault(pid, {})[ano] = (
            index.get(pid, {}).get(ano, 0.0) + (r.get("valor") or 0.0)
        )
    return index


def _load_politicians(sb) -> list[dict]:
    return _paginate_table(sb, "politicians", "id,cargo")


# ──────────────────────────────────────────────────────────────
# Score calculators
# ──────────────────────────────────────────────────────────────

def calc_score_produtividade(
    politician_id: str,
    sb,
    *,
    cargo: str = "deputado",
    expenses_index: Optional[dict] = None,
    attendance_index: Optional[dict] = None,
) -> dict[str, Any]:
    """
    Componentes:
      • presença em sessões (percentual médio vs 100%)
      • uso do CEAP (uso médio mensal vs teto do cargo — menor uso = maior score)

    Quando um componente não tem dado, é omitido da média.
    Retorna score 0–10 e detalhes dos componentes.
    """
    teto = CEAP_TETO.get(cargo, CEAP_TETO["deputado"])
    components: list[float] = []
    detail: dict[str, Any] = {"cargo": cargo, "ceap_teto": teto}

    # — Presença —
    if attendance_index is None:
        rows = sb.table("attendances").select("percentual").eq("politician_id", politician_id).execute()
        att_vals = [r.get("percentual") or 0.0 for r in (rows.data or [])]
    else:
        pct = attendance_index.get(politician_id)
        att_vals = [pct] if pct is not None else []

    if att_vals:
        avg_pct = mean(att_vals)
        att_score = _clamp(avg_pct / 10.0)  # 100% → 10, 0% → 0
        components.append(att_score)
        detail["attendance_pct"] = round(avg_pct, 1)
        detail["attendance_score"] = round(att_score, 2)

    # — CEAP —
    if expenses_index is None:
        rows = sb.table("expenses").select("ano,mes,valor").eq("politician_id", politician_id).execute()
        monthly: dict[tuple[int, int], float] = {}
        for r in (rows.data or []):
            k = (r.get("ano") or 0, r.get("mes") or 0)
            monthly[k] = monthly.get(k, 0.0) + (r.get("valor") or 0.0)
    else:
        monthly = expenses_index.get(politician_id, {})

    if monthly:
        avg_monthly = mean(monthly.values())
        usage_ratio = avg_monthly / teto
        ceap_score = _clamp((1.0 - usage_ratio) * 10.0)
        components.append(ceap_score)
        detail["ceap_avg_monthly"] = round(avg_monthly, 2)
        detail["ceap_usage_pct"] = round(usage_ratio * 100, 1)
        detail["ceap_score"] = round(ceap_score, 2)

    score = round(_safe_mean(components), 2)
    return {"score": score, **detail}


def calc_score_entregas(
    politician_id: str,
    sb,
    *,
    cargo: str = "deputado",
    bills_index: Optional[dict] = None,
    bench_taxa: float = 0.10,
) -> dict[str, Any]:
    """
    Taxa de aprovação dos projetos apresentados vs benchmark do cargo.
    Sem dados → score neutro 5.0.
    """
    if bills_index is None:
        rows = sb.table("bills").select("status").eq("politician_id", politician_id).execute()
        total = len(rows.data or [])
        aprovados = sum(
            1 for r in (rows.data or [])
            if "aprovad" in (r.get("status") or "").lower()
            or "transform" in (r.get("status") or "").lower()
        )
    else:
        entry = bills_index.get(politician_id, {})
        total = entry.get("total", 0)
        aprovados = entry.get("aprovados", 0)

    if total == 0:
        return {"score": 5.0, "total_projetos": 0, "aprovados": 0, "taxa": None}

    taxa = aprovados / total
    # Escala: 0 → 0, benchmark → 5, 2× benchmark → 10
    upper = max(bench_taxa * 2.0, 1e-6)
    score = _clamp(taxa / upper * 10.0)

    return {
        "score": round(score, 2),
        "total_projetos": total,
        "aprovados": aprovados,
        "taxa": round(taxa, 4),
    }


def calc_score_ficha(
    politician_id: str,
    sb,
    *,
    cargo: str = "deputado",
    lawsuits_index: Optional[dict] = None,
    assets_index: Optional[dict] = None,
) -> dict[str, Any]:
    """
    Componentes:
      • processos ativos: 0 processos → 10; cada processo ativo subtrai 2 pontos
      • variação patrimonial entre eleições: variação > 500% é penalizada

    Sem dados em qualquer componente → pontuação máxima (benefício da dúvida).
    """
    components: list[float] = []
    detail: dict[str, Any] = {}

    # — Processos —
    if lawsuits_index is None:
        rows = sb.table("lawsuits").select("status").eq("politician_id", politician_id).execute()
        entry_l: dict[str, int] = {"ativos": 0, "total": 0}
        for r in (rows.data or []):
            entry_l["total"] += 1
            if (r.get("status") or "").lower() in ("ativo", "em andamento", "tramitando"):
                entry_l["ativos"] += 1
    else:
        entry_l = lawsuits_index.get(politician_id, {"ativos": 0, "total": 0})

    lawsuit_score = _clamp(10.0 - entry_l["ativos"] * 2.0)
    components.append(lawsuit_score)
    detail["lawsuits_ativos"] = entry_l["ativos"]
    detail["lawsuit_score"] = round(lawsuit_score, 2)

    # — Patrimônio —
    if assets_index is None:
        rows = sb.table("assets").select("eleicao_ano,valor").eq("politician_id", politician_id).execute()
        by_year: dict[int, float] = {}
        for r in (rows.data or []):
            yr = r.get("eleicao_ano") or 0
            by_year[yr] = by_year.get(yr, 0.0) + (r.get("valor") or 0.0)
    else:
        by_year = assets_index.get(politician_id, {})

    if len(by_year) >= 2:
        years = sorted(by_year)
        oldest = by_year[years[0]]
        newest = by_year[years[-1]]
        if oldest > 0:
            variation = (newest - oldest) / oldest
        else:
            variation = newest / 1_000.0  # baseline proxy when starts at zero

        # Penalidade progressiva: variação > 5× → score zero
        asset_score = _clamp(10.0 - variation * 2.0)
        components.append(asset_score)
        detail["patrimonio_base"] = round(oldest, 2)
        detail["patrimonio_atual"] = round(newest, 2)
        detail["variacao_pct"] = round(variation * 100, 1)
        detail["asset_score"] = round(asset_score, 2)
    elif by_year:
        detail["patrimonio_atual"] = round(sum(by_year.values()), 2)

    score = round(_safe_mean(components), 2)
    return {"score": score, **detail}


# ──────────────────────────────────────────────────────────────
# run_scoring_all
# ──────────────────────────────────────────────────────────────

def run_scoring_all(sb) -> dict[str, Any]:
    """
    Calcula os 3 scores para todos os políticos do banco.
    Pré-carrega todos os dados em memória para evitar N×M queries.
    Faz upsert em batch na tabela scores com score_version=SCORE_VERSION.
    Retorna um resumo com contagens e médias.
    """
    print("Carregando dados...", flush=True)
    politicians   = _load_politicians(sb)
    expenses_idx  = _load_expenses(sb)
    attendance_idx = _load_attendances(sb)
    bills_idx     = _load_bills(sb)
    lawsuits_idx  = _load_lawsuits(sb)
    assets_idx    = _load_assets(sb)

    pol_cargo = {p["id"]: (p.get("cargo") or "deputado") for p in politicians}
    print(f"\n{len(politicians)} políticos a pontuar...", flush=True)

    # — Primeira passagem: calcular scores individuais —
    raw_scores: list[dict] = []  # todos os resultados individuais
    cargo_groups: dict[str, list[dict]] = {}  # cargo → lista de score_dicts

    for i, pol in enumerate(politicians, 1):
        pid  = pol["id"]
        cargo = pol.get("cargo") or "deputado"

        r_prod  = calc_score_produtividade(
            pid, sb, cargo=cargo,
            expenses_index=expenses_idx,
            attendance_index=attendance_idx,
        )
        r_entr  = calc_score_entregas(
            pid, sb, cargo=cargo,
            bills_index=bills_idx,
        )
        r_ficha = calc_score_ficha(
            pid, sb, cargo=cargo,
            lawsuits_index=lawsuits_idx,
            assets_index=assets_idx,
        )

        raw_scores.append({
            "pid": pid,
            "cargo": cargo,
            "score_produtividade": r_prod["score"],
            "score_entregas": r_entr["score"],
            "score_ficha": r_ficha["score"],
        })
        cargo_groups.setdefault(cargo, []).append(raw_scores[-1])

        if i % 50 == 0 or i == len(politicians):
            print(f"  {i}/{len(politicians)} calculados", flush=True)

    # — Benchmarks por cargo (média simples dos scores) —
    benchmarks: dict[str, dict[str, float]] = {}
    for cargo, group in cargo_groups.items():
        benchmarks[cargo] = {
            "bench_produtividade": round(mean(s["score_produtividade"] for s in group), 2),
            "bench_entregas":      round(mean(s["score_entregas"]      for s in group), 2),
            "bench_ficha":         round(mean(s["score_ficha"]         for s in group), 2),
        }

    # — Montar registros e upsert em lote —
    BATCH = 200
    total_saved = 0
    records = []
    for rs in raw_scores:
        cargo = rs["cargo"]
        bench = benchmarks.get(cargo, {"bench_produtividade": 5.0, "bench_entregas": 5.0, "bench_ficha": 5.0})
        records.append(
            {
                "id": _score_id(rs["pid"], SCORE_VERSION),
                "politician_id": rs["pid"],
                "score_produtividade": rs["score_produtividade"],
                "score_entregas":      rs["score_entregas"],
                "score_ficha":         rs["score_ficha"],
                "bench_produtividade": bench["bench_produtividade"],
                "bench_entregas":      bench["bench_entregas"],
                "bench_ficha":         bench["bench_ficha"],
                "score_version":       SCORE_VERSION,
                "updated_at":          "now()",
            }
        )

    print(f"\nSalvando {len(records)} scores...", flush=True)
    for i in range(0, len(records), BATCH):
        chunk = records[i : i + BATCH]
        sb.table("scores").upsert(chunk, on_conflict="id").execute()
        total_saved += len(chunk)

    # — Resumo —
    summary: dict[str, Any] = {
        "total_scored": total_saved,
        "benchmarks": benchmarks,
    }
    for cargo, bench in benchmarks.items():
        summary[cargo] = bench
    return summary


# ──────────────────────────────────────────────────────────────
# __main__
# ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    load_dotenv()
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    print("=== Capivarou Scoring Engine ===\n")
    summary = run_scoring_all(sb)

    print("\n─── Resumo ───")
    print(f"Scores calculados e salvos: {summary['total_scored']}")
    print(f"Score version: {SCORE_VERSION}")
    for cargo, bench in summary.get("benchmarks", {}).items():
        print(f"\n{cargo.capitalize()}:")
        for k, v in bench.items():
            print(f"  {k}: {v}")
