"""
Collector de votações nominais e gastos CEAP da Câmara.

Votações: a API não tem endpoint per-deputado; o caminho real é
  GET /votacoes          → lista de votações (paginado, filtrado por ano)
  GET /votacoes/{id}/votos → todos os votos daquela votação

O módulo faz o fetch uma vez, constrói um índice dep_id→votos e expõe
fetch_votos_deputado() com a assinatura per-deputado pedida.

Despesas (CEAP): endpoint real é per-deputado com paginação por ano.
"""

import asyncio
import os
import uuid
from typing import Optional

import httpx
from dotenv import load_dotenv
from supabase import create_client

# ──────────────────────────────────────────────────────────────
# Shared votações cache (built once, used by all dep queries)
# ──────────────────────────────────────────────────────────────
_votos_index: Optional[dict[str, list[dict]]] = None  # "camara:{id}" → [vote, ...]
_votos_lock = asyncio.Lock()

_UUID_NS = uuid.UUID("12345678-1234-5678-1234-567812345678")

_CONCURRENCY = 20  # simultaneous HTTP requests


def _vote_id(politician_id: str, votacao_id: str) -> str:
    return str(uuid.uuid5(_UUID_NS, f"vote:{politician_id}:{votacao_id}"))


def _expense_id(politician_id: str, cod_documento: str) -> str:
    return str(uuid.uuid5(_UUID_NS, f"expense:{politician_id}:{cod_documento}"))


async def _get(client: httpx.AsyncClient, url: str, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            r = await client.get(url, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)
    raise RuntimeError("unreachable")


async def _paginate(client: httpx.AsyncClient, base_url: str) -> list[dict]:
    """Fetch all pages of a paginated endpoint (uses links.next)."""
    results = []
    url = base_url
    while url:
        data = await _get(client, url)
        results.extend(data.get("dados", []))
        links = {l["rel"]: l["href"] for l in data.get("links", [])}
        url = links.get("next")
    return results


# ──────────────────────────────────────────────────────────────
# Build votações index
# ──────────────────────────────────────────────────────────────

async def _build_votos_index(client: httpx.AsyncClient, anos: list[int]) -> None:
    global _votos_index
    async with _votos_lock:
        if _votos_index is not None:
            return

        print("  Buscando lista de votações...", flush=True)
        # dataFim não é suportado pela API — usamos dataInicio do primeiro ano
        # e filtramos por data no lado do cliente
        ano_inicio = min(anos)
        ano_fim = max(anos)
        url = (
            f"https://dadosabertos.camara.leg.br/api/v2/votacoes"
            f"?dataInicio={ano_inicio}-01-01&itens=100"
        )
        all_votacoes = [
            v for v in await _paginate(client, url)
            if (v.get("data") or "")[:4].isdigit()
            and ano_inicio <= int((v.get("data") or "9999")[:4]) <= ano_fim
        ]

        print(f"  {len(all_votacoes)} votações encontradas, buscando votos...", flush=True)

        sem = asyncio.Semaphore(_CONCURRENCY)
        index: dict[str, list[dict]] = {}
        flags: list[str] = []

        async def fetch_votos(v: dict) -> None:
            vid = v["id"]
            async with sem:
                try:
                    data = await _get(
                        client,
                        f"https://dadosabertos.camara.leg.br/api/v2/votacoes/{vid}/votos",
                    )
                    for vote in data.get("dados", []):
                        dep = vote.get("deputado_", {})
                        dep_id = dep.get("id")
                        if not dep_id:
                            continue
                        pol_id = f"camara:{dep_id}"
                        index.setdefault(pol_id, []).append(
                            {
                                "politician_id": pol_id,
                                "proposicao_id": v.get("proposicaoObjeto") or vid,
                                "proposicao_desc": (v.get("descricao") or "")[:500],
                                "voto": vote.get("tipoVoto", ""),
                                "data": (vote.get("dataRegistroVoto") or v.get("data") or "")[:10],
                            }
                        )
                except Exception as exc:
                    flags.append(f"{vid}: {exc}")

        await asyncio.gather(*[fetch_votos(v) for v in all_votacoes])

        if flags:
            print(f"  {len(flags)} votações com erro (primeiras 3: {flags[:3]})")

        _votos_index = index
        total = sum(len(vs) for vs in index.values())
        print(f"  Índice construído: {total} votos para {len(index)} deputados")


async def fetch_votos_deputado(
    dep_id: str,
    anos: list[int] = [2023, 2024, 2025],
    client: Optional[httpx.AsyncClient] = None,
) -> list[dict]:
    """
    Retorna votos nominais de um deputado.
    dep_id é o ID numérico da Câmara (ex: 204379).
    Usa cache interno compartilhado — o fetch real ocorre só na primeira chamada.
    """
    own_client = client is None
    if own_client:
        client = httpx.AsyncClient()
    try:
        await _build_votos_index(client, anos)
    finally:
        if own_client:
            await client.aclose()

    pol_id = f"camara:{dep_id}"
    return (_votos_index or {}).get(pol_id, [])


# ──────────────────────────────────────────────────────────────
# Despesas CEAP
# ──────────────────────────────────────────────────────────────

async def fetch_despesas_deputado(
    dep_id: str,
    anos: list[int] = [2023, 2024, 2025],
    client: Optional[httpx.AsyncClient] = None,
) -> list[dict]:
    """Busca gastos CEAP de um deputado para os anos indicados (paginado)."""
    pol_id = f"camara:{dep_id}"
    results: list[dict] = []

    own_client = client is None
    if own_client:
        client = httpx.AsyncClient()

    try:
        for ano in anos:
            try:
                url = (
                    f"https://dadosabertos.camara.leg.br/api/v2/deputados/{dep_id}"
                    f"/despesas?ano={ano}&itens=100"
                )
                despesas = await _paginate(client, url)
                for d in despesas:
                    results.append(
                        {
                            "id": _expense_id(pol_id, str(d.get("codDocumento", ""))),
                            "politician_id": pol_id,
                            "ano": d.get("ano") or ano,
                            "mes": d.get("mes"),
                            "categoria": (d.get("tipoDespesa") or "")[:200],
                            "valor": d.get("valorLiquido") or d.get("valorDocumento") or 0,
                            "fornecedor": (d.get("nomeFornecedor") or "")[:200],
                            "descricao": (d.get("tipoDocumento") or "")[:200],
                        }
                    )
            except Exception as exc:
                print(f"    despesas dep {dep_id} ano {ano}: {exc}")
    finally:
        if own_client:
            await client.aclose()

    return results


# ──────────────────────────────────────────────────────────────
# Save
# ──────────────────────────────────────────────────────────────

def save_votos(votos: list[dict], sb) -> int:
    if not votos:
        return 0
    seen: set[str] = set()
    unique = []
    for v in votos:
        v["id"] = _vote_id(v["politician_id"], v["proposicao_id"])
        if v["id"] not in seen:
            seen.add(v["id"])
            unique.append(v)
    result = sb.table("votes").upsert(unique, on_conflict="id").execute()
    return len(result.data) if result.data else 0


def save_despesas(despesas: list[dict], sb) -> int:
    if not despesas:
        return 0
    # deduplicate by id within the batch to avoid ON CONFLICT self-conflicts
    seen: set[str] = set()
    unique = [d for d in despesas if d["id"] not in seen and not seen.add(d["id"])]  # type: ignore[func-returns-value]
    result = sb.table("expenses").upsert(unique, on_conflict="id").execute()
    return len(result.data) if result.data else 0


# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────

ANOS = [2023, 2024, 2025]
BATCH_SIZE = 50
SAVE_CHUNK = 500


async def _run(sb) -> None:
    # Load all deputados from DB
    rows = sb.table("politicians").select("id").eq("cargo", "deputado").execute()
    dep_ids = [r["id"].replace("camara:", "") for r in rows.data]
    print(f"Deputados no banco: {len(dep_ids)}")

    total_votos = 0
    total_despesas = 0

    async with httpx.AsyncClient() as client:
        # ── Votes (single pass via votações API) ──────────────────
        print("\nColetando votações nominais...")
        await _build_votos_index(client, ANOS)

        all_votos: list[dict] = []
        for dep_id in dep_ids:
            pol_id = f"camara:{dep_id}"
            all_votos.extend((_votos_index or {}).get(pol_id, []))

        print(f"Total votos coletados: {len(all_votos)}")
        for i in range(0, len(all_votos), SAVE_CHUNK):
            saved = save_votos(all_votos[i : i + SAVE_CHUNK], sb)
            total_votos += saved
        print(f"Votos salvos: {total_votos}")

        # ── Expenses (per-deputy, batched) ────────────────────────
        print("\nColetando despesas CEAP...")
        sem = asyncio.Semaphore(_CONCURRENCY)

        async def _get_despesas(dep_id: str) -> list[dict]:
            async with sem:
                return await fetch_despesas_deputado(dep_id, ANOS, client)

        all_despesas: list[dict] = []
        for i in range(0, len(dep_ids), BATCH_SIZE):
            batch = dep_ids[i : i + BATCH_SIZE]
            results = await asyncio.gather(*[_get_despesas(d) for d in batch], return_exceptions=True)
            for dep_id, result in zip(batch, results):
                if isinstance(result, Exception):
                    print(f"  Erro despesas dep {dep_id}: {result}")
                else:
                    all_despesas.extend(result)
            print(f"  Progresso: {min(i + BATCH_SIZE, len(dep_ids))}/{len(dep_ids)} deputados")

        print(f"Total despesas coletadas: {len(all_despesas)}")
        for i in range(0, len(all_despesas), SAVE_CHUNK):
            saved = save_despesas(all_despesas[i : i + SAVE_CHUNK], sb)
            total_despesas += saved
        print(f"Despesas salvas: {total_despesas}")


if __name__ == "__main__":
    load_dotenv()
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    print("=== Câmara: votações + despesas CEAP ===")
    asyncio.run(_run(sb))
    print("\nConcluído.")
