"""
Collector de bens declarados do TSE (eleições 2022).

Fluxo:
  1. Baixa bem_candidato_2022.zip e consulta_cand_2022.zip (CDN do TSE)
  2. Lê consulta_cand de todos os estados → mapa SQ_CANDIDATO→{nome_urna,cargo,uf}
  3. Lê bem_candidato_2022_BRASIL.csv e junta com o mapa acima
  4. Filtra DEPUTADO FEDERAL e SENADOR
  5. Normaliza nome_urna e faz match contra politicians do Supabase
  6. Upsert na tabela assets
"""

import csv
import io
import os
import re
import unicodedata
import uuid
import zipfile
from typing import Optional

import httpx
from dotenv import load_dotenv
from supabase import create_client

_BENS_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato/bem_candidato_2022.zip"
_CAND_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand/consulta_cand_2022.zip"

_ELEICAO_ANO = 2022
_UUID_NS = uuid.UUID("12345678-1234-5678-1234-567812345678")

_CARGOS_ALVO = {"DEPUTADO FEDERAL", "SENADOR"}
# Senadores também têm suplentes listados; incluímos suplentes para ampliar o match
_CARGOS_EXTRA = {"1º SUPLENTE", "2º SUPLENTE"}

_UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]


def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r" +", " ", text).strip()


def _asset_id(sq_candidato: str, nr_ordem: str) -> str:
    return str(uuid.uuid5(_UUID_NS, f"asset:tse2022:{sq_candidato}:{nr_ordem}"))


def _parse_decimal(value: str) -> float:
    try:
        return float(value.replace(".", "").replace(",", "."))
    except (ValueError, AttributeError):
        return 0.0


def _read_zip_csv(zf: zipfile.ZipFile, filename: str) -> list[dict]:
    try:
        with zf.open(filename) as f:
            text = f.read().decode("latin-1")
            reader = csv.DictReader(io.StringIO(text), delimiter=";", quotechar='"')
            return list(reader)
    except KeyError:
        return []


def _strip_quotes(s: str) -> str:
    return s.strip().strip('"')


def download(url: str) -> bytes:
    print(f"  Baixando {url.split('/')[-1]}...", flush=True)
    with httpx.stream("GET", url, timeout=120, follow_redirects=True) as r:
        r.raise_for_status()
        chunks = []
        for chunk in r.iter_bytes(chunk_size=65536):
            chunks.append(chunk)
    return b"".join(chunks)


def build_candidatos_map(cand_zip: zipfile.ZipFile) -> dict[str, dict]:
    """
    Returns: SQ_CANDIDATO (str) → {nome_urna, cargo, uf, cpf, partido}
    Reads all state CSVs, keeps only DEPUTADO FEDERAL / SENADOR / suplentes.
    """
    candidatos: dict[str, dict] = {}
    for uf in _UFS:
        rows = _read_zip_csv(cand_zip, f"consulta_cand_2022_{uf}.csv")
        for row in rows:
            cargo = _strip_quotes(row.get("DS_CARGO", ""))
            if cargo not in _CARGOS_ALVO and cargo not in _CARGOS_EXTRA:
                continue
            sq = _strip_quotes(row.get("SQ_CANDIDATO", ""))
            if not sq:
                continue
            candidatos[sq] = {
                "nome_urna": _strip_quotes(row.get("NM_URNA_CANDIDATO", "")),
                "nome_urna_norm": _normalize(_strip_quotes(row.get("NM_URNA_CANDIDATO", ""))),
                "cargo": cargo,
                "uf": _strip_quotes(row.get("SG_UF", "")),
                "cpf": _strip_quotes(row.get("NR_CPF_CANDIDATO", "")),
                "partido": _strip_quotes(row.get("SG_PARTIDO", "")),
            }
    return candidatos


def match_politicians(
    candidatos: dict[str, dict],
    sb,
) -> dict[str, str]:
    """
    Returns: nome_urna_norm → politician_id (from Supabase)
    Fetches all politicians and matches by normalized nome_urna.
    """
    rows = sb.table("politicians").select("id,nome_urna").execute()
    pol_map: dict[str, str] = {}
    for r in rows.data:
        norm = _normalize(r.get("nome_urna") or "")
        if norm:
            pol_map[norm] = r["id"]
    return pol_map


def process_bens(
    bens_zip: zipfile.ZipFile,
    candidatos: dict[str, dict],
    pol_map: dict[str, str],
) -> tuple[list[dict], int]:
    """
    Parse bem_candidato_2022_BRASIL.csv, join with candidatos, match politicians.
    Returns (assets_list, unmatched_count).
    """
    rows = _read_zip_csv(bens_zip, "bem_candidato_2022_BRASIL.csv")
    assets: list[dict] = []
    unmatched = 0

    for row in rows:
        sq = _strip_quotes(row.get("SQ_CANDIDATO", ""))
        cand = candidatos.get(sq)
        if not cand:
            continue  # not a deputado federal / senador

        pol_id = pol_map.get(cand["nome_urna_norm"])
        if not pol_id:
            unmatched += 1
            continue

        nr_ordem = _strip_quotes(row.get("NR_ORDEM_BEM_CANDIDATO", "0"))
        valor_str = _strip_quotes(row.get("VR_BEM_CANDIDATO", "0"))

        assets.append(
            {
                "id": _asset_id(sq, nr_ordem),
                "politician_id": pol_id,
                "eleicao_ano": _ELEICAO_ANO,
                "item_descricao": (
                    _strip_quotes(row.get("DS_TIPO_BEM_CANDIDATO", ""))
                    + " — "
                    + _strip_quotes(row.get("DS_BEM_CANDIDATO", ""))
                )[:500],
                "valor": _parse_decimal(valor_str),
            }
        )

    return assets, unmatched


def save_assets(assets: list[dict], sb) -> int:
    if not assets:
        return 0
    chunk_size = 500
    total = 0
    for i in range(0, len(assets), chunk_size):
        result = sb.table("assets").upsert(assets[i : i + chunk_size], on_conflict="id").execute()
        total += len(result.data) if result.data else 0
    return total


def run(sb) -> None:
    print("=== TSE: bens declarados 2022 ===")

    bens_bytes = download(_BENS_URL)
    cand_bytes = download(_CAND_URL)

    with zipfile.ZipFile(io.BytesIO(cand_bytes)) as cand_zip:
        print("  Lendo candidatos por estado...", flush=True)
        candidatos = build_candidatos_map(cand_zip)
        print(f"  Candidatos federais/senadores: {len(candidatos)}")

    pol_map = match_politicians(candidatos, sb)
    print(f"  Políticos no banco com nome correspondente: {len(pol_map)}")

    with zipfile.ZipFile(io.BytesIO(bens_bytes)) as bens_zip:
        print("  Processando bens...", flush=True)
        assets, unmatched = process_bens(bens_zip, candidatos, pol_map)

    print(f"  Bens encontrados: {len(assets)} ({unmatched} sem match)")

    saved = save_assets(assets, sb)
    print(f"  Bens salvos: {saved}")


if __name__ == "__main__":
    load_dotenv()
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
    run(sb)
    print("\nConcluído.")
