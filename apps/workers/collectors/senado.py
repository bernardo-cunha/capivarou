import asyncio
import os
import re
import unicodedata
from typing import Optional

import httpx
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError
from supabase import create_client

_LIST_URL = "https://legis.senado.leg.br/dadosabertos/senador/lista/atual"
_DETAIL_URL = "https://legis.senado.leg.br/dadosabertos/senador/{id}"


class _IdentificacaoParlamentar(BaseModel):
    CodigoParlamentar: str
    NomeParlamentar: str
    NomeCompletoParlamentar: Optional[str] = None
    SexoParlamentar: Optional[str] = None
    UrlFotoParlamentar: Optional[str] = None
    EmailParlamentar: Optional[str] = None
    SiglaPartidoParlamentar: Optional[str] = None
    UfParlamentar: Optional[str] = None


class _Legislatura(BaseModel):
    DataInicio: Optional[str] = None
    DataFim: Optional[str] = None


class _Mandato(BaseModel):
    PrimeiraLegislaturaDoMandato: Optional[_Legislatura] = None
    DescricaoParticipacao: Optional[str] = None


class _Parlamentar(BaseModel):
    IdentificacaoParlamentar: _IdentificacaoParlamentar
    Mandato: Optional[_Mandato] = None


class _DadosBasicosSenador(BaseModel):
    DataNascimento: Optional[str] = None
    NaturalicidadeCidade: Optional[str] = None
    NaturalicidadeUf: Optional[str] = None


class _DetalheIdentificacao(BaseModel):
    CodigoParlamentar: str
    NomeParlamentar: Optional[str] = None
    NomeCivilCompleto: Optional[str] = None
    SexoParlamentar: Optional[str] = None
    UrlFotoParlamentar: Optional[str] = None
    EmailParlamentar: Optional[str] = None
    SiglaPartidoParlamentar: Optional[str] = None
    UfParlamentar: Optional[str] = None


def _slugify(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    return re.sub(r"\s+", "-", text.strip())


def _make_slug(nome_urna: str, partido: str, estado: str) -> str:
    return "-".join(_slugify(p) for p in [nome_urna, partido, estado] if p)


async def _get(client: httpx.AsyncClient, url: str, retries: int = 3) -> dict:
    headers = {"Accept": "application/json"}
    for attempt in range(retries):
        try:
            r = await client.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2**attempt)
    raise RuntimeError("unreachable")


def _parse_detail(raw: dict) -> dict:
    """Extract birth date from detail endpoint response."""
    try:
        parlamentar = raw.get("DetalheParlamentar", {}).get("Parlamentar", {})
        ident = parlamentar.get("IdentificacaoParlamentar", {})
        dados = parlamentar.get("DadosBasicosParlamentar", {})
        return {
            "data_nascimento": dados.get("DataNascimento"),
            "nome_civil": ident.get("NomeCivilCompleto"),
        }
    except Exception:
        return {}


async def fetch_senadores() -> tuple[list[dict], list[dict]]:
    valid: list[dict] = []
    flags: list[dict] = []

    async with httpx.AsyncClient() as client:
        data = await _get(client, _LIST_URL)
        lista = (
            data.get("ListaParlamentarEmExercicio", {})
            .get("Parlamentares", {})
            .get("Parlamentar", [])
        )

        batch_size = 20
        for i in range(0, len(lista), batch_size):
            batch = lista[i : i + batch_size]

            # Validate list-level data first
            parsed_batch: list[tuple[dict, _Parlamentar | None]] = []
            for raw_item in batch:
                try:
                    p = _Parlamentar.model_validate(raw_item)
                    parsed_batch.append((raw_item, p))
                except ValidationError as exc:
                    codigo = raw_item.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar", "?")
                    flags.append({"source_id": codigo, "error": str(exc)})

            # Fetch detail for each senator in the batch
            detail_tasks = [
                _get(client, _DETAIL_URL.format(id=p.IdentificacaoParlamentar.CodigoParlamentar))
                for _, p in parsed_batch
            ]
            detail_results = await asyncio.gather(*detail_tasks, return_exceptions=True)

            for (raw_item, p), detail_result in zip(parsed_batch, detail_results):
                ident = p.IdentificacaoParlamentar
                nome_urna = ident.NomeParlamentar
                partido = ident.SiglaPartidoParlamentar or ""
                estado = ident.UfParlamentar or ""

                extra: dict = {}
                if isinstance(detail_result, Exception):
                    flags.append({
                        "source_id": ident.CodigoParlamentar,
                        "nome": nome_urna,
                        "error": f"detail fetch: {detail_result}",
                    })
                else:
                    extra = _parse_detail(detail_result)

                valid.append(
                    {
                        "id": f"senado:{ident.CodigoParlamentar}",
                        "slug": _make_slug(nome_urna, partido, estado),
                        "nome": extra.get("nome_civil") or ident.NomeCompletoParlamentar or nome_urna,
                        "nome_urna": nome_urna,
                        "cargo": "senador",
                        "partido": partido,
                        "estado": estado,
                        "legislatura": "57",
                        "foto_url": ident.UrlFotoParlamentar,
                        "ativo": True,
                    }
                )

    return valid, flags


def save_senadores(senadores: list[dict], sb) -> int:
    if not senadores:
        return 0
    result = sb.table("politicians").upsert(senadores, on_conflict="id").execute()
    return len(result.data) if result.data else 0


if __name__ == "__main__":
    load_dotenv()
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    print("Buscando senadores do Senado Federal...")
    valid, flags = asyncio.run(fetch_senadores())
    print(f"  válidos  : {len(valid)}")
    print(f"  flags    : {len(flags)}")

    saved = save_senadores(valid, sb)
    print(f"  salvos   : {saved}")

    if flags:
        print("\nPrimeiras flags:")
        for f in flags[:5]:
            print(f"  {f}")
        if len(flags) > 5:
            print(f"  ...e mais {len(flags) - 5}")
