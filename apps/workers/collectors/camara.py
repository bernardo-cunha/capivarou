import asyncio
import os
import re
import unicodedata
from typing import Optional

import httpx
from dotenv import load_dotenv
from pydantic import BaseModel, ValidationError
from supabase import create_client


class _UltimoStatus(BaseModel):
    nome: str
    nomeEleitoral: Optional[str] = None
    siglaPartido: Optional[str] = None
    siglaUf: Optional[str] = None
    urlFoto: Optional[str] = None
    situacao: Optional[str] = None


class _DeputadoDetalhe(BaseModel):
    id: int
    nomeCivil: Optional[str] = None
    ultimoStatus: _UltimoStatus


def _slugify(text: str) -> str:
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    return re.sub(r"\s+", "-", text.strip())


def _make_slug(nome_urna: str, partido: str, estado: str) -> str:
    return "-".join(_slugify(p) for p in [nome_urna, partido, estado] if p)


async def _get(client: httpx.AsyncClient, url: str, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            r = await client.get(url, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == retries - 1:
                raise
            await asyncio.sleep(2**attempt)
    raise RuntimeError("unreachable")


async def fetch_deputados() -> tuple[list[dict], list[dict]]:
    valid: list[dict] = []
    flags: list[dict] = []

    async with httpx.AsyncClient() as client:
        data = await _get(
            client,
            "https://dadosabertos.camara.leg.br/api/v2/deputados"
            "?itens=600&ordem=ASC&ordenarPor=nome",
        )
        lista = data.get("dados", [])

        batch_size = 20
        for i in range(0, len(lista), batch_size):
            batch = lista[i : i + batch_size]
            tasks = [
                _get(client, f"https://dadosabertos.camara.leg.br/api/v2/deputados/{d['id']}")
                for d in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for item, result in zip(batch, results):
                if isinstance(result, Exception):
                    flags.append({"source_id": str(item["id"]), "nome": item.get("nome"), "error": str(result)})
                    continue

                raw = result.get("dados", {})
                try:
                    dep = _DeputadoDetalhe.model_validate(raw)
                except ValidationError as exc:
                    flags.append({"source_id": str(raw.get("id")), "nome": raw.get("nomeCivil"), "error": str(exc)})
                    continue

                st = dep.ultimoStatus
                nome_urna = st.nomeEleitoral or st.nome
                partido = st.siglaPartido or ""
                estado = st.siglaUf or ""

                valid.append(
                    {
                        "id": f"camara:{dep.id}",
                        "slug": _make_slug(nome_urna, partido, estado),
                        "nome": dep.nomeCivil or nome_urna,
                        "nome_urna": nome_urna,
                        "cargo": "deputado",
                        "partido": partido,
                        "estado": estado,
                        "legislatura": "57",
                        "foto_url": st.urlFoto,
                        "ativo": st.situacao == "Exercício" if st.situacao else True,
                    }
                )

    return valid, flags


def save_deputados(deputados: list[dict], sb) -> int:
    if not deputados:
        return 0
    result = sb.table("politicians").upsert(deputados, on_conflict="id").execute()
    return len(result.data) if result.data else 0


if __name__ == "__main__":
    load_dotenv()
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    print("Buscando deputados da Câmara...")
    valid, flags = asyncio.run(fetch_deputados())
    print(f"  válidos  : {len(valid)}")
    print(f"  flags    : {len(flags)}")

    saved = save_deputados(valid, sb)
    print(f"  salvos   : {saved}")

    if flags:
        print("\nPrimeiras flags:")
        for f in flags[:5]:
            print(f"  {f}")
        if len(flags) > 5:
            print(f"  ...e mais {len(flags) - 5}")
