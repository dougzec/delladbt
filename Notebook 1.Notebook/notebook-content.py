# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5f362fb2-ab9d-46e5-9cb4-f05e6646a44c",
# META       "default_lakehouse_name": "lh_raw",
# META       "default_lakehouse_workspace_id": "f2dedd7c-0772-4684-9e4c-1e90421acbcf",
# META       "known_lakehouses": [
# META         {
# META           "id": "5f362fb2-ab9d-46e5-9cb4-f05e6646a44c"
# META         },
# META         {
# META           "id": "aeab88d4-35f4-4f7c-a91b-b80737c9fc89"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# CÉLULA 1 — Instalação e imports
# ============================================================
%pip install requests --quiet

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StringType
import requests
import json
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ingest_dellamed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 2 — Configuração
# ============================================================

# --- Credencial via Key Vault (produção) ---
# API_KEY = notebookutils.credentials.getSecret("kv_dellamed", "x-api-key")

# --- Credencial direta (desenvolvimento / POC) ---
API_KEY  = "4120df87-7eee-47e9-bf4c-71042097ad99"   # ⚠️ remover antes de commitar

API_BASE  = "https://bi.datafrete.app/api"
LAKEHOUSE = "datafrete"           # nome do Lakehouse no Fabric
SCHEMA    = "dbo"           # schema de destino
TIMEOUT   = 60                 # segundos por request
PAGE_SIZE = 2_000             # registros por página (loop de paginação)

# ------------------------------------------------------------------
# Endpoints com sort key conhecida (usada na paginação incremental)
# ------------------------------------------------------------------
SORT_KEYS = {
    "conhecimentotransporte": "conhecimentoTransporteId",
    "notafiscal":             "notaFiscalId",
    "pedido":                 "pedidoId",
    "fatura":                 "faturaId",
}

# ------------------------------------------------------------------
# Todos os endpoints a extrair
# ------------------------------------------------------------------
ENDPOINTS = [
    # ── fatos ──────────────────────────────────────────────────────
    # "notafiscal",
    # "notafiscalproduto",
    # "notafiscalevento",
    # "pedido",
    # "pedidonotafiscal",
    # "expedicao",
    # "expedicaonotafiscal",
    # "expedicaoveiculo",
    # "conhecimentotransporte",
    # "conhecimentotransportesla",
    # "conhecimentotransportenotafiscal",
    # "conhecimentotransportepessoa",
    # "conhecimentotransportefatura",
    # "fatura",
    # "agendamentoentrega",
    # "agendamentoentregadocumento",
    # "eventocorreios",
    # "eventointerno",
    # ── dimensões ──────────────────────────────────────────────────
    # "naturezaoperacao",
    # "naturezaoperacaodoc",
    # "grupooperacao",
    # "grupooperacaonatureza",
    # "transportador",
    # "transportadordocumento",
    # "plataforma",
    # "pedidoplataforma",
    # "canalnegocio",
    # "canalnegociodocumento",
    # "unidadenegocio",
    # "tipoevento",
    # "cidade",
    "uf",
    "pais",
    # "endereco",
    "usuario",
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 3 — Extração com paginação + conversão segura de tipos
# ============================================================
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql import functions as F
import json

def fetch_endpoint(endpoint: str) -> list[dict]:
    """
    Extrai todos os registros com paginação automática respeitando
    o limite de 2.000 registros por chamada da API DELLAMED.
    """
    headers = {"x-api-key": API_KEY}
    all_records = []
    offset = 0
    page = 1

    while True:
        params = {
            "limit":  PAGE_SIZE,
            "offset": offset,
        }
        if endpoint in SORT_KEYS:
            params["sort"] = f"{SORT_KEYS[endpoint]}(desc)"

        log.info(f"[{endpoint}] página {page} | offset={offset}")

        try:
            resp = requests.get(
                f"{API_BASE}/{endpoint}",
                headers=headers,
                params=params,
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            log.error(f"[{endpoint}] HTTP error: {e}")
            raise
        except requests.exceptions.Timeout:
            log.error(f"[{endpoint}] Timeout após {TIMEOUT}s")
            raise

        data = resp.json()

        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get("data") or data.get("items") or data.get("results") or []
            if not records and data:
                records = [data]
        else:
            records = []

        if not records:
            log.info(f"[{endpoint}] sem registros na página {page} — encerrando.")
            break

        all_records.extend(records)
        log.info(f"[{endpoint}] +{len(records)} | total acumulado: {len(all_records)}")

        # ✅ Condição correta: página incompleta = última página
        if len(records) < PAGE_SIZE:
            log.info(f"[{endpoint}] última página ({len(records)} < {PAGE_SIZE}) — encerrando.")
            break

        # avança offset pelo número real de registros recebidos
        offset += len(records)
        page += 1

    log.info(f"[{endpoint}] ✅ total final: {len(all_records)} registros em {page} página(s)")
    return all_records


def records_to_df(records: list[dict]):
    """
    Converte lista de dicts para Spark DataFrame de forma segura:
    - Serializa tudo para string via JSON (resolve nulls, arrays aninhados, tipos mistos)
    - Usa pandas como ponte para evitar CANNOT_DETERMINE_TYPE
    """
    # Coleta todas as chaves possíveis (union de todos os registros)
    all_keys = set()
    for r in records:
        all_keys.update(r.keys())
    all_keys = sorted(all_keys)

    # Normaliza cada registro: garante todas as chaves, serializa valores complexos
    normalized = []
    for r in records:
        row = {}
        for k in all_keys:
            v = r.get(k)
            if v is None:
                row[k] = None
            elif isinstance(v, (dict, list)):
                # objetos aninhados viram JSON string — dbt vai parsear depois
                row[k] = json.dumps(v, ensure_ascii=False)
            else:
                row[k] = str(v)
        normalized.append(row)

    # Schema explícito: todas as colunas como StringType
    schema = StructType([
        StructField(k, StringType(), nullable=True)
        for k in all_keys
    ])

    # Pandas como ponte (evita inferência de tipo do Spark)
    pdf = pd.DataFrame(normalized, columns=all_keys)
    return spark.createDataFrame(pdf, schema=schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 4 — Loop principal (usa records_to_df no lugar de createDataFrame)
# ============================================================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
log.info(f"Schema '{SCHEMA}' verificado/criado.")

ingested_at = datetime.now(timezone.utc).isoformat()
summary = []

for ep in ENDPOINTS:
    log.info(f"══ Iniciando endpoint: {ep} ══")
    status = "OK"
    row_count = 0

    try:
        records = fetch_endpoint(ep)

        if not records:
            log.warning(f"[{ep}] Nenhum registro retornado.")
            summary.append({"endpoint": ep, "rows": 0, "status": "EMPTY"})
            continue

        # ✅ substitui spark.createDataFrame(records) direto
        df = records_to_df(records)
        df = df.withColumn("_ingested_at", lit(ingested_at).cast(StringType()))

        table_path = f"{SCHEMA}.{ep}"

        (df.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(table_path))

        row_count = df.count()
        log.info(f"[{ep}] ✓ {table_path} — {row_count} linhas")

    except Exception as e:
        log.error(f"[{ep}] ✗ Falha: {e}")
        status = f"ERROR: {e}"

    summary.append({"endpoint": ep, "rows": row_count, "status": status})

log.info("══ Ingestão concluída ══")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CÉLULA 5 — Relatório de execução
# ============================================================

from pyspark.sql import Row

summary_df = spark.createDataFrame([Row(**r) for r in summary])
summary_df.show(truncate=False)

# conta erros para poder falhar o notebook explicitamente
errors = [r for r in summary if r["status"].startswith("ERROR")]
empty  = [r for r in summary if r["status"] == "EMPTY"]

print(f"\n✅ Sucesso:  {len([r for r in summary if r['status'] == 'OK'])}")
print(f"⚠️  Vazios:   {len(empty)}")
print(f"❌ Erros:    {len(errors)}")

if errors:
    raise RuntimeError(f"{len(errors)} endpoint(s) falharam: {[e['endpoint'] for e in errors]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
