#!/usr/bin/env python3
"""
Script de validação da conexão e sanidade do banco.
Execute: python scripts/validate_connection.py
"""
import sys
from pathlib import Path

# Garante que src/ está no path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.connection import get_connection
from src.db.schema import get_schema_info, build_schema_prompt


def validate():
    print("=" * 60)
    print("VALIDAÇÃO DE CONEXÃO — SIH-RD DATASUS")
    print("=" * 60)

    # 1. Conexão básica
    print("\n[1] Teste de conexão básica...")
    with get_connection() as conn:
        result = conn.execute("SELECT 42 AS answer").fetchone()
        assert result[0] == 42, "Query de sanidade falhou"
    print("    OK — conexão estabelecida com sucesso.")

    # 2. Contagem de tabelas
    print("\n[2] Verificando tabelas disponíveis...")
    with get_connection() as conn:
        tables = conn.execute(
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_schema = 'main' ORDER BY table_name"
        ).fetchall()
    print(f"    {len(tables)} tabelas encontradas:")
    for name, ttype in tables:
        print(f"      - {name} ({ttype})")

    # 3. Contagem das tabelas fato
    print("\n[3] Contagem das tabelas fato...")
    with get_connection() as conn:
        n_int = conn.execute("SELECT COUNT(*) FROM internacoes").fetchone()[0]
        n_ate = conn.execute("SELECT COUNT(*) FROM atendimentos").fetchone()[0]
    print(f"    internacoes:  {n_int:>15,} linhas")
    print(f"    atendimentos: {n_ate:>15,} linhas")

    # 4. Período dos dados
    print("\n[4] Período temporal dos dados...")
    with get_connection() as conn:
        row = conn.execute("SELECT MIN(DT_INTER), MAX(DT_INTER) FROM internacoes").fetchone()
    print(f"    DT_INTER: {row[0]} → {row[1]}")

    # 5. Query analítica de exemplo
    print("\n[5] Query analítica de exemplo (top 5 diagnósticos)...")
    with get_connection() as conn:
        rows = conn.execute("""
            SELECT i.DIAG_PRINC, c.CD_DESCRICAO, COUNT(*) AS n
            FROM internacoes i
            LEFT JOIN cid c ON i.DIAG_PRINC = c.CID
            GROUP BY 1, 2
            ORDER BY 3 DESC
            LIMIT 5
        """).fetchall()
    for r in rows:
        print(f"    {r[0]}: {r[1]} ({r[2]:,})")

    # 6. Schema prompt (primeiras linhas)
    print("\n[6] Schema prompt gerado (prévia)...")
    schema = build_schema_prompt()
    preview = "\n".join(schema.split("\n")[:20])
    print(preview)
    print("    ...")
    print(f"    Total: {len(schema)} caracteres")

    print("\n" + "=" * 60)
    print("VALIDAÇÃO CONCLUÍDA — Banco pronto para uso no pipeline Text-to-SQL")
    print("=" * 60)


if __name__ == "__main__":
    validate()
