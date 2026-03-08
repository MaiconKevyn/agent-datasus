"""
Funções de roteamento condicional para as arestas do grafo LangGraph.

Cada função recebe o AgentState e retorna o nome do próximo nó.
As arestas condicionais são registradas no grafo com add_conditional_edges().
"""
from __future__ import annotations

from src.agent.state import AgentState, MAX_REPAIR_ATTEMPTS


def route_after_classify(state: AgentState) -> str:
    """Após classificação: simples vai gerar SQL, complexo vai decompor."""
    if state.get("query_type") == "complex":
        return "decompose"
    return "schema_link"


def route_after_validate_syntax(state: AgentState) -> str:
    """
    Após validação sintática:
      - Sem erro → executa
      - Erro + tentativas disponíveis → repara
      - Erro + tentativas esgotadas → falha
    """
    if not state.get("last_error"):
        return "execute"
    attempts = state.get("sql_attempts", 0)
    if attempts <= MAX_REPAIR_ATTEMPTS:
        return "repair"
    return "end_error"


def route_after_execute(state: AgentState) -> str:
    """
    Após execução:
      - Sem erro → valida resultado semanticamente
      - Erro + tentativas disponíveis → repara
      - Erro + tentativas esgotadas → falha
    """
    if not state.get("last_error"):
        return "validate_result"
    attempts = state.get("sql_attempts", 0)
    if attempts <= MAX_REPAIR_ATTEMPTS:
        return "repair"
    return "end_error"


def route_after_validate_result(state: AgentState) -> str:
    """
    Após validação semântica:
      - Sem erro → gera resposta (mesmo com warnings)
      - Erro semântico (resultado vazio suspeito) + tentativas → repara
      - Erro + tentativas esgotadas → explica o que tiver (melhor que silêncio)
    """
    if not state.get("last_error"):
        return "explain"
    attempts = state.get("sql_attempts", 0)
    if attempts <= MAX_REPAIR_ATTEMPTS:
        return "repair"
    # Esgotou tentativas mas tem dados parciais: explica assim mesmo
    return "explain"


def route_after_repair(state: AgentState) -> str:
    """
    Após incrementar o contador de repair:
      - Sempre volta para gerar SQL (o limite já foi verificado antes de chegar aqui)
    """
    return "generate_sql"
