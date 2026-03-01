"""SQL majority voting node — SelECT-SQL style self-consistency (N=5)."""

import time
from typing import Dict, List, Optional, Tuple

from .llm_manager import get_llm_manager
from .state import MessagesStateTXT2SQL, ExecutionPhase, update_phase
from ..utils.logging_config import get_nodes_logger

logger = get_nodes_logger()


def _result_fingerprint(raw: str) -> str:
    """Normalise execution result for order-insensitive comparison (mirrors EX metric).

    Sorts lines so two SQLs that return the same multiset vote together,
    regardless of ORDER BY differences. This matches the Counter-based EX
    semantics used in evaluation/metrics/execution_accuracy.py.
    """
    lines = sorted(line.strip() for line in raw.strip().splitlines() if line.strip())
    return "\n".join(lines)


def _execute_safe(query_tool, sql: str) -> Optional[str]:
    """Execute a candidate SQL. Returns normalised fingerprint or None on error."""
    try:
        raw = query_tool.invoke(sql)
        if isinstance(raw, str):
            lower = raw.lower()
            error_tokens = [
                "does not exist", "syntax error", "error:", "psycopg2.errors",
                "invalid sql", "relation", "column", "não existe",
            ]
            if any(tok in lower for tok in error_tokens):
                return None
            return _result_fingerprint(raw)
        return None
    except Exception:
        return None


def vote_sql_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """SQL Majority Voting Node — SelECT-SQL style self-consistency.

    Executes every candidate from state["sql_candidates"], groups by
    execution-result fingerprint, and promotes the majority winner to
    state["generated_sql"].  Falls back to the original primary SQL when:
      - fewer than 2 candidates are available
      - all candidates fail execution
      - the sql_db_query tool is unavailable
    """
    start_time = time.time()

    candidates: Optional[List[Dict]] = state.get("sql_candidates")

    if not candidates or len(candidates) < 2:
        logger.info("vote_sql: skipping — fewer than 2 candidates", extra={
            "n_candidates": len(candidates) if candidates else 0,
        })
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, time.time() - start_time)
        return state

    try:
        llm_manager = get_llm_manager()
        tools = llm_manager.get_sql_tools()
        query_tool = next((t for t in tools if t.name == "sql_db_query"), None)

        if not query_tool:
            logger.warning("vote_sql: sql_db_query tool not found, skipping vote")
            state = update_phase(state, ExecutionPhase.SQL_GENERATION, time.time() - start_time)
            return state

        # Execute all candidates, collect (sql, confidence, fingerprint) triples
        executed: List[Tuple[str, float, Optional[str]]] = []
        for c in candidates:
            sql = c.get("sql", "")
            confidence = c.get("confidence", 0.5)
            if not sql:
                continue
            fp = _execute_safe(query_tool, sql)
            executed.append((sql, confidence, fp))

        # Keep only successful executions for voting
        successful = [(sql, conf, fp) for sql, conf, fp in executed if fp is not None]

        if not successful:
            logger.warning("vote_sql: all candidates failed execution, keeping primary SQL")
            state = update_phase(state, ExecutionPhase.SQL_GENERATION, time.time() - start_time)
            return state

        # Group by result fingerprint
        groups: Dict[str, List[Tuple[str, float]]] = {}
        for sql, conf, fp in successful:
            groups.setdefault(fp, []).append((sql, conf))

        # Pick the largest group; break ties by highest confidence within group
        majority_fp = max(groups, key=lambda k: (len(groups[k]), max(c for _, c in groups[k])))
        majority_size = len(groups[majority_fp])
        winner_sql, winner_conf = max(groups[majority_fp], key=lambda x: x[1])

        original_sql = state.get("generated_sql", "")
        changed = winner_sql != original_sql

        # Guard against majority contamination: at high temperature (0.8), a wrong answer
        # may appear more often than the correct primary (temperature=0) answer.
        # Only override the primary when the winner has a clear majority (≥3 of 5 agree).
        PRIMARY_OVERRIDE_MIN_SIZE = 3
        if changed and majority_size < PRIMARY_OVERRIDE_MIN_SIZE:
            logger.warning(
                "vote_sql: winner group too small to override primary — keeping primary SQL",
                extra={"majority_size": majority_size, "threshold": PRIMARY_OVERRIDE_MIN_SIZE},
            )
            winner_sql = original_sql
            changed = False

        logger.info("vote_sql: voting complete", extra={
            "n_candidates": len(candidates),
            "n_successful": len(successful),
            "n_groups": len(groups),
            "majority_size": majority_size,
            "winner_confidence": winner_conf,
            "changed": changed,
            "winner_sql": winner_sql[:200],
        })

        if majority_size == 1:
            logger.warning(
                "vote_sql: no consensus — all candidates produced different results; "
                "using primary SQL",
                extra={"n_groups": len(groups)},
            )

        if changed:
            state["generated_sql"] = winner_sql

        # Persist voting metadata for observability
        meta = state.get("response_metadata", {}) or {}
        meta["voting"] = {
            "n_candidates": len(candidates),
            "n_successful": len(successful),
            "n_groups": len(groups),
            "majority_size": majority_size,
            "winner_confidence": winner_conf,
            "changed": changed,
            "consensus": majority_size > 1,
        }
        state["response_metadata"] = meta

        state = update_phase(state, ExecutionPhase.SQL_GENERATION, time.time() - start_time)
        return state

    except Exception as e:
        logger.error("vote_sql: unexpected error, keeping primary SQL", extra={"error": str(e)})
        state = update_phase(state, ExecutionPhase.SQL_GENERATION, time.time() - start_time)
        return state
