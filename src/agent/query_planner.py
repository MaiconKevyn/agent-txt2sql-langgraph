"""Query planner node — decides single vs. multi-query strategy."""

import json
import time

from langchain_core.messages import SystemMessage, HumanMessage

from .llm_manager import get_llm_manager
from .state import (
    MessagesStateTXT2SQL,
    ExecutionPhase,
    QueryPlan,
    SubQuery,
    add_ai_message,
    update_phase,
)
from ..utils.logging_config import get_nodes_logger

logger = get_nodes_logger()

_SYSTEM_PROMPT = """\
You are a query planner for a Brazilian healthcare SQL agent (DATASUS/SIH-RS).
Decide: answer with a SINGLE SQL query or decompose into N sub-queries.

BIAS TOWARD SINGLE. Only use "multi" when a sub-query genuinely needs data from a previous
result, or when the question explicitly asks for separate outputs that cannot be expressed as
a single GROUP BY / CTE / UNION ALL.

HARD LIMITS:
- max 4 sub-queries
- no circular depends_on
- if unsure: single

WHEN MULTI GENUINELY HELPS:
- "Find the top hospital, then show its year-by-year cost trend" → sq1 finds hospital ID,
  sq2 uses that specific ID for trend (sq2 depends_on sq1).
- "For each of the 5 regions, show the top procedure separately" when each region needs an
  independent ranked query with its own LIMIT 1 that cannot collapse into one result set.
- Multi-year growth: sq1=2019 aggregation, sq2=2020 aggregation → synthesizer computes delta.

WHEN SINGLE IS ALWAYS CORRECT:
- Multi-region statistics → GROUP BY uf / estado handles this.
- Top-N from combined set → ORDER BY + LIMIT in one query.
- Any question solvable by CTE / window function (ROW_NUMBER, RANK) / UNION ALL in one pass.
- Intersection of ranked sets → CTE + WHERE id IN (SELECT …) — single query.

Respond ONLY with valid JSON (no markdown):
{
  "strategy": "single" | "multi",
  "reasoning": "<one sentence>",
  "sub_queries": [{"id": "sq1", "description": "...", "depends_on": []}]
}
For "single": exactly one item with id="sq1".
"""


def query_planner_node(state: MessagesStateTXT2SQL) -> MessagesStateTXT2SQL:
    """
    Decide whether to use a single SQL query or decompose into multiple sub-queries.

    Sets:
      state["query_plan"]   — QueryPlan with strategy + sub_queries list
      state["is_multi_query"] — True iff strategy=="multi" and len(sub_queries) > 1
    """
    start_time = time.time()

    try:
        llm_manager = get_llm_manager()
        user_query = state.get("user_query", "")
        schema_context = state.get("schema_context", "")
        selected_tables = state.get("selected_tables", [])

        human_prompt = (
            f"User question: {user_query}\n\n"
            f"Selected tables: {', '.join(selected_tables) if selected_tables else 'unknown'}\n\n"
            f"Schema context (excerpt):\n{schema_context[:1500]}\n\n"
            "Decide: single query or multi-query decomposition? Respond with JSON only."
        )

        response = llm_manager.invoke_chat([
            SystemMessage(content=_SYSTEM_PROMPT),
            HumanMessage(content=human_prompt),
        ])

        response_text = response.content.strip() if hasattr(response, "content") else str(response)

        # Strip markdown fences if present
        if "```json" in response_text:
            response_text = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            response_text = response_text.split("```")[1].split("```")[0].strip()

        plan_data = json.loads(response_text)

        strategy = plan_data.get("strategy", "single")
        reasoning = plan_data.get("reasoning", "")
        sqs_data = plan_data.get("sub_queries", [])

        sub_queries = [
            SubQuery(
                id=sq.get("id", f"sq{i + 1}"),
                description=sq.get("description", ""),
                depends_on=sq.get("depends_on", []),
            )
            for i, sq in enumerate(sqs_data)
        ]

        if not sub_queries:
            strategy = "single"
            sub_queries = [SubQuery(id="sq1", description=user_query)]

        query_plan = QueryPlan(
            strategy=strategy,
            reasoning=reasoning,
            sub_queries=sub_queries,
        )

        state["query_plan"] = query_plan
        state["is_multi_query"] = strategy == "multi" and len(sub_queries) > 1

        logger.info("Query planner decided", extra={
            "strategy": strategy,
            "n_sub_queries": len(sub_queries),
            "reasoning": reasoning[:150],
        })

        state = add_ai_message(
            state,
            f"Query plan: {strategy} ({len(sub_queries)} sub-quer{'ies' if len(sub_queries) != 1 else 'y'}). "
            f"{reasoning[:120]}"
        )

        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.REASONING, execution_time)
        return state

    except Exception as e:
        logger.warning("Query planner failed — defaulting to single", extra={"error": str(e)})
        # Graceful fallback: never block the pipeline
        state["query_plan"] = QueryPlan(
            strategy="single",
            reasoning=f"Planner error: {str(e)[:100]}",
            sub_queries=[SubQuery(id="sq1", description=state.get("user_query", ""))],
        )
        state["is_multi_query"] = False

        execution_time = time.time() - start_time
        state = update_phase(state, ExecutionPhase.REASONING, execution_time)
        return state
