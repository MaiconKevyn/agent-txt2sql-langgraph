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
You are a query decomposition expert for a Brazilian healthcare database (DATASUS/SIH-RS).
Your task: decide if a natural-language question can be answered by a SINGLE SQL query or
needs to be DECOMPOSED into multiple independent (or sequentially dependent) sub-queries.

Rules:
1. Default to "single" unless the question CLEARLY requires multiple SQL operations.
2. Use "multi" ONLY when:
   - The question asks about 2+ distinct geographic regions separately
     (e.g., "quantas mortes no RS E no MA?" — one query per state).
   - Step 2 mathematically depends on the specific numeric output of Step 1
     (e.g., "find the top hospital, then count its patients by year").
3. Do NOT use "multi" just because the query is complex — GROUP BY, CTEs,
   window functions, and UNION handle complexity within a single query.
4. For "multi", list each sub-query with a unique id (sq1, sq2, …) and its description.
5. Sub-queries that depend on prior results must list the dependency id in "depends_on".

Respond ONLY with valid JSON — no markdown, no extra text:
{
  "strategy": "single" | "multi",
  "reasoning": "<one sentence>",
  "sub_queries": [
    {"id": "sq1", "description": "<what this query fetches>", "depends_on": []}
  ]
}
For "single", sub_queries must contain exactly ONE item with id="sq1".
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
