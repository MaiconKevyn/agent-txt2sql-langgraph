"""
Embedding-based table selector (Stage 2 of 3-stage cascade).

Stage 1: regex heuristic in _heuristic_table_selection (instant, conf ≥ 0.85)
Stage 2: semantic similarity — THIS MODULE (< 10 ms after first call, conf ≥ 0.50)
Stage 3: LLM fallback for genuinely ambiguous queries

Uses sentence-transformers `all-MiniLM-L6-v2` (22 MB, CPU-friendly).
Embeddings are computed once at startup and cached in-process.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Table corpus — one document per table, combining name + title + purpose +
# top use_cases for maximum semantic coverage.
# Keep in sync with table_descriptions.py (keys must match available_tables).
# ─────────────────────────────────────────────────────────────────────────────
_TABLE_CORPUS: Dict[str, str] = {
    "internacoes": (
        "internações hospitalares AIH autorização internação hospitalar "
        "paciente óbito morte diagnóstico cirurgia clínica obstetrícia "
        "MORTE boolean UTI unidade terapia intensiva VAL_UTI "
        "SEXO IDADE ESPEC VDRL IND_VDRL custos permanência DIAS_PERM "
        "VAL_TOT VAL_SH VAL_SP MUNIC_RES DIAG_PRINC CID_MORTE DT_INTER"
    ),
    "atendimentos": (
        "atendimentos procedimentos realizados por internação tabela junction "
        "N_AIH PROC_REA procedimentos mais comuns mais realizados "
        "frequência de procedimento quais procedimentos foram realizados"
    ),
    "procedimentos": (
        "procedimentos NOME_PROC PROC_REA descrição nome do procedimento "
        "código do procedimento lista de procedimentos nomes"
    ),
    "cid": (
        "cid CID-10 doença diagnóstico código CD_DESCRICAO causa de morte "
        "causa da internação doenças respiratórias cardiovasculares câncer "
        "diagnóstico principal código de diagnóstico internado por doença"
    ),
    "hospital": (
        "hospital estabelecimento CNES nome fantasia localização MUNIC_MOV "
        "por hospital cidade do hospital qual hospital nome do hospital"
    ),
    "municipios": (
        "municipios município cidade nome codigo_6d localização "
        "por município por cidade residência do paciente nome do município"
    ),
    "socioeconomico": (
        "socioeconomico IDHM índice desenvolvimento humano "
        "mortalidade infantil taxa de mortalidade infantil "
        "crianças menores de 1 ano bebês óbitos infantis "
        "bolsa família saneamento esgotamento sanitário "
        "população total taxa de envelhecimento dados IBGE "
        "metrica valor por município indicadores socioeconômicos"
    ),
    "raca_cor": (
        "raça cor RACA_COR branca parda preta amarela indígena "
        "por raça por cor descrição da raça cor da pele"
    ),
    "instrucao": (
        "instrução escolaridade INSTRU nível de instrução "
        "alfabetizado analfabeto grau de instrução por escolaridade "
        "nível educacional"
    ),
    "especialidade": (
        "especialidade ESPEC cirúrgico obstétrico clínico pediátrico "
        "psiquiatria crônico descrição de especialidade tipo de leito "
        "por especialidade especialidade médica"
    ),
    "vincprev": (
        "vínculo previdenciário VINCPREV autônomo desempregado aposentado "
        "empregado empregador segurado previdência tipo de vínculo"
    ),
    "etnia": (
        "etnia ETNIA grupo étnico indígena descrição da etnia"
    ),
    "nacionalidade": (
        "nacionalidade NACIONAL estrangeiro brasileiro país de origem"
    ),
    "sexo": (
        "sexo SEXO masculino feminino por sexo gênero"
    ),
}

# ─────────────────────────────────────────────────────────────────────────────
# Expansion triggers: when the key table is selected, co-require the others.
# Only the first element is the trigger; the rest are dependencies.
# ─────────────────────────────────────────────────────────────────────────────
_TABLE_EXPANSIONS: List[Tuple[str, ...]] = [
    # atendimentos is the junction table — always needs internacoes + procedimentos
    ("atendimentos", "procedimentos", "internacoes"),
]


class EmbeddingTableSelector:
    """Semantic table selection using cosine similarity of sentence embeddings.

    Lazy-loaded: the SentenceTransformer model is NOT imported at module level
    to avoid slowing down startup for callers that never use embedding selection.
    """

    def __init__(self) -> None:
        self._model = None  # lazy
        self._table_embeddings: Optional[Dict[str, "np.ndarray"]] = None

    # ------------------------------------------------------------------
    # Lazy initialisation
    # ------------------------------------------------------------------
    def _ensure_model(self) -> None:
        if self._model is not None:
            return
        try:
            from sentence_transformers import SentenceTransformer
            import numpy as np

            # Use multilingual model — required for Portuguese queries.
            # paraphrase-multilingual-MiniLM-L12-v2 handles PT/EN/ES natively.
            model_name = "paraphrase-multilingual-MiniLM-L12-v2"
            logger.info(f"Loading SentenceTransformer model ({model_name})…")
            self._model = SentenceTransformer(model_name)
            self._np = np
            logger.info("SentenceTransformer loaded")
        except Exception as exc:
            logger.warning("Failed to load SentenceTransformer", extra={"error": str(exc)})
            raise

    def _ensure_embeddings(self) -> None:
        if self._table_embeddings is not None:
            return
        self._ensure_model()
        np = self._np
        corpus = list(_TABLE_CORPUS.items())
        texts = [doc for _, doc in corpus]
        names = [name for name, _ in corpus]
        vecs = self._model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)
        self._table_embeddings = {name: vecs[i] for i, name in enumerate(names)}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def select(
        self,
        query: str,
        available_tables: Optional[List[str]] = None,
        threshold: float = 0.50,
        top_k: int = 3,
    ) -> Tuple[List[str], float]:
        """Return (selected_tables, max_similarity).

        - Only tables in *available_tables* (if given) are considered.
        - Returns ([], 0.0) if max similarity is below *threshold*.
        - Caps result at *top_k* tables, but expands via group rules.
        """
        try:
            self._ensure_embeddings()
            np = self._np

            q_vec = self._model.encode([query], convert_to_numpy=True, normalize_embeddings=True)[0]

            # Score each table
            scores: Dict[str, float] = {}
            for tname, tvec in self._table_embeddings.items():
                if available_tables and tname not in available_tables:
                    continue
                scores[tname] = float(np.dot(q_vec, tvec))

            if not scores:
                return [], 0.0

            sorted_tables = sorted(scores, key=scores.__getitem__, reverse=True)
            max_sim = scores[sorted_tables[0]]

            if max_sim < threshold:
                return [], max_sim

            # Top-k candidates
            candidates = sorted_tables[:top_k]

            # Expand via trigger rules (only when trigger table is in candidates)
            result = list(candidates)
            for expansion in _TABLE_EXPANSIONS:
                trigger = expansion[0]
                deps = expansion[1:]
                if trigger in result:
                    for dep in deps:
                        if dep not in result and (available_tables is None or dep in available_tables):
                            result.append(dep)

            return result, max_sim

        except Exception as exc:
            logger.warning("Embedding table selection failed", extra={"error": str(exc)})
            return [], 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Module-level singleton
# ─────────────────────────────────────────────────────────────────────────────
_table_selector: Optional[EmbeddingTableSelector] = None


def get_embedding_selector() -> EmbeddingTableSelector:
    """Return the module-level EmbeddingTableSelector singleton."""
    global _table_selector
    if _table_selector is None:
        _table_selector = EmbeddingTableSelector()
    return _table_selector
