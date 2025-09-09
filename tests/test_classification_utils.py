import pytest

from src.utils.classification import (
    normalize_text,
    detect_sql_snippets,
    keyword_scores,
    heuristic_route,
    try_extract_json_block,
    combine_scores,
)


def test_normalize_text_removes_diacritics_and_lowercases():
    s = "MÉDIA de Óbitos"
    assert normalize_text(s) == "media de obitos"


@pytest.mark.parametrize(
    "text,expected",
    [
        ("SELECT * FROM t", True),
        ("```sql\nselect 1;\n```", True),
        ("Quais colunas existem?", False),
        ("Explica o que é CID?", False),
    ],
)
def test_detect_sql_snippets(text, expected):
    assert detect_sql_snippets(text) is expected


def test_keyword_scores_and_heuristic_route_database():
    text = "Quantos óbitos ocorreram? Top 5 por ano"
    route, scores = heuristic_route(text)
    assert route in ("DATABASE", "SCHEMA", "CONVERSATIONAL")
    assert scores["DATABASE"] >= 2
    assert route == "DATABASE"


def test_heuristic_route_conversational():
    text = "O que significa o CID J189?"
    route, scores = heuristic_route(text)
    assert route == "CONVERSATIONAL"
    assert scores["CONVERSATIONAL"] >= 1


def test_try_extract_json_block_parses_direct_json():
    s = '{"route":"SCHEMA","confidence":0.8,"reasons":"tabelas"}'
    data = try_extract_json_block(s)
    assert isinstance(data, dict)
    assert data["route"] == "SCHEMA"


def test_try_extract_json_block_from_text():
    s = "Resposta: {\n  \"route\": \"CONVERSATIONAL\", \n  \"confidence\": 0.9, \n  \"reasons\": \"definição\"\n} Obrigado"
    data = try_extract_json_block(s)
    assert isinstance(data, dict)
    assert data["route"] == "CONVERSATIONAL"


def test_combine_scores_prefers_llm_when_confident():
    heur = {"DATABASE": 1, "CONVERSATIONAL": 0, "SCHEMA": 0}
    route = combine_scores("CONVERSATIONAL", 0.9, heur, w_llm=0.7)
    assert route == "CONVERSATIONAL"


def test_combine_scores_uses_heuristics_when_llm_uncertain():
    heur = {"DATABASE": 2, "CONVERSATIONAL": 0, "SCHEMA": 0}
    route = combine_scores("CONVERSATIONAL", 0.5, heur, w_llm=0.7)
    # heuristic should push toward DATABASE
    assert route == "DATABASE"

