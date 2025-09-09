import pytest

from src.utils.sql_safety import is_select_only


@pytest.mark.parametrize(
    "sql,expected_ok",
    [
        ("SELECT * FROM tabela;", True),
        ("with cte as (select 1) select * from cte;", True),
        (" -- comment only\nSELECT 1; -- end", True),
        ("UPDATE tabela SET a=1;", False),
        ("DROP TABLE tabela;", False),
        ("CREATE TABLE x(a int);", False),
        ("SELECT 1; SELECT 2;", False),
        ("DELETE FROM t;", False),
        ("TRUNCATE TABLE t;", False),
        ("GRANT SELECT ON t TO u;", False),
        ("VACUUM;", False),
        (";", False),
        ("", False),
    ],
)
def test_is_select_only(sql, expected_ok):
    ok, _ = is_select_only(sql)
    assert ok is expected_ok


def test_is_select_only_reason_messages():
    ok, reason = is_select_only("UPDATE t SET a=1;")
    assert not ok
    assert "SELECT" in reason

    ok, reason = is_select_only("SELECT 1; SELECT 2;")
    assert not ok
    assert "Múltiplas instruções" in reason

