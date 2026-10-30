"""Every QL request in queries/ql_corpus.py parses (the parser's only test; no lake needed).

  python -m pytest tests/test_ql_corpus.py
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "queries")]
import ql  # noqa: E402
from ql_corpus import QL, QL_HISTORY  # noqa: E402


@pytest.mark.parametrize("name,query", QL + QL_HISTORY, ids=[n for n, _ in QL + QL_HISTORY])
def test_corpus_query_parses(name, query):
    ql.parse(query)  # raises ql.QLError on a syntax or static error
