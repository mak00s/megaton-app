"""The runtime capability follows the actual delegated query implementation."""

from megaton.query import run_query

from megaton_lib import megaton_client


def test_query_limit_contract_matches_core_delegation():
    assert megaton_client.QUERY_LIMIT_CONTRACT == "total_rows"
    assert megaton_client.query_ga4.__globals__["run_query"] is run_query
    assert megaton_client.query_gsc.__globals__["run_query"] is run_query
