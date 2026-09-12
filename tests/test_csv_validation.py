import pandas as pd
import pytest

from megaton_lib.csv_validation import validate_generated_import_csv


def validate(expected, path, **kwargs):
    return validate_generated_import_csv(df_expected=expected, csv_path=path, key_column="id", **kwargs)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig"])
def test_snapshot_accepts_reordered_rows_and_bom(tmp_path, encoding):
    expected = pd.DataFrame({"id": ["001", "002"], "value": ["日本語", "line\nbreak"]})
    path = tmp_path / "generated.csv"
    expected.iloc[::-1].to_csv(path, index=False, encoding=encoding)
    report = validate(expected, path, compare_all_columns_by_key=True)
    assert report["row_count"] == 2
    assert report["mismatch_rows"] == 0
    assert len(report["key_fingerprint"]) == 64


@pytest.mark.parametrize(("actual", "message"), [
    ({"id": ["", "2"], "value": ["a", "b"]}, "empty key"),
    ({"id": ["1", "1"], "value": ["a", "b"]}, "duplicate keys"),
    ({"id": ["1", "3"], "value": ["a", "b"]}, "fingerprint mismatch"),
    ({"value": ["a", "b"], "id": ["1", "2"]}, "header mismatch"),
    ({"id": ["1"], "value": ["a"]}, "row count mismatch"),
])
def test_mismatched_snapshot_is_rejected(tmp_path, actual, message):
    path = tmp_path / "generated.csv"
    pd.DataFrame(actual).to_csv(path, index=False)
    with pytest.raises(RuntimeError, match=message):
        validate(pd.DataFrame({"id": ["1", "2"], "value": ["a", "b"]}), path)


def test_value_comparison_is_explicit_and_detects_same_count_changes(tmp_path):
    expected = pd.DataFrame({"id": ["1"], "value": ["old"]})
    path = tmp_path / "generated.csv"
    pd.DataFrame({"id": ["1"], "value": ["new"]}).to_csv(path, index=False)
    validate(expected, path)
    with pytest.raises(RuntimeError, match="differs from expected source values"):
        validate(expected, path, compare_all_columns_by_key=True)


def test_missing_values_keep_existing_not_set_policy(tmp_path):
    expected = pd.DataFrame({"id": ["1"], "value": [None]})
    path = tmp_path / "generated.csv"
    expected.fillna("(not set)").to_csv(path, index=False)
    validate(expected, path, compare_all_columns_by_key=True)
    expected.fillna("").to_csv(path, index=False)
    with pytest.raises(RuntimeError, match="differs from expected source values"):
        validate(expected, path, compare_all_columns_by_key=True)


def test_required_headers_are_ordered(tmp_path):
    expected = pd.DataFrame({"id": ["1"], "value": ["a"]})
    path = tmp_path / "generated.csv"
    expected.to_csv(path, index=False)
    with pytest.raises(RuntimeError, match="header mismatch"):
        validate(expected, path, required_headers=["value", "id"])


def test_whitespace_padded_keys_align_in_value_comparison(tmp_path):
    expected = pd.DataFrame({"id": [" 1 ", "2"], "value": ["a", "b"]})
    path = tmp_path / "generated.csv"
    pd.DataFrame({"id": ["1", "2 "], "value": ["a", "b"]}).to_csv(path, index=False)
    validate(expected, path, compare_all_columns_by_key=True)
    pd.DataFrame({"id": ["1", "2 "], "value": ["a", "CHANGED"]}).to_csv(path, index=False)
    with pytest.raises(RuntimeError, match="differs from expected source values"):
        validate(expected, path, compare_all_columns_by_key=True)


def test_missing_file_is_reported(tmp_path):
    with pytest.raises(RuntimeError, match="not found"):
        validate(pd.DataFrame({"id": []}), tmp_path / "missing.csv")


def test_invalid_utf8_is_not_silently_decoded(tmp_path):
    path = tmp_path / "generated.csv"
    path.write_bytes(b"id,value\n1,\xff\n")
    with pytest.raises(UnicodeDecodeError):
        validate(pd.DataFrame({"id": ["1"], "value": ["a"]}), path)
