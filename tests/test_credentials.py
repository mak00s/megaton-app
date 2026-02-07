import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from megaton_lib.credentials import resolve_service_account_path, list_service_account_paths


class TestCredentials(unittest.TestCase):
    def test_resolve_from_env_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            cred_file = Path(tmp) / "sa.json"
            cred_file.write_text("{}", encoding="utf-8")
            with patch.dict(os.environ, {"MEGATON_CREDS_PATH": str(cred_file)}, clear=False):
                path = resolve_service_account_path(default_dir=Path(tmp) / "unused")
                self.assertEqual(path, str(cred_file))

    def test_resolve_from_default_directory_single_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            cred_file = Path(tmp) / "sa.json"
            cred_file.write_text("{}", encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                path = resolve_service_account_path(default_dir=tmp)
                self.assertEqual(path, str(cred_file))

    def test_resolve_from_default_directory_multiple_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "a.json").write_text("{}", encoding="utf-8")
            (Path(tmp) / "b.json").write_text("{}", encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(RuntimeError):
                    resolve_service_account_path(default_dir=tmp)

    def test_resolve_from_default_directory_no_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(FileNotFoundError):
                    resolve_service_account_path(default_dir=tmp)


class TestListServiceAccountPaths(unittest.TestCase):
    def test_list_single_file_via_env(self):
        with tempfile.TemporaryDirectory() as tmp:
            cred_file = Path(tmp) / "sa.json"
            cred_file.write_text("{}", encoding="utf-8")
            with patch.dict(os.environ, {"MEGATON_CREDS_PATH": str(cred_file)}, clear=False):
                paths = list_service_account_paths(default_dir=Path(tmp) / "unused")
                self.assertEqual(paths, [str(cred_file)])

    def test_list_multiple_files_in_default_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = Path(tmp) / "a.json"
            b = Path(tmp) / "b.json"
            a.write_text("{}", encoding="utf-8")
            b.write_text("{}", encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                paths = list_service_account_paths(default_dir=tmp)
                self.assertEqual(len(paths), 2)
                self.assertEqual(paths, sorted(paths))
                self.assertIn(str(a), paths)
                self.assertIn(str(b), paths)

    def test_list_env_dir_multiple(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "x.json").write_text("{}", encoding="utf-8")
            (Path(tmp) / "y.json").write_text("{}", encoding="utf-8")
            with patch.dict(os.environ, {"MEGATON_CREDS_PATH": tmp}, clear=False):
                paths = list_service_account_paths()
                self.assertEqual(len(paths), 2)

    def test_list_empty_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {}, clear=True):
                paths = list_service_account_paths(default_dir=tmp)
                self.assertEqual(paths, [])

    def test_list_nonexistent_env_path(self):
        with patch.dict(os.environ, {"MEGATON_CREDS_PATH": "/nonexistent/path"}, clear=False):
            with self.assertRaises(FileNotFoundError):
                list_service_account_paths()

    def test_list_nonexistent_default_dir_returns_empty(self):
        with patch.dict(os.environ, {}, clear=True):
            paths = list_service_account_paths(default_dir="/nonexistent/dir")
            self.assertEqual(paths, [])

    def test_resolve_still_errors_on_multiple(self):
        """resolve_service_account_path は複数ファイルでエラーのまま（後方互換）"""
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "a.json").write_text("{}", encoding="utf-8")
            (Path(tmp) / "b.json").write_text("{}", encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(RuntimeError):
                    resolve_service_account_path(default_dir=tmp)


if __name__ == "__main__":
    unittest.main()
