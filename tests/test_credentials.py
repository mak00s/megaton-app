import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import megaton_lib.credentials as credentials_mod
from megaton_lib.credentials import (
    list_adobe_oauth_paths,
    list_service_account_paths,
    load_adobe_oauth_credentials,
    resolve_service_account_path,
)


SERVICE_ACCOUNT_JSON = (
    '{"type":"service_account","client_email":"svc@example.com",'
    '"private_key":"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n"}'
)
ADOBE_OAUTH_JSON = (
    '{"client_id":"cid","client_secret":"csec","org_id":"ORG@AdobeOrg"}'
)


class TestCredentials(unittest.TestCase):
    def test_resolve_from_env_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            cred_file = Path(tmp) / "sa.json"
            cred_file.write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            with patch.dict(os.environ, {"MEGATON_CREDS_PATH": str(cred_file)}, clear=False):
                path = resolve_service_account_path(default_dir=Path(tmp) / "unused")
                self.assertEqual(path, str(cred_file))

    def test_resolve_from_default_directory_single_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            cred_file = Path(tmp) / "sa.json"
            cred_file.write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                path = resolve_service_account_path(default_dir=tmp)
                self.assertEqual(path, str(cred_file))

    def test_resolve_from_default_directory_multiple_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "a.json").write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            (Path(tmp) / "b.json").write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(RuntimeError):
                    resolve_service_account_path(default_dir=tmp)

    def test_resolve_from_default_directory_no_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(FileNotFoundError):
                    resolve_service_account_path(default_dir=tmp)

    def test_resolve_walks_up_to_find_credentials_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "credentials").mkdir()
            cred_file = root / "credentials" / "sa.json"
            cred_file.write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")

            nested = root / "notebooks" / "reports"
            nested.mkdir(parents=True)

            old_cwd = os.getcwd()
            try:
                os.chdir(nested)
                with patch.dict(os.environ, {}, clear=True):
                    path = resolve_service_account_path()
                    self.assertEqual(str(Path(path).resolve()), str(cred_file.resolve()))
            finally:
                os.chdir(old_cwd)

    def test_resolve_fallback_to_package_parent_credentials(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_app = Path(tmp) / "fake_app"
            fake_pkg = fake_app / "megaton_lib"
            fake_pkg.mkdir(parents=True)
            fake_module_file = fake_pkg / "credentials.py"
            fake_module_file.write_text("# test marker\n", encoding="utf-8")

            fake_creds = fake_app / "credentials"
            fake_creds.mkdir()
            cred_file = fake_creds / "sa.json"
            cred_file.write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")

            external = Path(tmp) / "other_repo"
            external.mkdir()

            old_cwd = os.getcwd()
            try:
                os.chdir(external)
                with patch.dict(os.environ, {}, clear=True):
                    with patch.object(credentials_mod, "__file__", str(fake_module_file)):
                        path = resolve_service_account_path()
                        self.assertEqual(str(Path(path).resolve()), str(cred_file.resolve()))
            finally:
                os.chdir(old_cwd)

    def test_resolve_fallback_to_package_parent_credentials_empty_raises(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_app = Path(tmp) / "fake_app"
            fake_pkg = fake_app / "megaton_lib"
            fake_pkg.mkdir(parents=True)
            fake_module_file = fake_pkg / "credentials.py"
            fake_module_file.write_text("# test marker\n", encoding="utf-8")
            (fake_app / "credentials").mkdir()

            external = Path(tmp) / "other_repo"
            external.mkdir()

            old_cwd = os.getcwd()
            try:
                os.chdir(external)
                with patch.dict(os.environ, {}, clear=True):
                    with patch.object(credentials_mod, "__file__", str(fake_module_file)):
                        with self.assertRaises(FileNotFoundError):
                            resolve_service_account_path()
            finally:
                os.chdir(old_cwd)


class TestListServiceAccountPaths(unittest.TestCase):
    def test_list_single_file_via_env(self):
        with tempfile.TemporaryDirectory() as tmp:
            cred_file = Path(tmp) / "sa.json"
            cred_file.write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            with patch.dict(os.environ, {"MEGATON_CREDS_PATH": str(cred_file)}, clear=False):
                paths = list_service_account_paths(default_dir=Path(tmp) / "unused")
                self.assertEqual(paths, [str(cred_file)])

    def test_list_multiple_files_in_default_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = Path(tmp) / "a.json"
            b = Path(tmp) / "b.json"
            a.write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            b.write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                paths = list_service_account_paths(default_dir=tmp)
                self.assertEqual(len(paths), 2)
                self.assertEqual(paths, sorted(paths))
                self.assertIn(str(a), paths)
                self.assertIn(str(b), paths)

    def test_list_env_dir_multiple(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "x.json").write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            (Path(tmp) / "y.json").write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
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
        """resolve_service_account_path still errors on multiple files (backward compatibility)."""
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "a.json").write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            (Path(tmp) / "b.json").write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(RuntimeError):
                    resolve_service_account_path(default_dir=tmp)

    def test_list_walks_up_to_find_credentials_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "credentials").mkdir()
            (root / "credentials" / "a.json").write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            (root / "credentials" / "b.json").write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")

            nested = root / "notebooks" / "reports"
            nested.mkdir(parents=True)

            old_cwd = os.getcwd()
            try:
                os.chdir(nested)
                with patch.dict(os.environ, {}, clear=True):
                    paths = list_service_account_paths()
                    self.assertEqual(len(paths), 2)
                    self.assertEqual(paths, sorted(paths))
            finally:
                os.chdir(old_cwd)

    def test_fallback_to_package_parent_credentials(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_app = Path(tmp) / "fake_app"
            fake_pkg = fake_app / "megaton_lib"
            fake_pkg.mkdir(parents=True)
            fake_module_file = fake_pkg / "credentials.py"
            fake_module_file.write_text("# test marker\n", encoding="utf-8")

            fake_creds = fake_app / "credentials"
            fake_creds.mkdir()
            a = fake_creds / "a.json"
            b = fake_creds / "b.json"
            a.write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            b.write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")

            external = Path(tmp) / "other_repo"
            external.mkdir()

            old_cwd = os.getcwd()
            try:
                os.chdir(external)
                with patch.dict(os.environ, {}, clear=True):
                    with patch.object(credentials_mod, "__file__", str(fake_module_file)):
                        paths = list_service_account_paths()
                        actual = sorted(str(Path(p).resolve()) for p in paths)
                        expected = sorted([str(a.resolve()), str(b.resolve())])
                        self.assertEqual(actual, expected)
            finally:
                os.chdir(old_cwd)

    def test_fallback_to_package_parent_credentials_empty_returns_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake_app = Path(tmp) / "fake_app"
            fake_pkg = fake_app / "megaton_lib"
            fake_pkg.mkdir(parents=True)
            fake_module_file = fake_pkg / "credentials.py"
            fake_module_file.write_text("# test marker\n", encoding="utf-8")
            (fake_app / "credentials").mkdir()

            external = Path(tmp) / "other_repo"
            external.mkdir()

            old_cwd = os.getcwd()
            try:
                os.chdir(external)
                with patch.dict(os.environ, {}, clear=True):
                    with patch.object(credentials_mod, "__file__", str(fake_module_file)):
                        paths = list_service_account_paths()
                        self.assertEqual(paths, [])
            finally:
                os.chdir(old_cwd)


class TestAdobeOAuthCredentials(unittest.TestCase):
    def test_list_adobe_oauth_paths_filters_only_adobe_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "sa.json").write_text(SERVICE_ACCOUNT_JSON, encoding="utf-8")
            adobe = Path(tmp) / "adobe.json"
            adobe.write_text(ADOBE_OAUTH_JSON, encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                paths = list_adobe_oauth_paths(default_dir=tmp)
                self.assertEqual(paths, [str(adobe)])

    def test_list_adobe_oauth_paths_from_env_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = Path(tmp) / "adobe-a.json"
            b = Path(tmp) / "adobe-b.json"
            a.write_text(ADOBE_OAUTH_JSON, encoding="utf-8")
            b.write_text(
                '{"client_id":"cid2","client_secret":"csec2","ims_org_id":"ORG2@AdobeOrg"}',
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"ADOBE_CREDS_PATH": tmp}, clear=False):
                paths = list_adobe_oauth_paths()
                self.assertEqual(paths, [str(a), str(b)])

    def test_load_adobe_oauth_credentials_normalizes_org_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            cred_file = Path(tmp) / "adobe.json"
            cred_file.write_text(
                '{"client_id":"cid","client_secret":"csec","ims_org_id":"ORG@AdobeOrg","scope":"openid"}',
                encoding="utf-8",
            )
            out = load_adobe_oauth_credentials(cred_file)
            self.assertEqual(out["client_id"], "cid")
            self.assertEqual(out["client_secret"], "csec")
            self.assertEqual(out["org_id"], "ORG@AdobeOrg")
            self.assertEqual(out["scopes"], "openid")
            self.assertEqual(out["source_path"], str(cred_file))


if __name__ == "__main__":
    unittest.main()
