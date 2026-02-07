"""megaton_client のレジストリパターン（複数クレデンシャル自動ルーティング）のテスト"""
import unittest
from unittest.mock import patch, MagicMock

import lib.megaton_client as mc


def _reset_registry():
    """テスト間でモジュール状態をリセット"""
    mc._instances.clear()
    mc._property_map.clear()
    mc._site_map.clear()
    mc._registry_built = False
    mc._bq_clients.clear()


def _make_mock_megaton(accounts=None, sites=None):
    """モック Megaton インスタンスを生成"""
    mg = MagicMock()
    # GA4
    ga4 = MagicMock()
    ga4.accounts = accounts or []
    mg.ga = {"4": ga4}
    # GSC
    search_get = MagicMock()
    search_get.sites.return_value = sites or []
    search = MagicMock()
    search.get = search_get
    mg.search = search
    return mg


class TestGetMegaton(unittest.TestCase):
    def setUp(self):
        _reset_registry()

    def tearDown(self):
        _reset_registry()

    @patch("lib.megaton_client.start.Megaton")
    @patch("lib.megaton_client.list_service_account_paths", return_value=["/creds/a.json"])
    def test_singleton_per_path(self, mock_list, mock_megaton):
        mg1 = mc.get_megaton("/creds/a.json")
        mg2 = mc.get_megaton("/creds/a.json")
        self.assertIs(mg1, mg2)
        mock_megaton.assert_called_once_with("/creds/a.json", headless=True)

    @patch("lib.megaton_client.start.Megaton")
    @patch("lib.megaton_client.list_service_account_paths", return_value=["/creds/a.json"])
    def test_different_paths_different_instances(self, mock_list, mock_megaton):
        mock_megaton.side_effect = [MagicMock(name="mg_a"), MagicMock(name="mg_b")]
        mg1 = mc.get_megaton("/creds/a.json")
        mg2 = mc.get_megaton("/creds/b.json")
        self.assertIsNot(mg1, mg2)
        self.assertEqual(mock_megaton.call_count, 2)

    @patch("lib.megaton_client.list_service_account_paths", return_value=[])
    def test_no_creds_raises(self, mock_list):
        with self.assertRaises(FileNotFoundError):
            mc.get_megaton()

    @patch("lib.megaton_client.start.Megaton")
    @patch("lib.megaton_client.list_service_account_paths", return_value=["/creds/first.json", "/creds/second.json"])
    def test_none_path_uses_first(self, mock_list, mock_megaton):
        mc.get_megaton()
        mock_megaton.assert_called_once_with("/creds/first.json", headless=True)


class TestBuildRegistry(unittest.TestCase):
    def setUp(self):
        _reset_registry()

    def tearDown(self):
        _reset_registry()

    @patch("lib.megaton_client.list_service_account_paths")
    @patch("lib.megaton_client.start.Megaton")
    def test_maps_properties_from_two_creds(self, mock_megaton_cls, mock_list):
        mock_list.return_value = ["/creds/a.json", "/creds/b.json"]

        mg_a = _make_mock_megaton(
            accounts=[{"id": "acc1", "properties": [{"id": "P1", "name": "Prop1"}]}],
            sites=["https://a.example.com/"],
        )
        mg_b = _make_mock_megaton(
            accounts=[{"id": "acc2", "properties": [{"id": "P2", "name": "Prop2"}]}],
            sites=["https://b.example.com/"],
        )
        mock_megaton_cls.side_effect = [mg_a, mg_b]

        mc.build_registry()

        self.assertEqual(mc._property_map["P1"], "/creds/a.json")
        self.assertEqual(mc._property_map["P2"], "/creds/b.json")
        self.assertEqual(mc._site_map["https://a.example.com/"], "/creds/a.json")
        self.assertEqual(mc._site_map["https://b.example.com/"], "/creds/b.json")

    @patch("lib.megaton_client.list_service_account_paths")
    @patch("lib.megaton_client.start.Megaton")
    def test_skips_no_access_credential(self, mock_megaton_cls, mock_list):
        """GA4アクセスのないクレデンシャルはスキップ"""
        mock_list.return_value = ["/creds/a.json", "/creds/b.json"]

        # a.json: GA4のみ（GSCアクセスなし）
        mg_a = MagicMock()
        ga4_a = MagicMock()
        ga4_a.accounts = [{"id": "acc1", "properties": [{"id": "P1", "name": "Prop1"}]}]
        mg_a.ga = {"4": ga4_a}
        mg_a.search.get.sites.side_effect = Exception("No GSC access")

        # b.json: GSCのみ（GA4アクセスなし）
        mg_b = MagicMock()
        mg_b.ga = {"4": MagicMock()}
        mg_b.ga["4"].accounts.__iter__ = MagicMock(side_effect=Exception("No GA4 access"))
        search_get_b = MagicMock()
        search_get_b.sites.return_value = ["https://b.example.com/"]
        mg_b.search.get = search_get_b

        mock_megaton_cls.side_effect = [mg_a, mg_b]

        mc.build_registry()

        self.assertIn("P1", mc._property_map)
        self.assertNotIn("P1", mc._site_map.values())
        self.assertIn("https://b.example.com/", mc._site_map)

    @patch("lib.megaton_client.list_service_account_paths")
    @patch("lib.megaton_client.start.Megaton")
    def test_registry_built_once(self, mock_megaton_cls, mock_list):
        """build_registry は一度だけ実行"""
        mock_list.return_value = ["/creds/a.json"]
        mock_megaton_cls.return_value = _make_mock_megaton(
            accounts=[{"id": "acc1", "properties": [{"id": "P1", "name": "Prop1"}]}],
            sites=[],
        )

        mc.build_registry()
        mc.build_registry()  # 2回目は何もしない

        mock_megaton_cls.assert_called_once()


class TestRoutingFunctions(unittest.TestCase):
    def setUp(self):
        _reset_registry()

    def tearDown(self):
        _reset_registry()

    @patch("lib.megaton_client.list_service_account_paths")
    @patch("lib.megaton_client.start.Megaton")
    def test_get_megaton_for_property(self, mock_megaton_cls, mock_list):
        mock_list.return_value = ["/creds/a.json"]
        mg_a = _make_mock_megaton(
            accounts=[{"id": "acc1", "properties": [{"id": "P1", "name": "Prop1"}]}],
            sites=[],
        )
        mock_megaton_cls.return_value = mg_a

        result = mc.get_megaton_for_property("P1")
        self.assertIs(result, mg_a)

    @patch("lib.megaton_client.list_service_account_paths", return_value=[])
    def test_get_megaton_for_property_not_found(self, mock_list):
        with self.assertRaises(ValueError) as ctx:
            mc.get_megaton_for_property("UNKNOWN")
        self.assertIn("UNKNOWN", str(ctx.exception))

    @patch("lib.megaton_client.list_service_account_paths")
    @patch("lib.megaton_client.start.Megaton")
    def test_get_megaton_for_site(self, mock_megaton_cls, mock_list):
        mock_list.return_value = ["/creds/b.json"]
        mg_b = _make_mock_megaton(
            accounts=[],
            sites=["https://example.com/"],
        )
        mock_megaton_cls.return_value = mg_b

        result = mc.get_megaton_for_site("https://example.com/")
        self.assertIs(result, mg_b)

    @patch("lib.megaton_client.list_service_account_paths", return_value=[])
    def test_get_megaton_for_site_not_found(self, mock_list):
        with self.assertRaises(ValueError) as ctx:
            mc.get_megaton_for_site("https://unknown.example.com/")
        self.assertIn("unknown", str(ctx.exception))


class TestMergedLists(unittest.TestCase):
    def setUp(self):
        _reset_registry()

    def tearDown(self):
        _reset_registry()

    @patch("lib.megaton_client.list_service_account_paths")
    @patch("lib.megaton_client.start.Megaton")
    def test_get_ga4_properties_merges(self, mock_megaton_cls, mock_list):
        mock_list.return_value = ["/creds/a.json", "/creds/b.json"]

        mg_a = _make_mock_megaton(
            accounts=[{"id": "acc1", "properties": [{"id": "P1", "name": "Prop1"}]}],
            sites=[],
        )
        mg_b = _make_mock_megaton(
            accounts=[{"id": "acc2", "properties": [{"id": "P2", "name": "Prop2"}]}],
            sites=[],
        )
        mock_megaton_cls.side_effect = [mg_a, mg_b]

        props = mc.get_ga4_properties()
        ids = [p["id"] for p in props]
        self.assertIn("P1", ids)
        self.assertIn("P2", ids)
        self.assertEqual(len(props), 2)

    @patch("lib.megaton_client.list_service_account_paths")
    @patch("lib.megaton_client.start.Megaton")
    def test_get_ga4_properties_deduplicates(self, mock_megaton_cls, mock_list):
        """同じプロパティIDが複数クレデンシャルにある場合、重複除去"""
        mock_list.return_value = ["/creds/a.json", "/creds/b.json"]

        mg_a = _make_mock_megaton(
            accounts=[{"id": "acc1", "properties": [{"id": "P1", "name": "Prop1"}]}],
            sites=[],
        )
        mg_b = _make_mock_megaton(
            accounts=[{"id": "acc1", "properties": [{"id": "P1", "name": "Prop1"}]}],
            sites=[],
        )
        mock_megaton_cls.side_effect = [mg_a, mg_b]

        props = mc.get_ga4_properties()
        self.assertEqual(len(props), 1)

    @patch("lib.megaton_client.list_service_account_paths")
    @patch("lib.megaton_client.start.Megaton")
    def test_get_gsc_sites_merges_and_deduplicates(self, mock_megaton_cls, mock_list):
        mock_list.return_value = ["/creds/a.json", "/creds/b.json"]

        mg_a = _make_mock_megaton(
            accounts=[],
            sites=["https://a.example.com/", "https://shared.example.com/"],
        )
        mg_b = _make_mock_megaton(
            accounts=[],
            sites=["https://b.example.com/", "https://shared.example.com/"],
        )
        mock_megaton_cls.side_effect = [mg_a, mg_b]

        sites = mc.get_gsc_sites()
        self.assertIn("https://a.example.com/", sites)
        self.assertIn("https://b.example.com/", sites)
        self.assertIn("https://shared.example.com/", sites)
        # shared は1回だけ
        self.assertEqual(sites.count("https://shared.example.com/"), 1)


if __name__ == "__main__":
    unittest.main()
