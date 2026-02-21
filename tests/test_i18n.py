"""Translation completeness and consistency tests."""
import re
import unittest
from unittest.mock import patch

import streamlit as st

from app.i18n import TRANSLATIONS, language_selector, t, translated_select_model


class TestTranslationCompleteness(unittest.TestCase):
    """Every key in 'ja' must exist in 'en' and vice versa."""

    def test_en_covers_ja(self):
        missing = set(TRANSLATIONS["ja"]) - set(TRANSLATIONS["en"])
        self.assertEqual(missing, set(), f"Keys in 'ja' but missing in 'en': {missing}")

    def test_ja_covers_en(self):
        missing = set(TRANSLATIONS["en"]) - set(TRANSLATIONS["ja"])
        self.assertEqual(missing, set(), f"Keys in 'en' but missing in 'ja': {missing}")


class TestPlaceholderConsistency(unittest.TestCase):
    """Placeholders ({name}) must match between languages."""

    _PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")

    def test_placeholders_match(self):
        for key in TRANSLATIONS["ja"]:
            ja_ph = set(self._PLACEHOLDER_RE.findall(TRANSLATIONS["ja"][key]))
            en_text = TRANSLATIONS["en"].get(key)
            if en_text is None:
                continue  # caught by completeness test
            en_ph = set(self._PLACEHOLDER_RE.findall(en_text))
            self.assertEqual(
                ja_ph,
                en_ph,
                f"Placeholder mismatch for '{key}': ja={ja_ph}, en={en_ph}",
            )


class TestTFunction(unittest.TestCase):
    """Basic t() behaviour."""

    def test_returns_key_on_miss(self):
        result = t("nonexistent.key.xyz")
        self.assertEqual(result, "nonexistent.key.xyz")

    def test_format_substitution(self):
        # Temporarily force ja
        import streamlit as st
        original = st.session_state.get("lang")
        st.session_state["lang"] = "ja"
        try:
            result = t("msg.rows_fetched", count="1,234")
            self.assertIn("1,234", result)
        finally:
            if original is None:
                st.session_state.pop("lang", None)
            else:
                st.session_state["lang"] = original

    def test_no_empty_values(self):
        for lang, table in TRANSLATIONS.items():
            for key, value in table.items():
                self.assertTrue(
                    value.strip(),
                    f"Empty translation for '{key}' in '{lang}'",
                )


class TestLanguageSwitchIntegration(unittest.TestCase):
    """Integration tests for language switch and translated select state."""

    SAVE_MODE_KEYS = {
        "save.mode_overwrite": "overwrite",
        "save.mode_append": "append",
        "save.mode_upsert": "upsert",
    }
    SAVE_BQ_MODE_KEYS = {
        "save.mode_overwrite": "overwrite",
        "save.mode_append": "append",
    }

    def setUp(self):
        self._backup = dict(st.session_state)
        for key in list(st.session_state.keys()):
            del st.session_state[key]

    def tearDown(self):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        for key, value in self._backup.items():
            st.session_state[key] = value

    @patch("app.i18n.st.rerun")
    @patch("app.i18n.st.radio")
    def test_language_switch_clears_select_keys_and_keeps_internal_values(self, radio_mock, rerun_mock):
        st.session_state["lang"] = "ja"
        st.session_state["w_save_mode"] = "upsert"
        st.session_state["w_save_bq_mode"] = "append"
        st.session_state["w_save_mode_select"] = "アップサート"
        st.session_state["w_save_bq_mode_select"] = "追記"
        st.session_state["w_untouched"] = "keep"

        radio_mock.return_value = "en"
        language_selector()

        self.assertEqual(st.session_state["lang"], "en")
        self.assertEqual(st.session_state["w_save_mode"], "upsert")
        self.assertEqual(st.session_state["w_save_bq_mode"], "append")
        self.assertNotIn("w_save_mode_select", st.session_state)
        self.assertNotIn("w_save_bq_mode_select", st.session_state)
        self.assertEqual(st.session_state["w_untouched"], "keep")
        rerun_mock.assert_called_once()

    @patch("app.i18n.st.rerun")
    @patch("app.i18n.st.radio")
    def test_save_mode_default_label_recovers_after_language_switch(self, radio_mock, _rerun_mock):
        # Initial JA selection stores internal values and translated labels.
        st.session_state["lang"] = "ja"
        _, _, opts_ja = translated_select_model(self.SAVE_MODE_KEYS, current_value="upsert")
        _, _, bq_opts_ja = translated_select_model(self.SAVE_BQ_MODE_KEYS, current_value="append")
        save_label_ja = next(label for label, value in opts_ja.items() if value == "upsert")
        bq_label_ja = next(label for label, value in bq_opts_ja.items() if value == "append")
        st.session_state["w_save_mode"] = opts_ja[save_label_ja]
        st.session_state["w_save_bq_mode"] = bq_opts_ja[bq_label_ja]
        st.session_state["w_save_mode_select"] = save_label_ja
        st.session_state["w_save_bq_mode_select"] = bq_label_ja

        # Switch to EN, selector labels should be rebuilt from internal values.
        radio_mock.return_value = "en"
        language_selector()
        save_labels_en, save_idx_en, save_opts_en = translated_select_model(
            self.SAVE_MODE_KEYS,
            current_value=st.session_state["w_save_mode"],
        )
        bq_labels_en, bq_idx_en, bq_opts_en = translated_select_model(
            self.SAVE_BQ_MODE_KEYS,
            current_value=st.session_state["w_save_bq_mode"],
        )

        self.assertEqual(st.session_state["lang"], "en")
        self.assertEqual(save_labels_en[save_idx_en], "Upsert")
        self.assertEqual(save_opts_en["Upsert"], "upsert")
        self.assertEqual(bq_labels_en[bq_idx_en], "Append")
        self.assertEqual(bq_opts_en["Append"], "append")


if __name__ == "__main__":
    unittest.main()
