"""Credential precedence and missing-file behavior, without real secrets."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from trading_engine.credentials import load_engine_environment


class CredentialLoadingTests(unittest.TestCase):
    def test_private_file_survives_new_checkout_and_overrides_repo_env(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private = root / "my-local-config.env"
            private.write_text("TRADIER_ACCESS_TOKEN=private-token\n"
                               "TRADIER_LIVE_DATA_TOKEN=private-data\n"
                               "TRADIER_ACCOUNT_ID=private-account\n"
                               "UW_API_KEY=private-uw\nMAX_ORDER_DEBIT=999999\n")
            checkout = root / "updated-repo"
            checkout.mkdir()
            (checkout / ".env").write_text("TRADIER_ACCESS_TOKEN=stale-token\n"
                                            "TRADIER_ACCOUNT_ID=stale-account\n"
                                            "MAX_ORDER_DEBIT=6000\n")
            with patch.dict(os.environ, {"ENGINE_CREDENTIALS_FILE": str(private)}, clear=True):
                self.assertEqual(load_engine_environment(checkout), private)
                self.assertEqual(os.environ["TRADIER_ACCESS_TOKEN"], "private-token")
                self.assertEqual(os.environ["TRADIER_ACCOUNT_ID"], "private-account")
                self.assertEqual(os.environ["UW_API_KEY"], "private-uw")
                self.assertEqual(os.environ["MAX_ORDER_DEBIT"], "6000")

    def test_process_environment_takes_priority(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private = root / "credentials.env"
            private.write_text("TRADIER_ACCESS_TOKEN=private-token\n")
            with patch.dict(os.environ, {"ENGINE_CREDENTIALS_FILE": str(private),
                                      "TRADIER_ACCESS_TOKEN": "process-token"}, clear=True):
                load_engine_environment(root)
                self.assertEqual(os.environ["TRADIER_ACCESS_TOKEN"], "process-token")

    def test_missing_explicit_file_fails_before_repo_fallback(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / ".env").write_text("TRADIER_ACCESS_TOKEN=stale-token\n")
            with patch.dict(os.environ, {"ENGINE_CREDENTIALS_FILE": str(root / "missing.env")}, clear=True):
                with self.assertRaisesRegex(FileNotFoundError, "ENGINE_CREDENTIALS_FILE"):
                    load_engine_environment(root)
                self.assertNotIn("TRADIER_ACCESS_TOKEN", os.environ)

    def test_home_default_loads_without_repo_env(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            private = root / ".trading_engine" / "credentials.env"
            private.parent.mkdir()
            private.write_text("TRADIER_ACCOUNT_ID=home-account\n")
            with patch.dict(os.environ, {}, clear=True), patch("trading_engine.credentials.Path.home", return_value=root):
                self.assertEqual(load_engine_environment(root / "new-checkout"), private)
                self.assertEqual(os.environ["TRADIER_ACCOUNT_ID"], "home-account")


if __name__ == "__main__":
    unittest.main()
