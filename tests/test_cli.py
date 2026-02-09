import unittest
from unittest.mock import patch

from awss.app import main


class TestCliDispatch(unittest.TestCase):
    def test_generate_config_subcommand_dispatches(self) -> None:
        with patch("awss.app.generate_config_main", return_value=7) as generate:
            code = main(["generate-config", "--sso-session", "corp"])

        self.assertEqual(code, 7)
        generate.assert_called_once_with(["--sso-session", "corp"])

    def test_default_cli_runs_tui(self) -> None:
        with patch("awss.app.S3Browser") as browser_cls:
            instance = browser_cls.return_value
            code = main(["--profiles", "dev,prod", "--region", "us-west-2"])

        browser_cls.assert_called_once_with(profiles=["dev", "prod"], region="us-west-2")
        instance.run.assert_called_once_with()
        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
