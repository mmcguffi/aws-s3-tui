import unittest
from unittest.mock import patch

from awss.app import _run_aws_s3_command, _run_login_command, main


class TestCliDispatch(unittest.TestCase):
    def test_generate_config_subcommand_dispatches(self) -> None:
        with patch("awss.app.generate_config_main", return_value=7) as generate:
            code = main(["generate-config", "--sso-session", "corp"])

        self.assertEqual(code, 7)
        generate.assert_called_once_with(["--sso-session", "corp"])

    def test_default_cli_runs_tui(self) -> None:
        with patch("awss.app._run_browser_command", return_value=0) as run_browser:
            code = main(["--profiles", "dev,prod", "--region", "us-west-2"])

        run_browser.assert_called_once_with(
            ["dev", "prod"],
            "us-west-2",
            initial_path=None,
            startup_force_refresh=False,
        )
        self.assertEqual(code, 0)

    def test_path_shortcut_opens_browser_to_path(self) -> None:
        with patch("awss.app._run_browser_command", return_value=0) as run_browser:
            code = main(["bucket-a/prefix"])

        run_browser.assert_called_once_with(
            None,
            None,
            initial_path="s3://bucket-a/prefix",
            startup_force_refresh=False,
        )
        self.assertEqual(code, 0)

    def test_s3_uri_shortcut_opens_browser_to_exact_path(self) -> None:
        with patch("awss.app._run_browser_command", return_value=0) as run_browser:
            code = main(["s3://bucket-a/prefix"])

        run_browser.assert_called_once_with(
            None,
            None,
            initial_path="s3://bucket-a/prefix",
            startup_force_refresh=False,
        )
        self.assertEqual(code, 0)

    def test_path_shortcut_allows_browse_options(self) -> None:
        with patch("awss.app._run_browser_command", return_value=0) as run_browser:
            code = main(["bucket-a/prefix", "--profiles", "dev"])

        run_browser.assert_called_once_with(
            ["dev"],
            None,
            initial_path="s3://bucket-a/prefix",
            startup_force_refresh=False,
        )
        self.assertEqual(code, 0)

    def test_reindex_forces_startup_refresh(self) -> None:
        with patch("awss.app._run_browser_command", return_value=0) as run_browser:
            code = main(["reindex", "bucket-a/data"])

        run_browser.assert_called_once_with(
            None,
            None,
            initial_path="s3://bucket-a/data",
            startup_force_refresh=True,
        )
        self.assertEqual(code, 0)

    def test_ls_dispatches_to_aws_wrapper(self) -> None:
        with patch("awss.app._run_aws_s3_command", return_value=0) as run_cmd:
            code = main(["ls", "bucket-a/prefix"])

        run_cmd.assert_called_once_with(
            "ls",
            ["s3://bucket-a/prefix"],
            [],
            dry_run=False,
        )
        self.assertEqual(code, 0)

    def test_ls_passes_through_aws_options(self) -> None:
        with patch("awss.app._run_aws_s3_command", return_value=0) as run_cmd:
            code = main(["ls", "bucket-a/prefix", "--recursive", "--profile", "dev"])

        run_cmd.assert_called_once_with(
            "ls",
            ["s3://bucket-a/prefix"],
            ["--recursive", "--profile", "dev"],
            dry_run=False,
        )
        self.assertEqual(code, 0)

    def test_cp_dispatches_and_normalizes_paths(self) -> None:
        with patch("awss.app._run_aws_s3_command", return_value=0) as run_cmd:
            code = main(["cp", "bucket-a/key.txt", ".", "--dry-run"])

        run_cmd.assert_called_once_with(
            "cp",
            ["s3://bucket-a/key.txt", "."],
            [],
            dry_run=True,
        )
        self.assertEqual(code, 0)

    def test_sync_normalizes_both_s3_paths(self) -> None:
        with patch("awss.app._run_aws_s3_command", return_value=0) as run_cmd:
            code = main(["sync", "bucket-a/src", "bucket-b/dst"])

        run_cmd.assert_called_once_with(
            "sync",
            ["s3://bucket-a/src", "s3://bucket-b/dst"],
            [],
            dry_run=False,
        )
        self.assertEqual(code, 0)

    def test_login_dispatches(self) -> None:
        with patch("awss.app._run_login_command", return_value=0) as login_cmd:
            code = main(["login", "--profiles", "dev,prod"])

        login_cmd.assert_called_once_with(["dev", "prod"])
        self.assertEqual(code, 0)


class TestAwsWrapper(unittest.TestCase):
    def test_ls_adds_human_readable(self) -> None:
        with patch("awss.app.subprocess.run") as run_mock:
            run_mock.return_value.returncode = 0
            code = _run_aws_s3_command("ls", ["s3://bucket-a"], [])

        run_mock.assert_called_once_with(
            ["aws", "s3", "ls", "s3://bucket-a", "--human-readable"]
        )
        self.assertEqual(code, 0)

    def test_ls_dry_run_prints_command(self) -> None:
        with patch("awss.app.subprocess.run") as run_mock:
            with patch("builtins.print") as print_mock:
                code = _run_aws_s3_command("ls", ["s3://bucket-a"], [], dry_run=True)

        run_mock.assert_not_called()
        print_mock.assert_called()
        self.assertEqual(code, 0)

    def test_cp_adds_dryrun(self) -> None:
        with patch("awss.app.subprocess.run") as run_mock:
            run_mock.return_value.returncode = 0
            code = _run_aws_s3_command(
                "cp",
                ["s3://bucket-a/key.txt", "."],
                [],
                dry_run=True,
            )

        run_mock.assert_called_once_with(
            ["aws", "s3", "cp", "s3://bucket-a/key.txt", ".", "--dryrun"]
        )
        self.assertEqual(code, 0)


class TestLoginWrapper(unittest.TestCase):
    def test_login_wrapper_runs_targets(self) -> None:
        with patch("awss.app.S3Service") as service_cls:
            service = service_cls.return_value
            service.sso_login_targets.return_value = ["dev", "prod"]
            with patch("awss.app._run_sso_login", side_effect=[0, 0]) as run_login:
                with patch("builtins.print"):
                    code = _run_login_command(["dev", "prod"])

        service_cls.assert_called_once_with(profiles=["dev", "prod"])
        run_login.assert_any_call("dev")
        run_login.assert_any_call("prod")
        self.assertEqual(code, 0)

    def test_login_wrapper_handles_no_targets(self) -> None:
        with patch("awss.app.S3Service") as service_cls:
            service = service_cls.return_value
            service.sso_login_targets.return_value = []
            with patch("awss.app._run_sso_login") as run_login:
                with patch("builtins.print"):
                    code = _run_login_command(None)

        run_login.assert_not_called()
        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
