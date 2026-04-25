from pathlib import Path

from stock_platform.ops.health import health_checks_to_markdown, run_health_checks


def test_run_health_checks_reports_missing_items(tmp_path: Path):
    checks = run_health_checks(tmp_path)
    by_name = {check.name: check for check in checks}

    assert by_name["Project state"].ok is False
    assert by_name["Environment file"].ok is False
    assert by_name["Local Git repository"].ok is False


def test_run_health_checks_passes_existing_project_files(tmp_path: Path):
    (tmp_path / "PROJECT_STATE.md").write_text("# State", encoding="utf-8")
    (tmp_path / ".env").write_text("APP_ENV=testing", encoding="utf-8")
    (tmp_path / "logs").mkdir()
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "stock_platform.db").write_text("", encoding="utf-8")
    (tmp_path / "data" / "sample").mkdir()
    (tmp_path / "data" / "sample" / "fundamentals_annual_sample.csv").write_text(
        "symbol,fiscal_year\nRELIANCE.NS,2024\n",
        encoding="utf-8",
    )

    checks = run_health_checks(tmp_path)
    by_name = {check.name: check for check in checks}

    assert by_name["Project state"].ok is True
    assert by_name["Environment file"].ok is True
    assert by_name["Logs folder"].ok is True
    assert by_name["SQLite database"].ok is True
    assert by_name["Sample fundamentals"].ok is True


def test_health_checks_to_markdown_is_beginner_readable():
    markdown = health_checks_to_markdown(run_health_checks(Path("missing-test-root")))

    assert "# Local Health Check" in markdown
    assert "ACTION" in markdown
    assert "Next:" in markdown
