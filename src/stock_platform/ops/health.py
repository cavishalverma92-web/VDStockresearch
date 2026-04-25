"""Local health checks for Phase 6 daily use."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

from stock_platform.config import ROOT_DIR, get_settings


@dataclass(frozen=True)
class HealthCheck:
    """One operational health-check result."""

    name: str
    ok: bool
    detail: str
    action: str = ""


def run_health_checks(root: Path = ROOT_DIR) -> list[HealthCheck]:
    """Check local setup without modifying files."""
    settings = get_settings()
    checks = [
        _exists("Project state", root / "PROJECT_STATE.md", "Keep this file updated after work."),
        _exists("Environment file", root / ".env", "Copy .env.example to .env."),
        _exists(
            "Sample fundamentals",
            _resolve_path(root, settings.fundamentals_csv_path),
            "Restore sample CSV.",
        ),
        _exists(
            "SQLite database", root / "data/stock_platform.db", "Run the app once to create it."
        ),
        _exists("Logs folder", root / "logs", "Create logs folder."),
        _git_repo(root),
        _git_identity(root, "user.name"),
        _git_identity(root, "user.email"),
    ]
    return checks


def health_checks_to_markdown(checks: list[HealthCheck]) -> str:
    """Render checks as beginner-friendly Markdown."""
    lines = ["# Local Health Check", ""]
    for check in checks:
        status = "PASS" if check.ok else "ACTION"
        lines.append(f"- **{status} - {check.name}:** {check.detail}")
        if check.action:
            lines.append(f"  - Next: {check.action}")
    return "\n".join(lines)


def _exists(name: str, path: Path, action: str) -> HealthCheck:
    exists = path.exists()
    return HealthCheck(
        name=name,
        ok=exists,
        detail=str(path) if exists else f"Missing: {path}",
        action="" if exists else action,
    )


def _resolve_path(root: Path, value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        return root / path
    try:
        relative = path.relative_to(ROOT_DIR)
    except ValueError:
        return path
    return root / relative


def _git_repo(root: Path) -> HealthCheck:
    exists = (root / ".git").exists()
    return HealthCheck(
        name="Local Git repository",
        ok=exists,
        detail="Initialized" if exists else "Not initialized",
        action="" if exists else "Run: git init",
    )


def _git_identity(root: Path, key: str) -> HealthCheck:
    value = _git_config(root, key)
    return HealthCheck(
        name=f"Git {key}",
        ok=bool(value),
        detail=value or "Not configured",
        action="" if value else f'Run: git config {key} "YOUR VALUE"',
    )


def _git_config(root: Path, key: str) -> str:
    try:
        result = subprocess.run(
            ["git", "config", key],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout.strip()


if __name__ == "__main__":
    print(health_checks_to_markdown(run_health_checks()))
