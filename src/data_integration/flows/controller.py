from __future__ import annotations

from pathlib import Path

from data_integration.config.loader import (
    DEFAULT_BULK_SOURCE_CONFIG_FILES,
    read_bulk_config_file,
    read_config_file,
)
from data_integration.config.schema import BulkIntegrationConfig, FlowConfigRef, IntegrationConfig


def project_root() -> Path:
    current = Path.cwd().resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    package_root = Path(__file__).resolve().parents[2]
    if (package_root / "pyproject.toml").is_file():
        return package_root
    raise RuntimeError("could not locate project root (missing pyproject.toml)")


def resolve_project_path(path: str | Path, root: Path | None = None) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate.resolve()
    return (root or project_root()) / candidate


def load_controller(controller_path: Path) -> BulkIntegrationConfig:
    if not controller_path.is_file():
        raise FileNotFoundError(f"bulk controller config not found: {controller_path}")
    return read_bulk_config_file(controller_path)


def iter_enabled_sources(
    controller: BulkIntegrationConfig,
    *,
    project_root_path: Path | None = None,
) -> list[tuple[str, Path]]:
    root = project_root_path or project_root()
    sources: list[tuple[str, Path]] = []
    for flow_config in controller.flow_configs:
        if not flow_config.enabled:
            continue
        config_path = resolve_source_config_path(flow_config, project_root_path=root)
        sources.append((flow_config.name, config_path))
    if not sources:
        raise ValueError("bulk controller has no enabled source flows")
    return sources


def resolve_source_config_path(flow_config: FlowConfigRef, *, project_root_path: Path) -> Path:
    if flow_config.config_path:
        return resolve_project_path(flow_config.config_path, project_root_path)
    if flow_config.config_variable:
        mapped = DEFAULT_BULK_SOURCE_CONFIG_FILES.get(flow_config.config_variable)
        if mapped is not None:
            return resolve_project_path(mapped, project_root_path)
    raise ValueError(
        f"flow {flow_config.name!r} has no config_path and no known config_variable mapping"
    )


def validate_source_configs(sources: list[tuple[str, Path]]) -> dict[str, IntegrationConfig]:
    validated: dict[str, IntegrationConfig] = {}
    for source_name, config_path in sources:
        if not config_path.is_file():
            raise FileNotFoundError(f"source config not found for {source_name}: {config_path}")
        config = read_config_file(config_path)
        _ensure_runtime_directories(config, source_name=source_name)
        validated[source_name] = config
    return validated


def _ensure_runtime_directories(config: IntegrationConfig, *, source_name: str) -> None:
    for source_dir in config.directories.source_dirs:
        if not source_dir.exists():
            raise FileNotFoundError(
                f"source directory does not exist for {source_name}: {source_dir}"
            )
    for label, directory in (
        ("archive_dir", config.directories.archive_dir),
        ("staging_dir", config.directories.staging_dir),
        ("output_root", config.directories.output_root),
        ("prepublish_dir", config.directories.prepublish_dir),
        ("quarantine_dir", config.directories.quarantine_dir),
    ):
        directory.mkdir(parents=True, exist_ok=True)
        if not directory.is_dir():
            raise NotADirectoryError(f"{label} is not a directory for {source_name}: {directory}")
