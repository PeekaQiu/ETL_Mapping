from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from data_integration.config.loader import (
    DEFAULT_CONTROLLER_PATH,
    DEFAULT_CONTROLLER_VAR,
    DEFAULT_SOURCE_CONFIGS,
    ensure_bulk_config_variable,
    ensure_config_variable,
    read_config_file,
)
from data_integration.config.guardrails import validate_config_for_runtime
from data_integration.config.schema import BulkIntegrationConfig, FlowConfigRef, IntegrationConfig
from data_integration.storage.db import open_repository


@dataclass(frozen=True)
class SourceConfigRef:
    source_name: str
    config_variable: str
    bootstrap_path: Path


@dataclass(frozen=True)
class CycleContext:
    root: Path
    controller: BulkIntegrationConfig
    sources: list[SourceConfigRef]


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


def resolve_bootstrap_path(
    config_variable: str,
    bootstrap_path: str | Path | None = None,
    *,
    project_root_path: Path | None = None,
) -> Path:
    root = project_root_path or project_root()
    if bootstrap_path is not None:
        return resolve_project_path(bootstrap_path, root)
    mapped = DEFAULT_SOURCE_CONFIGS.get(config_variable)
    if mapped is None:
        raise ValueError(
            f"Prefect Variable {config_variable!r} is missing and no bootstrap config path is known"
        )
    return resolve_project_path(mapped, root)


def load_controller_variable(
    *,
    variable_name: str = DEFAULT_CONTROLLER_VAR,
    bootstrap_path: str | Path = DEFAULT_CONTROLLER_PATH,
    project_root_path: Path | None = None,
) -> BulkIntegrationConfig:
    root = project_root_path or project_root()
    resolved_bootstrap = resolve_project_path(bootstrap_path, root)
    if not resolved_bootstrap.is_file():
        raise FileNotFoundError(f"bulk controller bootstrap config not found: {resolved_bootstrap}")
    return ensure_bulk_config_variable(variable_name, resolved_bootstrap)


def prepare_cycle(
    *,
    controller_variable: str = DEFAULT_CONTROLLER_VAR,
    controller_bootstrap_path: str | Path = DEFAULT_CONTROLLER_PATH,
    project_root_path: Path | None = None,
) -> CycleContext:
    root = project_root_path or project_root()
    bootstrap_path = resolve_project_path(controller_bootstrap_path, root)
    controller = load_controller_variable(
        variable_name=controller_variable,
        bootstrap_path=bootstrap_path,
        project_root_path=root,
    )
    sources = enabled_sources(controller, project_root_path=root)
    load_and_validate_sources(sources)
    return CycleContext(root=root, controller=controller, sources=sources)


def enabled_sources(
    controller: BulkIntegrationConfig,
    *,
    project_root_path: Path | None = None,
) -> list[SourceConfigRef]:
    root = project_root_path or project_root()
    sources: list[SourceConfigRef] = []
    for flow_config in controller.flow_configs:
        if not flow_config.enabled:
            continue
        config_variable = _source_config_variable(flow_config)
        bootstrap_path = _source_bootstrap_path(flow_config, project_root_path=root)
        sources.append(
            SourceConfigRef(
                source_name=flow_config.name,
                config_variable=config_variable,
                bootstrap_path=bootstrap_path,
            )
        )
    if not sources:
        raise ValueError("bulk controller has no enabled source flows")
    return sources


def _source_config_variable(flow_config: FlowConfigRef) -> str:
    if flow_config.config_variable:
        return flow_config.config_variable
    if flow_config.config_path:
        raise ValueError(
            f"flow {flow_config.name!r} uses config_path; runtime requires config_variable"
        )
    raise ValueError(f"flow {flow_config.name!r} has no config_variable")


def _source_bootstrap_path(flow_config: FlowConfigRef, *, project_root_path: Path) -> Path:
    if flow_config.config_path:
        return resolve_project_path(flow_config.config_path, project_root_path)
    if flow_config.config_variable:
        mapped = DEFAULT_SOURCE_CONFIGS.get(flow_config.config_variable)
        if mapped is not None:
            return resolve_project_path(mapped, project_root_path)
    raise ValueError(
        f"flow {flow_config.name!r} has no bootstrap config_path mapping for {flow_config.config_variable!r}"
    )


def load_and_validate_sources(refs: list[SourceConfigRef]) -> dict[str, IntegrationConfig]:
    validated: dict[str, IntegrationConfig] = {}
    for ref in refs:
        if not ref.bootstrap_path.is_file():
            raise FileNotFoundError(
                f"source bootstrap config not found for {ref.source_name}: {ref.bootstrap_path}"
            )
        bootstrap_config = read_config_file(ref.bootstrap_path)
        config = ensure_config_variable(
            ref.config_variable,
            ref.bootstrap_path,
            bootstrap_config=bootstrap_config,
        )
        repository = open_repository(config.directories.sqlite_path)
        validate_config_for_runtime(
            config,
            bootstrap_config=bootstrap_config,
            repository=repository,
        )
        _ensure_dirs(config, source_name=ref.source_name)
        validated[ref.source_name] = config
    return validated


def _ensure_dirs(config: IntegrationConfig, *, source_name: str) -> None:
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
