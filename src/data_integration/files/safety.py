from __future__ import annotations

import hashlib
import os
import re
import shutil
import time
from pathlib import Path
from string import Formatter
from typing import Any


WINDOWS_UNSAFE_CHARS = re.compile(r'[\\/:*?"<>|]')
REPEATED_UNDERSCORES = re.compile(r"_+")


class FileSafetyError(RuntimeError):
    pass


def is_xml_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() == ".xml"


def is_file_stable(path: Path, stability_seconds: int) -> bool:
    if stability_seconds <= 0:
        return path.is_file()
    if not path.is_file():
        return False
    stat = path.stat()
    return stat.st_size > 0 and (time.time() - stat.st_mtime) >= stability_seconds


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_verify(source: Path, destination: Path, expected_sha256: str | None = None) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        raise FileSafetyError(f"destination already exists: {destination}")
    shutil.copy2(source, destination)
    source_hash = expected_sha256 or sha256_file(source)
    destination_hash = sha256_file(destination)
    if destination_hash != source_hash:
        destination.unlink(missing_ok=True)
        raise FileSafetyError(f"copy verification failed for {source} -> {destination}")


def promote_file(staging_path: Path, target_path: Path, expected_sha256: str) -> None:
    if target_path.exists():
        raise FileSafetyError(f"target already exists: {target_path}")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if _same_drive(staging_path, target_path):
        os.replace(staging_path, target_path)
        if sha256_file(target_path) != expected_sha256:
            raise FileSafetyError(f"promoted file hash mismatch: {target_path}")
        return

    copy_verify(staging_path, target_path, expected_sha256)
    staging_path.unlink()


def move_to_quarantine(source: Path, quarantine_root: Path, run_id: str, reason: str) -> Path:
    safe_reason = sanitize_path_token(reason) or "unknown"
    destination_dir = quarantine_root / run_id / safe_reason
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = unique_destination(destination_dir / source.name)
    if source.exists():
        shutil.move(str(source), str(destination))
    return destination


def unique_destination(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    counter = 1
    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def render_safe_target(output_root: Path, template: str, variables: dict[str, Any]) -> Path:
    template_fields = [field for _, field, _, _ in Formatter().parse(template) if field]
    missing = sorted(set(template_fields) - set(variables))
    if missing:
        raise FileSafetyError(f"target template references unknown fields: {', '.join(missing)}")

    safe_variables = {key: sanitize_path_token(value) for key, value in variables.items()}
    rendered = template.format_map(safe_variables).replace("\\", "/")
    relative = Path(rendered)
    if relative.is_absolute() or ".." in relative.parts:
        raise FileSafetyError(f"unsafe target path rendered from template: {rendered}")

    root = output_root.resolve()
    target = (root / relative).resolve()
    if root != target and root not in target.parents:
        raise FileSafetyError(f"target path escapes output root: {target}")
    return target


def archive_destination(archive_dir: Path, run_id: str, source: Path) -> Path:
    return unique_destination(archive_dir / run_id / source.name)


def staging_destination(staging_dir: Path, run_id: str, rule_id: str, source: Path) -> Path:
    return unique_destination(staging_dir / run_id / sanitize_path_token(rule_id) / source.name)


def sanitize_path_token(value: Any) -> str:
    text = str(value).strip()
    text = WINDOWS_UNSAFE_CHARS.sub("_", text)
    text = text.replace("..", "_")
    text = REPEATED_UNDERSCORES.sub("_", text).strip(" ._")
    return text or "unknown"


def _same_drive(left: Path, right: Path) -> bool:
    left_drive = left.resolve().drive.lower()
    right_drive = right.resolve().drive.lower()
    return left_drive == right_drive
