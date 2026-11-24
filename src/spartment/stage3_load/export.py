"""Preserve reconciled analytical CSVs after confirmed publication."""
from __future__ import annotations

import shutil
from pathlib import Path
from uuid import uuid4

from .contract import OUTPUT_TABLES
from .models import LoadError, Package, atomic_json
from .package import sha256


def export_csv(root: Path, package: Package, result: dict) -> Path:
    """Publish all tables and their manifest together on the same filesystem."""
    parent = root / "loaded" / "exports"
    parent.mkdir(parents=True, exist_ok=True)
    destination = parent / result["run_id"]
    if destination.exists():
        raise LoadError("EXPORT_EXISTS", "Export folder already exists")
    temporary = parent / f".{result['run_id']}-{uuid4().hex}.tmp"
    temporary.mkdir()
    try:
        tables = {}
        for table in OUTPUT_TABLES:
            filename = f"{table}.csv"
            target = temporary / filename
            shutil.copyfile(package.folder / filename, target)
            checksum = sha256(target)
            if checksum != package.handover["output_checksums"][table]:
                raise LoadError("EXPORT_CHECKSUM", "Export differs from the validated input")
            tables[table] = dict(file=filename, row_count=package.counts[table], sha256=checksum)
        atomic_json(temporary / "manifest.json", {
            "run_id": result["run_id"], "batch_id": result["batch_id"],
            "transformation_run_id": result["transformation_run_id"],
            "published_at": result["published_at"], "tables": tables,
        })
        temporary.rename(destination)
    finally:
        if temporary.exists():
            if not temporary.resolve().is_relative_to(parent.resolve()) or temporary.resolve() == parent.resolve():
                raise LoadError("EXPORT_PATH", "Temporary export escaped the export directory")
            shutil.rmtree(temporary)
    return destination
