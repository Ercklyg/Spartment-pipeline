"""Compares the live PostgREST schema against the extraction contract."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from spartment.stage1_extract.contract import TABLES, TableSpec, select_columns


@dataclass(frozen=True)
class TablePlan:
    spec: TableSpec
    selected: tuple[str, ...]
    missing: tuple[str, ...]
    exposed: bool
    column_types: Mapping[str, str]

    @property
    def ok(self) -> bool:
        return self.exposed and not self.missing

    @property
    def reason(self) -> str | None:
        if not self.exposed:
            return "table is not exposed by the PostgREST schema"
        if self.missing:
            return f"missing required columns: {', '.join(self.missing)}"
        return None

    def manifest(self) -> dict[str, str]:
        return {name: self.column_types.get(name, "unknown") for name in self.selected}


def _schema_definitions(document: Mapping[str, Any]) -> Mapping[str, Any]:
    definitions = document.get("definitions")
    if isinstance(definitions, Mapping):
        return definitions
    components = document.get("components")
    if isinstance(components, Mapping):
        schemas = components.get("schemas")
        if isinstance(schemas, Mapping):
            return schemas
    return {}


def _property_types(properties: Any) -> dict[str, str]:
    if not isinstance(properties, Mapping):
        return {}
    result: dict[str, str] = {}
    for name, definition in properties.items():
        if isinstance(definition, Mapping):
            result[name] = str(definition.get("format") or definition.get("type") or "unknown")
        else:
            result[name] = "unknown"
    return result


def build_plans(document: Mapping[str, Any], include_extra: bool = True) -> list[TablePlan]:
    definitions = _schema_definitions(document)
    plans: list[TablePlan] = []

    for spec in TABLES:
        definition = definitions.get(spec.name)
        if not isinstance(definition, Mapping):
            plans.append(TablePlan(spec, (), spec.columns, False, {}))
            continue
        column_types = _property_types(definition.get("properties"))
        selected, missing = select_columns(spec, column_types.keys(), include_extra)
        plans.append(TablePlan(spec, selected, missing, True, column_types))

    return plans