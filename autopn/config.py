import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _candidate_paths(explicit: Optional[str] = None) -> List[Path]:
    """Return config paths to try, in priority order."""
    paths: List[Path] = []
    if explicit:
        paths.append(Path(explicit))
    env_path = os.environ.get("AUTOPN_CONFIG")
    if env_path:
        paths.append(Path(env_path))
    paths.append(PROJECT_ROOT / "config" / "autopn.yaml")
    paths.append(PROJECT_ROOT / "config" / "autopn.example.yaml")
    return paths


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """Load the YAML config, falling back to the example file when needed."""
    last_error: Optional[Exception] = None
    for candidate in _candidate_paths(path):
        try:
            if candidate.exists():
                with candidate.open("r", encoding="utf-8") as fh:
                    data = yaml.safe_load(fh) or {}
                data.setdefault("relations", [])
                return data
        except Exception as exc:
            last_error = exc
    hint = " → copy config/autopn.example.yaml to config/autopn.yaml"
    if last_error:
        raise RuntimeError(f"Failed to read config: {last_error}{hint}") from last_error
    raise FileNotFoundError(f"No config file found.{hint}")


def resolve_relation(config: Dict[str, Any], relation_id: Optional[str]) -> Dict[str, Any]:
    """Find a relation entry by id. If only one relation exists, it becomes the default."""
    relations = config.get("relations") or []
    if not relations:
        raise ValueError("No relations defined in config.")
    if relation_id:
        for rel in relations:
            if rel.get("id") == relation_id:
                return rel
        raise ValueError(f"Relation '{relation_id}' not found. Available ids: "
                         f"{', '.join(r.get('id') or '?' for r in relations)}")
    if len(relations) == 1:
        return relations[0]
    raise ValueError("Multiple relations configured. Pass --relation to select one.")


def expand_path(path_value: Optional[str]) -> Path:
    """Resolve paths relative to the project root for convenience."""
    if not path_value:
        raise ValueError("Missing path in config.")
    path = Path(os.path.expanduser(path_value))
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def owner_emails(config: Dict[str, Any]) -> List[str]:
    owner = config.get("owner") or {}
    return [e.lower() for e in (owner.get("emails") or [])]


def relation_emails(relation: Dict[str, Any]) -> List[str]:
    return [e.lower() for e in (relation.get("emails") or [])]


def relation_label(relation: Dict[str, Any]) -> str:
    return relation.get("name") or relation.get("id") or "Relation"


def owner_label(config: Dict[str, Any]) -> str:
    owner = config.get("owner") or {}
    return owner.get("name") or "Owner"


def openai_settings(config: Dict[str, Any]) -> Dict[str, Any]:
    defaults = {"model": "gpt-4o-mini", "temperature": 0.6, "sleep_between_requests": 0.25}
    data = config.get("openai") or {}
    return {
        "model": data.get("model", defaults["model"]),
        "temperature": data.get("temperature", defaults["temperature"]),
        "sleep_between_requests": data.get("sleep_between_requests", defaults["sleep_between_requests"]),
    }


def relation_context(relation: Dict[str, Any]) -> str:
    return relation.get("context_history") or ""


def relation_theme_keywords(relation: Dict[str, Any]) -> Dict[str, Any]:
    return relation.get("theme_keywords") or {}
