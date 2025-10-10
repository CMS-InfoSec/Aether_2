"""Lightweight YAML compatibility layer for environments without PyYAML."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, List, MutableMapping, Optional, Tuple, Union

Scalar = Union[str, int, float, bool, None]
YamlValue = Union[Scalar, "YamlMapping", "YamlSequence"]
YamlMapping = MutableMapping[str, YamlValue]
YamlSequence = List[YamlValue]


@dataclass
class _ParseState:
    lines: List[str]
    index: int = 0

    def has_more(self) -> bool:
        return self.index < len(self.lines)

    def current(self) -> Tuple[int, str]:
        raw = self.lines[self.index]
        indent = len(raw) - len(raw.lstrip(" "))
        return indent, raw.lstrip()

    def advance(self) -> None:
        self.index += 1


def _coerce_scalar(token: str) -> Scalar:
    if token == "":
        return ""
    lowered = token.lower()
    if lowered in {"null", "none", "~"}:
        return None
    if lowered in {"true", "false"}:
        return lowered == "true"
    if token.startswith("\"") and token.endswith("\"") and len(token) >= 2:
        return token[1:-1]
    if token.startswith("'") and token.endswith("'") and len(token) >= 2:
        return token[1:-1]
    if token == "{}":
        return {}
    if token == "[]":
        return []
    try:
        if any(ch in token for ch in (".", "e", "E")):
            return float(token)
        return int(token)
    except ValueError:
        return token


def _parse_block(
    state: _ParseState, indent: int, *, allow_sibling_at_indent: bool = False
) -> YamlValue:
    mapping: dict[str, YamlValue] = {}
    sequence: list[YamlValue] = []
    container: Optional[str] = None

    while state.has_more():
        current_indent, content = state.current()
        if content == "" or content.startswith("#"):
            state.advance()
            continue
        if current_indent < indent or (
            allow_sibling_at_indent
            and current_indent == indent
            and not content.startswith("-")
        ):
            break
        if content.startswith("-"):
            if container == "mapping":
                raise ValueError("Cannot mix mappings and sequences at the same level")
            container = "sequence"
            item_text = content[1:].strip()
            state.advance()
            if item_text == "":
                value = _parse_block(
                    state, current_indent + 2, allow_sibling_at_indent=True
                )
            elif ":" in item_text:
                key, rest = item_text.split(":", 1)
                key = key.strip()
                rest = rest.strip()
                if rest:
                    value = {key: _coerce_scalar(rest)}
                else:
                    nested = _parse_block(
                        state, current_indent + 2, allow_sibling_at_indent=True
                    )
                    value = {key: nested}
            else:
                value = _coerce_scalar(item_text)
            sequence.append(value)
            continue

        if container == "sequence":
            if not sequence or not isinstance(sequence[-1], dict):
                raise ValueError("Cannot mix sequences and mappings at the same level")
            if ":" not in content:
                raise ValueError(f"Malformed YAML line: {content!r}")
            key, rest = content.split(":", 1)
            key = key.strip()
            rest = rest.strip()
            state.advance()
            if rest:
                sequence[-1][key] = _coerce_scalar(rest)
            else:
                sequence[-1][key] = _parse_block(state, current_indent + 2)
            continue
        container = "mapping"
        if ":" not in content:
            raise ValueError(f"Malformed YAML line: {content!r}")
        key, rest = content.split(":", 1)
        key = key.strip()
        rest = rest.strip()
        state.advance()
        if rest:
            mapping[key] = _coerce_scalar(rest)
            continue
        mapping[key] = _parse_block(state, current_indent + 2)

    if container == "sequence":
        return sequence
    return mapping


def safe_load(stream: Union[str, bytes, Iterable[str]]) -> YamlValue:
    if hasattr(stream, "read"):
        text = stream.read()
    else:
        text = stream
    if isinstance(text, bytes):
        text = text.decode("utf-8")
    if isinstance(text, str):
        lines = text.splitlines()
    else:
        lines = list(text)
    state = _ParseState(lines)
    value = _parse_block(state, 0)
    return value


def safe_load_all(stream: Union[str, bytes, Iterable[str]]) -> Iterator[YamlValue]:
    if hasattr(stream, "read"):
        text = stream.read()
    else:
        text = stream
    if isinstance(text, bytes):
        text = text.decode("utf-8")
    documents = str(text).split("\n---")
    for document in documents:
        yield safe_load(document.splitlines())


def dump(data: YamlValue) -> str:
    if isinstance(data, dict):
        lines: List[str] = []
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                lines.append(f"{key}:")
                nested = dump(value)
                lines.extend(f"  {line}" for line in nested.splitlines())
            else:
                lines.append(f"{key}: {value}")
        return "\n".join(lines)
    if isinstance(data, list):
        lines = []
        for item in data:
            if isinstance(item, (dict, list)):
                nested = dump(item)
                nested_lines = nested.splitlines()
                if nested_lines:
                    lines.append(f"- {nested_lines[0]}")
                    lines.extend(f"  {line}" for line in nested_lines[1:])
                else:
                    lines.append("- {}")
            else:
                lines.append(f"- {item}")
        return "\n".join(lines)
    return str(data)


def safe_dump(data: YamlValue) -> str:
    return dump(data)


__all__ = ["safe_load", "safe_load_all", "safe_dump", "dump"]
