#!/usr/bin/env python3
"""Emit the union of host ports across stacks/*/docker-compose.yaml and
plugins/*/docker-compose.yaml (one port per line, sorted, deduplicated).

For demonstration purposes only.

Used by clean-ports.sh and make clean to know which host ports might be held
by diab stacks after a partial teardown — so we can verify they're released
or surface foreign holders.

Why regex over yaml.safe_load: compose files in this repo use anchors,
multi-line strings, and several stacks reference templated images that can
trip pyyaml. We only need the integer host ports, so a one-line regex is
both faster and more robust.
"""
import re
import sys
from pathlib import Path

PORT_PATTERN = re.compile(
    r'^\s*-\s*"?(?:[\d.]+:)?(\d{2,5}):\d+(?:/\w+)?"?\s*$',
    re.MULTILINE,
)


def extract(compose_path):
    try:
        text = compose_path.read_text(errors="ignore")
    except OSError:
        return set()
    return {int(m.group(1)) for m in PORT_PATTERN.finditer(text)}


def main():
    root = Path(__file__).resolve().parent.parent
    ports = set()
    for parent_name in ("stacks", "plugins"):
        parent = root / parent_name
        if not parent.is_dir():
            continue
        for compose in parent.rglob("docker-compose.yaml"):
            ports |= extract(compose)
    ports.add(4000)
    for p in sorted(ports):
        print(p)


if __name__ == "__main__":
    main()
