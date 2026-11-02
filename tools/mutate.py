"""Tiny mutation check: apply each (file, old, new) mutant, run pytest, expect failure, restore."""

import json
import subprocess
import sys
from pathlib import Path

root = Path(sys.argv[1])
mutants = json.loads(Path(sys.argv[2]).read_text())
survivors = 0
for m in mutants:
    f = root / m["file"]
    orig = f.read_bytes()
    crlf = b"\r\n" in orig
    text = orig.decode("utf-8").replace("\r\n", "\n")  # anchors are written with \n
    assert text.count(m["old"]) == 1, f"mutant anchor not unique/missing: {m['old']!r}"
    mutated = text.replace(m["old"], m["new"])
    f.write_bytes((mutated.replace("\n", "\r\n") if crlf else mutated).encode("utf-8"))
    try:
        r = subprocess.run(
            ["uv", "run", "pytest", "-q", "-x", *m.get("tests", [])], cwd=root, capture_output=True, text=True
        )
    finally:
        f.write_bytes(orig)
    killed = r.returncode != 0
    survivors += not killed
    print(("KILLED  " if killed else "SURVIVED") + "  " + m["name"])
sys.exit(1 if survivors else 0)
