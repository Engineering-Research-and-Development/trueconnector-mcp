"""Mutation check: apply each (file, old, new) mutant, run the unit tests, expect a failure.

Usage: uv run python tools/mutate.py <repo> <mutants.json> [workers]

Mutants run in parallel, each worker in its own temporary copy of src/ and tests/, so the working tree is never
modified (an interrupted run can't leave a mutant behind).
"""

import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

root = Path(sys.argv[1]).resolve()
mutants = json.loads(Path(sys.argv[2]).read_text())
workers = int(sys.argv[3]) if len(sys.argv) > 3 else min(8, os.cpu_count() or 1)


def text_of(path: Path) -> str:
    return path.read_bytes().decode("utf-8").replace("\r\n", "\n")  # anchors are written with \n


# Check every anchor first: a stale or ambiguous one would silently mutate the wrong line.
bad = [m["name"] for m in mutants if text_of(root / m["file"]).count(m["old"]) != 1]
if bad:
    sys.exit(f"mutant anchor missing or not unique: {bad}")

scratch = Path(tempfile.mkdtemp(prefix="mutants-"))
copies: queue.Queue[Path] = queue.Queue()
for i in range(workers):
    copy = scratch / str(i)
    for part in ("src", "tests"):
        shutil.copytree(root / part, copy / part, ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copy2(root / "pyproject.toml", copy)
    copies.put(copy)


def tests_fail(copy: Path, tests: list[str]) -> bool:
    r = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider", *tests],
        cwd=copy,
        env={**os.environ, "PYTHONPATH": str(copy / "src")},
        capture_output=True,
    )
    return r.returncode != 0


def killed(m: dict[str, str]) -> bool:
    copy = copies.get()
    f = copy / m["file"]
    orig = f.read_bytes()
    mutated = text_of(f).replace(m["old"], m["new"])
    f.write_bytes((mutated.replace("\n", "\r\n") if b"\r\n" in orig else mutated).encode("utf-8"))
    try:
        return tests_fail(copy, m.get("tests", []))
    finally:
        f.write_bytes(orig)
        copies.put(copy)


survivors = 0
try:
    # A broken copy would fail every run and report every mutant as killed, so the clean suite must pass first.
    if tests_fail(scratch / "0", []):
        sys.exit("the unmutated test suite fails in the scratch copy; fix that before trusting any result")
    with ThreadPoolExecutor(workers) as pool:
        for m, k in zip(mutants, pool.map(killed, mutants), strict=True):
            survivors += not k
            print(("KILLED  " if k else "SURVIVED") + "  " + m["name"], flush=True)
finally:
    shutil.rmtree(scratch, ignore_errors=True)
sys.exit(1 if survivors else 0)
