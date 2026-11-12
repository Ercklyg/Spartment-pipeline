"""Run all tests against a short-lived, loopback-only PostgreSQL cluster.

Usage: python tools/test_stage3.py --pg-bin "C:/Program Files/PostgreSQL/18/bin"
Never connects to or modifies the user's normal PostgreSQL database.
"""
from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-bin", type=Path, required=True)
    parser.add_argument("--pattern", default="test*.py", help="Optional focused unittest filename pattern")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    temporary_root = root / "tmp"
    temporary_root.mkdir(exist_ok=True)
    folder = Path(tempfile.mkdtemp(prefix="stage3-verification-", dir=temporary_root)).resolve()
    if not folder.is_relative_to(temporary_root.resolve()):
        raise RuntimeError("Test folder escaped the workspace tmp directory")
    data = folder / "data"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    def run(tool, *arguments):
        return subprocess.run([str(args.pg_bin / tool), *map(str, arguments)], check=True,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=120)
    started = False
    try:
        run("initdb", "-D", data, "-U", "stage3_test", "--auth=trust", "--encoding=UTF8", "--locale=C")
        run("pg_ctl", "-D", data, "-l", folder / "postgres.log", "-o", f"-p {port} -h 127.0.0.1", "-w", "start")
        started = True
        run("createdb", "-h", "127.0.0.1", "-p", port, "-U", "stage3_test", "stage3_tests")
        env = dict(os.environ)
        env["SPARTMENT_TEST_DSN"] = f"host=127.0.0.1 port={port} user=stage3_test dbname=stage3_tests"
        env["SPARTMENT_PG_BIN"] = str(args.pg_bin)
        print("Testing with an isolated, disposable PostgreSQL instance.", flush=True)
        return subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", args.pattern, "-v"],
                              cwd=root, env=env).returncode
    finally:
        if data.exists():
            # Also check after a partially successful startup; pg_ctl may have
            # launched PostgreSQL even when its own acknowledgement timed out.
            status = subprocess.run([str(args.pg_bin / "pg_ctl"), "-D", str(data), "status"],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if status.returncode == 0:
                run("pg_ctl", "-D", data, "-m", "fast", "-w", "stop")
        # Exact resolved test folder was checked above; no computed external path.
        shutil.rmtree(folder)


if __name__ == "__main__":
    raise SystemExit(main())
