"""Run the CI fixture in the foreground and reap every child on exit."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time
import tomllib
from urllib.error import URLError
from urllib.request import urlopen

import redis

ROOT = Path(__file__).resolve().parents[2]


def allocate_ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--tidis-binary", type=Path, default=ROOT / "target/debug/tidis-server")
    parser.add_argument("--startup-timeout", type=float, default=120)
    parser.add_argument("--test-timeout", type=float, default=900)
    args = parser.parse_args()
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=False)
    children = []

    def start(name, command, cwd=work):
        with (work / f"{name}.log").open("w") as log:
            child = subprocess.Popen(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT)
        children.append((name, child))
        return child

    def wait_ready(name, probe):
        deadline = time.monotonic() + args.startup_timeout
        last_error = "not ready"
        while time.monotonic() < deadline:
            for child_name, child in children:
                if child.poll() is not None:
                    raise RuntimeError(f"{child_name} exited with {child.returncode} before {name} was ready")
            try:
                if probe():
                    print(f"{name} ready", flush=True)
                    return
            except (OSError, URLError, ValueError, redis.RedisError) as exc:
                last_error = str(exc)
            time.sleep(0.5)
        raise TimeoutError(f"{name} not ready within {args.startup_timeout}s: {last_error}")

    def http_json(url):
        with urlopen(url, timeout=1) as response:
            return json.load(response)

    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"received signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    exit_code = 1
    try:
        api_version = int(os.environ.get("TIKV_API_VERSION", "1"))
        config = ROOT / ".github/ci/tikv.toml"
        if tomllib.loads(config.read_text())["storage"]["api-version"] != api_version:
            raise ValueError("TIKV_API_VERSION does not match the checked-in TiKV config")
        pd_port, peer_port, kv_port, status_port, tidis_port, metrics_port = allocate_ports(6)
        pd_url = f"http://127.0.0.1:{pd_port}"
        (work / "pd.toml").write_text("[replication]\nmax-replicas = 1\n")
        start("pd", [
            str(args.bin_dir.resolve() / "pd-server"),
            "--name=ci", f"--data-dir={work / 'pd-data'}",
            f"--client-urls={pd_url}", f"--advertise-client-urls={pd_url}",
            f"--peer-urls=http://127.0.0.1:{peer_port}",
            f"--advertise-peer-urls=http://127.0.0.1:{peer_port}",
            f"--config={work / 'pd.toml'}",
        ])

        def pd_ready():
            members = http_json(f"{pd_url}/pd/api/v1/health")
            return bool(members) and all(member["health"] for member in members)

        wait_ready("PD", pd_ready)
        start("tikv", [
            str(args.bin_dir.resolve() / "tikv-server"),
            f"--addr=127.0.0.1:{kv_port}", f"--advertise-addr=127.0.0.1:{kv_port}",
            f"--status-addr=127.0.0.1:{status_port}", f"--pd=127.0.0.1:{pd_port}",
            f"--data-dir={work / 'tikv-data'}", f"--config={config}",
        ])
        wait_ready("TiKV registered in PD", lambda: any(
            store["store"]["state_name"] == "Up"
            for store in http_json(f"{pd_url}/pd/api/v1/stores").get("stores", [])
        ))
        tidis_config = (ROOT / "config.toml").read_text().replace(
            "[server]", "[server]\nmeta_key_number = 1", 1
        )
        for key, value in {
            "listen": "127.0.0.1", "port": tidis_port,
            "prometheus_listen": "127.0.0.1", "prometheus_port": metrics_port,
            "pd_addrs": f"127.0.0.1:{pd_port}", "log_file": str(work / "tidis-service.log"),
        }.items():
            tidis_config = re.sub(
                rf"^{key} = .*?$", f"{key} = {json.dumps(value)}", tidis_config, flags=re.MULTILINE
            )
        (work / "tidis.toml").write_text(tidis_config)
        start("tidis", [str(args.tidis_binary.resolve()), f"--config={work / 'tidis.toml'}"])
        client = redis.StrictRedis(
            host="127.0.0.1", port=tidis_port, socket_timeout=1, socket_connect_timeout=1
        )

        def tidis_ready():
            # PING alone cannot establish that TiKV can serve transactions.
            if not client.ping() or not client.set("__ci_readiness__", "ready"):
                return False
            if client.get("__ci_readiness__") != b"ready":
                return False
            return client.delete("__ci_readiness__") == 1

        wait_ready("Tidis PING and backend read/write", tidis_ready)
        e2e = start("e2e", [
            sys.executable, str(ROOT / ".github/ci/e2e_suite.py"), "--port", str(tidis_port),
        ], cwd=ROOT / "test")
        exit_code = e2e.wait(timeout=args.test_timeout)
        if exit_code:
            raise RuntimeError(f"E2E exited with {exit_code}")
        for name, child in children[:-1]:
            if child.poll() is not None:
                raise RuntimeError(f"{name} exited during E2E with {child.returncode}")
    except (Exception, KeyboardInterrupt) as exc:
        print(f"E2E fixture failed: {exc}", file=sys.stderr, flush=True)
        exit_code = 1
    finally:
        # Stop all children together, then bound the total graceful shutdown.
        for _, child in reversed(children):
            if child.poll() is None:
                child.terminate()
        deadline = time.monotonic() + 5
        for _, child in reversed(children):
            try:
                child.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for log in sorted(work.glob("*.log")):
            print(f"\n--- {log.name} ---", flush=True)
            lines = log.read_text(errors="replace").splitlines()
            # Keep the complete suite summary; bound noisy service output.
            print("\n".join(lines if log.name == "e2e.log" else lines[-80:]), flush=True)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
