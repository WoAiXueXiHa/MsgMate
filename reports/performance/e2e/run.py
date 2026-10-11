"""Run fixed-rate groups against the dedicated msgmate-e2e Compose project."""
import argparse
import json
import pathlib
import subprocess
import time

parser = argparse.ArgumentParser()
parser.add_argument("--output-dir", default="/tmp/msgmate-e2e")
args = parser.parse_args()
output = pathlib.Path(args.output_dir)
output.mkdir(parents=True, exist_ok=True)


def docker(*arguments, check=True):
    return subprocess.run(
        ["docker", *arguments], capture_output=True, text=True, check=check
    )


# Refuse to touch containers outside this dedicated Compose project.
for service in ["runtime", "mysql", "redis", "kafka", "zookeeper"]:
    label = docker(
        "inspect", "--format",
        '{{index .Config.Labels "com.docker.compose.project"}}',
        f"msgmate-e2e-{service}-1",
    ).stdout.strip()
    if label != "msgmate-e2e":
        raise RuntimeError("unexpected container ownership")

for mode in ["mysql", "kafka"]:
    for rate in [20, 50, 100]:
        docker("exec", "msgmate-e2e-runtime-1", "pkill", "-TERM", "bench", check=False)
        # The simulated channel takes 10ms; allow graceful joining before reset.
        time.sleep(3)
        still_running = docker(
            "exec", "msgmate-e2e-runtime-1", "pgrep", "bench", check=False
        )
        if still_running.returncode == 0:
            raise RuntimeError("previous consumer is still running; refusing reset")
        tables = [
            "t_msg_record", "t_msg_queue_low", "t_msg_queue_middle",
            "t_msg_queue_high", "t_msg_queue_retry", "t_msg_tmp_queue_timer",
        ]
        sql = ";".join("TRUNCATE TABLE " + table for table in tables)
        docker(
            "exec", "msgmate-e2e-mysql-1", "mysql", "-uroot", "-pe2e-only",
            "msgmate_e2e_test", "-e", sql,
        )
        docker("exec", "msgmate-e2e-redis-1", "redis-cli", "FLUSHDB")
        docker(
            "exec", "-d", "msgmate-e2e-runtime-1", "sh", "-c",
            f"exec /work/bench -config /work/{mode}.toml "
            f"> /work/{mode}-{rate}-server.log 2>&1",
        )
        for attempt in range(60):
            ready = docker(
                "exec", "msgmate-e2e-runtime-1", "wget", "-qO-",
                "http://runtime:18081/bench/stats", check=False,
            )
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("server unavailable: " + mode)
        print("START", mode, rate, flush=True)
        result = docker(
            "exec", "msgmate-e2e-runtime-1", "/work/bench", "-action", "load",
            "-rate", str(rate), "-seconds", "30",
        )
        (output / f"{mode}-{rate}.jsonl").write_text(result.stdout)
        print(result.stdout, flush=True)
        final = json.loads(result.stdout.strip().splitlines()[-1])
        stats = final["stats"]
        if not (
            final["issued"] == final["accepted"] == stats["total"]
            == stats["success"] == stats["calls"]
            and final["business_errors"] == final["transport_errors"]
            == stats["pending"] == stats["failed"] == 0
        ):
            raise RuntimeError("final counts failed reconciliation")
