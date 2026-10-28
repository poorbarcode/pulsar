#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import json
import os
from pathlib import Path
import platform
import shlex
import subprocess
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
TEMP = Path(os.environ["RUNNER_TEMP"])
OUT = TEMP / "request-frame-pr-comparison"
OUT.mkdir(exist_ok=True)
LOGS = OUT / "logs"
LOGS.mkdir(exist_ok=True)
REFS = {
    "without-pr": "17166c820bf011ce7dd8b2b6795cc9c62e734af7",
    "with-pr": "8f7ac6e0cd92af0aecea130c0f6563bec56742ac",
}
PHASE = sys.argv[1]
MANIFEST_PATH = OUT / "manifest.json"
GRADLE = ["./gradlew", "--console=plain", "--max-workers=3", "-PtestRetryCount=0"]


def run(command, name, cwd=ROOT, check=True):
    print(time.strftime("%H:%M:%S"), name, shlex.join(map(str, command)), flush=True)
    started = time.time()
    with (LOGS / (name + ".log")).open("w") as log:
        result = subprocess.run(list(map(str, command)), cwd=cwd, stdout=log, stderr=subprocess.STDOUT)
    print(name, "exit", result.returncode, "seconds", round(time.time() - started, 1), flush=True)
    if check and result.returncode:
        print((LOGS / (name + ".log")).read_text()[-12000:], flush=True)
        raise RuntimeError("Failed: " + name)
    return result.returncode


def capture(command, path, cwd=ROOT):
    result = subprocess.run(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    path.write_text(result.stdout)
    return result.stdout


manifest = json.loads(MANIFEST_PATH.read_text()) if MANIFEST_PATH.exists() else {
    "sourceRefs": REFS, "checkouts": {}, "runs": [],
    "question": "Without this PR versus the complete current PR",
    "hostScope": "One native Linux amd64 GitHub-hosted VM; host tuning limitations are recorded.",
}
assert manifest["sourceRefs"] == REFS


def save_manifest():
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))


checkouts = {variant: TEMP / ("pr-comparison-" + variant) for variant in REFS}
if PHASE == "prepare":
    assert platform.system() == "Linux" and platform.machine() == "x86_64"
    capture(["uname", "-a"], OUT / "uname.txt")
    capture(["lscpu", "--json"], OUT / "lscpu.json")
    capture(["docker", "info", "--format", "{{json .}}"], OUT / "docker-info.json")
    capture(["df", "-h"], OUT / "disk-before.txt")
    run(["tests/performance/environment/scripts/configure-perf-test-environment.sh", "validate"],
        "host-validation", check=False)
    run(["git", "diff", "--exit-code", REFS["without-pr"], REFS["with-pr"], "--",
         "tests/performance", "docker/pulsar/Dockerfile", "pulsar-client", "pulsar-client-api"],
        "verify-identical-harness-and-client")
    for variant, ref in REFS.items():
        checkout = checkouts[variant]
        run(["git", "worktree", "add", "--detach", str(checkout), ref], "checkout-" + variant)
        manifest["checkouts"][variant] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=checkout, text=True).strip()
        tasks = [":tests:java-test-image:dockerBuild", ":tests:performance:tools:installDist",
                 ":tests:performance:launcher:classes"]
        run(GRADLE + tasks + ["-Pdocker.tag=pr27110-" + variant], "build-" + variant, cwd=checkout)
        image = "apachepulsar/pulsar:pr27110-" + variant
        capture(["docker", "image", "inspect", image], OUT / ("image-" + variant + ".json"))
        run(["docker", "run", "--rm", "--entrypoint", "sh", image, "-c",
             "uname -m; cat /etc/os-release; java -version; printenv LD_PRELOAD; ls -l /usr/lib/libmimalloc*"],
            "runtime-" + variant)
        save_manifest()

    # Complete every image build before measuring either revision.
    for variant, checkout in checkouts.items():
        run(["./gradlew", "--stop"], "stop-daemon-" + variant, cwd=checkout)
    save_manifest()
    sys.exit(0)

reports = OUT / "reports"
common = ["--scenario", "tests/performance/scenarios/iot-telemetry-base.yaml",
          "--extends", "configs/iot-telemetry-low-mem", "--extends", "configs/bookie-journal-tmpfs"]
settings = {
    "workloads.iotTelemetry.rate": 20000,
    "metrics.intervalSeconds": 1,
    "workloads.iotTelemetry.warmup.seconds": 20,
    "workloads.iotTelemetry.warmup.messages": 0,
    "workloads.iotTelemetry.measurement.seconds": 60,
    "workloads.iotTelemetry.measurement.messages": 0,
    "workloads.iotTelemetry.timeoutSeconds": 600,
    "workloads.iotTelemetry.devices.count": 10000,
    "workloads.iotTelemetry.gateways.count": 10,
    "workloads.iotTelemetry.gateways.producer.ioThreads": 4,
    "workloads.iotTelemetry.gateways.producer.listenerThreads": 4,
    "workloads.iotTelemetry.gateways.producer.maxOutstanding": 1024,
    "workloads.iotTelemetry.gateways.producer.precreate": "true",
    "workloads.iotTelemetry.gateways.producer.batchingEnabled": "false",
    "workloads.iotTelemetry.topics.count": 8,
    "workloads.iotTelemetry.applications.count": 1,
    "workloads.iotTelemetry.applications.podsPerApplication": 1,
    "workloads.iotTelemetry.applications.client.ioThreads": 2,
    "workloads.iotTelemetry.applications.client.listenerThreads": 2,
    "cluster.brokers.env.numIOThreads": "4",
    "cluster.brokers.env.maxMessagePublishBufferSizeInMB": "256",
    "cluster.brokers.env.managedLedgerCacheCopyEntries": "false",
}
scenarios = {
    "fixed-small": {"workloads.iotTelemetry.payload.size": 128,
                         "cluster.brokers.env.inProgressRequestFrameMemoryLimitEnabled": "false"},
    "fixed-large": {"workloads.iotTelemetry.payload.size": 131072,
                     "cluster.brokers.env.inProgressRequestFrameMemoryLimitEnabled": "true",
                     "workloads.iotTelemetry.gateways.producer.maxOutstanding": 128,
                     "workloads.iotTelemetry.rate": 500},
}
scenarios["max-small"] = scenarios["fixed-small"] | {
    "workloads.iotTelemetry.rate": 0,
    "workloads.iotTelemetry.warmup.seconds": 0,
    "workloads.iotTelemetry.warmup.messages": 500000,
    "workloads.iotTelemetry.measurement.messages": 3000000,
}
scenarios["max-large"] = scenarios["fixed-large"] | {
    "workloads.iotTelemetry.rate": 0,
    "workloads.iotTelemetry.warmup.seconds": 0,
    "workloads.iotTelemetry.warmup.messages": 20000,
    "workloads.iotTelemetry.measurement.messages": 100000,
}
assert PHASE in scenarios, PHASE
run(GRADLE + [":tests:performance:metrics:up"], "metrics-up", cwd=checkouts["with-pr"])
try:
    for scenario, overrides in scenarios.items():
        if scenario != PHASE:
            continue
        for repetition in range(3):
            order = ["without-pr", "with-pr"] if repetition % 2 == 0 else ["with-pr", "without-pr"]
            for variant in order:
                name = f"{scenario}-{variant}-r{repetition}"
                args = common + ["--name", name]
                for key, value in (settings | overrides).items():
                    args += ["--set", f"{key}={value}"]
                code = run(GRADLE + [":tests:performance:launcher:run", "-Pdocker.tag=pr27110-" + variant,
                           "-Pperformance.reportsDir=" + str(reports), "-Pperformance.keepLauncherLog",
                           "--args=" + shlex.join(args)], name, cwd=checkouts[variant], check=False)
                entry = {"scenario": scenario, "variant": variant, "repetition": repetition,
                         "exitCode": code, "settings": settings | overrides}
                manifest["runs"].append(entry)
                save_manifest()
                if code:
                    raise RuntimeError("Invalid scenario run: " + name)
                found = [p.parent for p in reports.rglob("run-info.json") if p.parent.parent.name == name]
                assert len(found) == 1, found
                directory = found[0]
                info = json.loads((directory / "run-info.json").read_text())
                assert info["git.commit.id"] == REFS[variant] and not info["git.dirty"], info
                gateway = json.loads((directory / "gateways/gateways-summary.json").read_text())
                applications = list((directory / "applications").rglob("application-summary.json"))
                assert len(applications) == 1
                application = json.loads(applications[0].read_text())
                assert application["uniqueMessages"] == gateway["sent"], application
                assert application["orderingViolations"] == 0 and application["invalidMessages"] == 0, application
                entry.update({"valid": True, "report": str(directory.relative_to(OUT)),
                              "sent": gateway["sent"], "messagesPerSecond": gateway["messagesPerSecond"],
                              "measurementMessages": gateway["measurementMessages"],
                              "measurementSeconds": gateway["measurementElapsedSeconds"],
                              "duplicates": application["duplicates"]})
                save_manifest()
                print("VALID", name, "msg/s", round(gateway["messagesPerSecond"], 1), flush=True)
                # Persist sampled broker memory and pending-request evidence before the ephemeral metrics stack stops.
                for metadata in reports.rglob("metrics.json"):
                    destination = metadata.parent / "memory-metrics.json"
                    if destination.exists():
                        continue
                    data = json.loads(metadata.read_text())
                    selector = data["selector"]
                    query = '{__name__=~"jvm_memory_direct_.*|jvm_memory_bytes_used|jvm_gc_.*|process_resident_memory_bytes|process_cpu_seconds_total|pulsar_broker_publish_in_progress_buffer_memory_usage.*|pulsar_.*pending.*",' + selector[1:]
                    params = urllib.parse.urlencode({"query": query, "start": data["startEpochMs"] / 1000,
                        "end": data["endEpochMs"] / 1000, "step": data["intervalSeconds"]})
                    url = data["victoriaMetrics"]["prometheusApi"] + "query_range?" + params
                    with urllib.request.urlopen(url, timeout=30) as response:
                        destination.write_bytes(response.read())
finally:
    run(GRADLE + [":tests:performance:metrics:down"], "metrics-down", cwd=checkouts["with-pr"], check=False)
    capture(["df", "-h"], OUT / "disk-after.txt")
    save_manifest()
