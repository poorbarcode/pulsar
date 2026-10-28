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
import shutil
import subprocess
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
TEMP = Path(os.environ["RUNNER_TEMP"])
OUT = TEMP / "request-frame-results"
OUT.mkdir(exist_ok=True)
LOGS = OUT / "logs"
LOGS.mkdir(exist_ok=True)
REFS = {
    "base": "17166c820b",
    "before": "81730ae9f22ceecf542e93388b0b4bd656e24adc",
    "after": "539b4c2ccd2663f28f230c9920da60e0d261e1a3",
}
BENCHMARK = "microbench/src/main/java/org/apache/pulsar/broker/service/RequestFrameMemoryControllerBenchmark.java"
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


assert platform.system() == "Linux" and platform.machine() == "x86_64", platform.uname()
capture(["uname", "-a"], OUT / "uname.txt")
capture(["lscpu", "--json"], OUT / "lscpu.json")
capture(["docker", "info", "--format", "{{json .}}"], OUT / "docker-info.json")
capture(["df", "-h"], OUT / "disk-before.txt")
run(["tests/performance/environment/scripts/configure-perf-test-environment.sh", "validate"],
    "host-validation", check=False)
manifest = {"sourceRefs": REFS, "checkouts": {}, "jmh": [], "runs": [],
            "hostScope": "GitHub-hosted native Linux amd64 VM; frequency and noisy neighbours are uncontrolled."}


def save_manifest():
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2))


checkouts = {}
for variant, ref in REFS.items():
    checkout = TEMP / ("request-frame-" + variant)
    run(["git", "worktree", "add", "--detach", str(checkout), ref], "checkout-" + variant)
    checkouts[variant] = checkout
    if variant == "before":
        destination = checkout / BENCHMARK
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / BENCHMARK, destination)
        run(["git", "add", BENCHMARK], "benchmark-add", cwd=checkout)
        run(["git", "-c", "user.name=Performance harness", "-c", "user.email=benchmark@localhost",
             "commit", "-m", "Add identical controller benchmark to the pre-optimization revision"],
            "benchmark-commit", cwd=checkout)
    manifest["checkouts"][variant] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=checkout, text=True).strip()
    tasks = [":tests:java-test-image:dockerBuild", ":tests:performance:tools:installDist",
             ":tests:performance:launcher:classes"]
    if variant != "base":
        tasks.append(":microbench:shadowJar")
    run(GRADLE + tasks + ["-Pdocker.tag=rfm-" + variant], "build-" + variant, cwd=checkout)
    image = "apachepulsar/pulsar:rfm-" + variant
    capture(["docker", "image", "inspect", image], OUT / ("image-" + variant + ".json"))
    run(["docker", "run", "--rm", "--entrypoint", "sh", image, "-c",
         "uname -m; cat /etc/os-release; java -version; printenv LD_PRELOAD; ls -l /usr/lib/libmimalloc*"],
        "runtime-" + variant)
    save_manifest()

# Finish all builds before measurements. The same official Pulsar Dockerfile and allocator are used on both sides.
for checkout in checkouts.values():
    run(["./gradlew", "--stop"], "stop-daemon-" + checkout.name, cwd=checkout)
jmh_out = OUT / "jmh"
jmh_out.mkdir(exist_ok=True)
for repetition in range(3):
    for threads in [1, 4]:
        order = ["before", "after"] if repetition % 2 == 0 else ["after", "before"]
        for variant in order:
            jar, = (checkouts[variant] / "microbench/build/libs").glob("*-benchmarks.jar")
            name = f"jmh-{variant}-t{threads}-r{repetition}"
            run(["docker", "run", "--rm", "--user", str(os.getuid()), "--entrypoint", "java",
                 "-v", str(jar) + ":/benchmarks.jar:ro", "-v", str(jmh_out) + ":/results",
                 "apachepulsar/pulsar:rfm-" + variant, "-jar", "/benchmarks.jar",
                 ".*RequestFrameMemoryControllerBenchmark.*", "-f", "1", "-t", str(threads),
                 "-wi", "3", "-i", "5", "-w", "1s", "-r", "1s", "-prof", "gc",
                 "-jvmArgsAppend", "-Xms256m -Xmx256m -XX:MaxDirectMemorySize=512m",
                 "-rf", "json", "-rff", "/results/" + name + ".json"], name)
            manifest["jmh"].append({"variant": variant, "threads": threads,
                                    "repetition": repetition, "file": name + ".json"})
            save_manifest()

reports = OUT / "reports"
common = ["--scenario", "tests/performance/scenarios/iot-telemetry-base.yaml",
          "--extends", "configs/iot-telemetry-low-mem", "--extends", "configs/bookie-journal-tmpfs"]
settings = {
    "workloads.iotTelemetry.rate": 0,
    "workloads.iotTelemetry.warmup.seconds": 0,
    "workloads.iotTelemetry.warmup.messages": 50000,
    "workloads.iotTelemetry.measurement.seconds": 0,
    "workloads.iotTelemetry.measurement.messages": 200000,
    "workloads.iotTelemetry.timeoutSeconds": 240,
    "workloads.iotTelemetry.devices.count": 10000,
    "workloads.iotTelemetry.gateways.count": 10,
    "workloads.iotTelemetry.gateways.producer.ioThreads": 4,
    "workloads.iotTelemetry.gateways.producer.listenerThreads": 4,
    "workloads.iotTelemetry.gateways.producer.maxOutstanding": 1024,
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
    "small-monitoring": {"workloads.iotTelemetry.payload.size": 128,
                         "cluster.brokers.env.inProgressRequestFrameMemoryLimitEnabled": "false"},
    "large-manual": {"workloads.iotTelemetry.payload.size": 131072,
                     "cluster.brokers.env.inProgressRequestFrameMemoryLimitEnabled": "true",
                     "workloads.iotTelemetry.gateways.producer.maxOutstanding": 128,
                     "workloads.iotTelemetry.warmup.messages": 10000,
                     "workloads.iotTelemetry.measurement.messages": 50000},
}
run(GRADLE + [":tests:performance:metrics:up"], "metrics-up", cwd=checkouts["after"])
try:
    for scenario, overrides in scenarios.items():
        for repetition in range(3):
            order = ["base", "before", "after"] if repetition % 2 == 0 else ["after", "before", "base"]
            for variant in order:
                name = f"{scenario}-{variant}-r{repetition}"
                args = common + ["--name", name]
                for key, value in (settings | overrides).items():
                    args += ["--set", f"{key}={value}"]
                code = run(GRADLE + [":tests:performance:launcher:run", "-Pdocker.tag=rfm-" + variant,
                           "-Pperformance.reportsDir=" + str(reports), "-Pperformance.keepLauncherLog",
                           "--args=" + shlex.join(args)], name, cwd=checkouts[variant], check=False)
                entry = {"scenario": scenario, "variant": variant, "repetition": repetition, "exitCode": code}
                manifest["runs"].append(entry)
                save_manifest()
                if code:
                    raise RuntimeError("Invalid scenario run: " + name)
                # Persist sampled broker memory and pending-request evidence before the ephemeral metrics stack stops.
                for metadata in reports.rglob("metrics.json"):
                    destination = metadata.parent / "memory-metrics.json"
                    if destination.exists():
                        continue
                    data = json.loads(metadata.read_text())
                    selector = data["selector"]
                    query = '{__name__=~"jvm_direct_memory.*|process_resident_memory_bytes|pulsar_broker_publish_in_progress_buffer_memory_usage.*|pulsar_.*pending.*",' + selector[1:]
                    params = urllib.parse.urlencode({"query": query, "start": data["startEpochMs"] / 1000,
                        "end": data["endEpochMs"] / 1000, "step": data["intervalSeconds"]})
                    url = data["victoriaMetrics"]["prometheusApi"] + "query_range?" + params
                    with urllib.request.urlopen(url, timeout=30) as response:
                        destination.write_bytes(response.read())
finally:
    run(GRADLE + [":tests:performance:metrics:down"], "metrics-down", cwd=checkouts["after"], check=False)
    capture(["df", "-h"], OUT / "disk-after.txt")
    save_manifest()
