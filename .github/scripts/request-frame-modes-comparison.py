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
import re
import shlex
import subprocess
import sys
import time
import threading
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
TEMP = Path(os.environ["RUNNER_TEMP"])
OUT = TEMP / "request-frame-modes-comparison"
OUT.mkdir(exist_ok=True)
LOGS = OUT / "logs"
LOGS.mkdir(exist_ok=True)
REFS = {
    "without-pr": "17166c820bf011ce7dd8b2b6795cc9c62e734af7",
    "with-pr": "ef2a4475722d94c0d673a317f9c0d82993c56251",
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
    "question": "Without this PR versus the current PR in DISABLED, AUTO and ENABLED modes",
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
    capture(["free", "-h"], OUT / "memory-before.txt")
    capture(["git", "rev-parse", "HEAD"], OUT / "driver-commit.txt")
    capture(["git", "status", "--porcelain"], OUT / "driver-status.txt")
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
    "cluster.brokers.env.OTEL_SDK_DISABLED": "false",
    "cluster.brokers.env.OTEL_METRICS_EXPORTER": "prometheus",
    "cluster.brokers.env.OTEL_TRACES_EXPORTER": "none",
    "cluster.brokers.env.OTEL_LOGS_EXPORTER": "none",
    "cluster.brokers.env.OTEL_EXPORTER_PROMETHEUS_HOST": "0.0.0.0",
    "cluster.brokers.env.OTEL_EXPORTER_PROMETHEUS_PORT": "9464",
}
scenarios = {
    "fixed-small": {"workloads.iotTelemetry.payload.size": 128},
    "fixed-large": {"workloads.iotTelemetry.payload.size": 131072,
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

class FrameMetricSampler:
    """Scrape both revisions identically; retain missing values as null, never as zero."""
    def __init__(self, name):
        self.name = name
        self.stop = threading.Event()
        self.samples = []
        self.errors = []
        self.thread = threading.Thread(target=self.collect, daemon=True)
        self.url = None

    def collect(self):
        with (LOGS / (self.name + "-frame-samples.jsonl")).open("w") as output:
            while not self.stop.is_set():
                started = time.monotonic()
                try:
                    if self.url is None:
                        listing = subprocess.check_output(
                            ["docker", "ps", "--format", "{{json .}}"], text=True, timeout=5)
                        brokers = [json.loads(line) for line in listing.splitlines()
                                   if "pulsar-broker-0" in json.loads(line)["Names"]]
                        if len(brokers) == 1:
                            broker = brokers[0]
                            networks = json.loads(subprocess.check_output(
                                ["docker", "inspect", "--format", "{{json .NetworkSettings.Networks}}",
                                 broker["ID"]], text=True, timeout=5))
                            addresses = [n["IPAddress"] for n in networks.values() if n.get("IPAddress")]
                            assert len(addresses) == 1, addresses
                            self.url = "http://" + addresses[0] + ":9464/metrics"
                            (LOGS / (self.name + "-broker.json")).write_text(json.dumps(
                                {"container": broker, "otelUrl": self.url}, indent=2))
                    if self.url is not None:
                        with urllib.request.urlopen(self.url, timeout=3) as response:
                            raw = response.read().decode()
                        values = {}
                        for line in raw.splitlines():
                            if line.startswith("pulsar_broker_publish_in_progress_buffer_memory_"):
                                metric = line.split("{", 1)[0].split(" ", 1)[0]
                                value = float(line.rsplit("}", 1)[-1].strip().split()[0]) if "}" in line else float(line.split()[1])
                                values[metric] = value
                        throttled = [v for k, v in values.items() if "_throttled" in k]
                        retained = [v for k, v in values.items() if "_usage" in k]
                        assert len(throttled) <= 1 and len(retained) <= 1, values
                        sample = {"timestampEpochMs": time.time() * 1000,
                                  "throttled": throttled[0] if throttled else None,
                                  "retainedBytes": retained[0] if retained else None,
                                  "metricValues": values}
                        if not self.samples:
                            (LOGS / (self.name + "-otel-first.prom")).write_text(raw)
                        self.samples.append(sample)
                        output.write(json.dumps(sample) + "\n")
                        output.flush()
                except Exception as error:
                    self.errors.append({"timestampEpochMs": time.time() * 1000, "error": str(error)})
                self.stop.wait(max(0, 1 - (time.monotonic() - started)))

    def finish(self):
        self.stop.set()
        self.thread.join(timeout=10)
        assert not self.thread.is_alive()
        (LOGS / (self.name + "-frame-sampling-errors.json")).write_text(json.dumps(self.errors, indent=2))


smoke = PHASE == "verify-metrics"
assert smoke or PHASE in scenarios, PHASE
scenario = "verify-metrics" if smoke else PHASE
overrides = scenarios["fixed-small"] if smoke else scenarios[PHASE]
if smoke:
    overrides = overrides | {"workloads.iotTelemetry.rate": 1000,
                             "workloads.iotTelemetry.warmup.seconds": 3,
                             "workloads.iotTelemetry.measurement.seconds": 10}
orders = [["without-pr", "disabled", "auto", "enabled"],
          ["enabled", "auto", "disabled", "without-pr"],
          ["auto", "without-pr", "enabled", "disabled"]]
if smoke:
    orders = [["without-pr", "disabled"]]
run(GRADLE + [":tests:performance:metrics:up"], "metrics-up-" + scenario, cwd=checkouts["with-pr"])
try:
    for repetition, order in enumerate(orders):
        for variant in order:
            revision = "without-pr" if variant == "without-pr" else "with-pr"
            name = f"{scenario}-{variant}-r{repetition}"
            resolved = settings | overrides
            if revision == "with-pr":
                resolved = resolved | {"cluster.brokers.env.inProgressRequestFrameMemoryLimitEnabled": variant.upper()}
            args = common + ["--name", name]
            for key, value in resolved.items():
                args += ["--set", f"{key}={value}"]
            sampler = FrameMetricSampler(name)
            sampler.thread.start()
            try:
                code = run(GRADLE + [":tests:performance:launcher:run", "-Pdocker.tag=pr27110-" + revision,
                           "-Pperformance.reportsDir=" + str(reports), "-Pperformance.keepLauncherLog",
                           "--args=" + shlex.join(args)], name, cwd=checkouts[revision], check=False)
            finally:
                sampler.finish()
            entry = {"scenario": scenario, "variant": variant, "revision": revision,
                     "repetition": repetition, "exitCode": code, "settings": resolved,
                     "measurementRun": not smoke,
                     "frameSamples": "logs/" + name + "-frame-samples.jsonl"}
            manifest["runs"].append(entry)
            save_manifest()
            if code:
                raise RuntimeError("Invalid scenario run: " + name)
            found = [p.parent for p in reports.rglob("run-info.json") if p.parent.parent.name == name]
            assert len(found) == 1, found
            directory = found[0]
            info = json.loads((directory / "run-info.json").read_text())
            assert info["git.commit.id"] == REFS[revision] and not info["git.dirty"], info
            gateway = json.loads((directory / "gateways/gateways-summary.json").read_text())
            applications = list((directory / "applications").rglob("application-summary.json"))
            assert len(applications) == 1
            application = json.loads(applications[0].read_text())
            assert application["uniqueMessages"] == gateway["sent"], application
            assert application["orderingViolations"] == 0 and application["invalidMessages"] == 0, application
            samples = [s for s in sampler.samples if gateway["measurementStartEpochMs"]
                       <= s["timestampEpochMs"] <= gateway["measurementEndEpochMs"]]
            assert len(samples) >= max(5, gateway["measurementElapsedSeconds"] * 0.75), (name, len(samples))
            if revision == "with-pr":
                assert all(s["throttled"] in (0, 1) and s["retainedBytes"] is not None for s in samples), samples
                if variant == "disabled":
                    assert all(s["throttled"] == 0 for s in samples), samples
            else:
                assert all(s["throttled"] is None and s["retainedBytes"] is None for s in samples), samples
            entry.update({"valid": True, "report": str(directory.relative_to(OUT)),
                          "sent": gateway["sent"], "messagesPerSecond": gateway["messagesPerSecond"],
                          "measurementMessages": gateway["measurementMessages"],
                          "measurementSeconds": gateway["measurementElapsedSeconds"],
                          "duplicates": application["duplicates"], "frameMetricSamples": len(samples),
                          "throttledSamples": sum(s["throttled"] == 1 for s in samples) if revision == "with-pr" else None,
                          "retainedFrameSampledPeakBytes": max(s["retainedBytes"] for s in samples) if revision == "with-pr" else None})
            # Persist native broker memory and CPU metrics before the metrics stack stops.
            metadata = json.loads((directory / "metrics.json").read_text())
            selector = metadata["selector"]
            query = '{__name__=~"jvm_memory_direct_.*|jvm_memory_bytes_used|jvm_gc_.*|process_resident_memory_bytes|process_cpu_seconds_total|pulsar_.*pending.*",' + selector[1:]
            params = urllib.parse.urlencode({"query": query, "start": metadata["startEpochMs"] / 1000,
                        "end": metadata["endEpochMs"] / 1000, "step": metadata["intervalSeconds"]})
            url = metadata["victoriaMetrics"]["prometheusApi"] + "query_range?" + params
            with urllib.request.urlopen(url, timeout=30) as response:
                (directory / "memory-metrics.json").write_bytes(response.read())
            save_manifest()
            print("VALID", name, "msg/s", round(gateway["messagesPerSecond"], 1),
                  "throttled samples", entry["throttledSamples"], "/", len(samples), flush=True)
finally:
    run(GRADLE + [":tests:performance:metrics:down"], "metrics-down-" + scenario,
        cwd=checkouts["with-pr"], check=False)
    capture(["df", "-h"], OUT / ("disk-after-" + scenario + ".txt"))
