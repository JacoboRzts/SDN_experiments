#!/usr/bin/env python3
import os
import time
import json
import argparse
import threading
import statistics
from datetime import datetime, timezone
from typing import Optional, Dict
from pathlib import Path
from topology import build_network

# PARAMETERS
DURATION = 30
COOLDOWN = 5
PKT_PAUSE = 10
RSD_TARGET = 10.0
REPS_MIN = 15
REPS_MAX = 30
PKT_SIZES = [64, 128, 256, 512, 1024, 1400]
BANDWIDTH = "1g"
CONGESTION_ALGORITHM = "cubic"

PROTOCOL_OPTIONS = ["udp", "tcp"]
EXPERIMENT_OPTIONS = ["a1", "a2", "a3", "a4"]

OUTPUT_DIR_BASE = Path.home() / "results"


term_cols = os.get_terminal_size().columns

EXPERIMENTS = {
    "a1": [
        {"id": "p1", "client": "h1", "server": "h5", "port": 5201},
    ],
    "a2": [
        {"id": "p1", "client": "h1", "server": "h5", "port": 5201},
        {"id": "p2", "client": "h2", "server": "h6", "port": 5202},
    ],
    "a3": [
        {"id": "p1", "client": "h1", "server": "h5", "port": 5201},
        {"id": "p2", "client": "h2", "server": "h6", "port": 5202},
        {"id": "p3", "client": "h3", "server": "h7", "port": 5203},
    ],
    "a4": [
        {"id": "p1", "client": "h1", "server": "h5", "port": 5201},
        {"id": "p2", "client": "h2", "server": "h6", "port": 5202},
        {"id": "p3", "client": "h3", "server": "h7", "port": 5203},
        {"id": "p4", "client": "h4", "server": "h8", "port": 5204},
    ],
}

# Campos que expone extract_metrics() por protocolo: (clave, etiqueta, decimales)
FIELDS_BY_PROTO = {
    "tcp": [
        ("throughput_mbps",     "Mbps",         3),
        ("retransmits",         "Retrans",      0),
        ("mean_rtt_us",         "RTT-avg(us)",  1),
        ("min_rtt_us",          "RTT-min(us)",  0),
        ("max_rtt_us",          "RTT-max(us)",  0),
        ("snd_cwnd_avg_bytes",  "Cwnd-avg(B)",  0),
        ("snd_cwnd_max_bytes",  "Cwnd-max(B)",  0),
    ],
    "udp": [
        ("throughput_mbps", "Mbps",          3),
        ("jitter_ms",       "Jitter(ms)",    3),
        ("lost_packets",    "Perdida(pkts)", 0),
        ("lost_percent",    "Perdida(%)",    2),
        ("packets",         "Packets",       0),
    ],
}

# Colors
class Color:
    GREEN = "\033[32m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[0;34m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    END = "\033[0m"

def ok(msg: str, end: str = "\n"):
    print(f"{Color.GREEN}  OK {msg}{Color.END}", end=end)

def info(msg, end="\n", start=" "):
    print(f"{start} {Color.BLUE}{msg}{Color.END}", end=end)

def warn(msg, end="\n", start=" "):
    print(f"{start} {Color.YELLOW}WARNING{Color.END} {msg}", end=end)

def error(msg, end="\n", start=" "):
    print(f"{start} {Color.RED}ERROR{Color.END} {msg}", end=end)

def title(title):
    print('\n'+'─'*term_cols)
    print(f"  {title}")
    print('─'*term_cols)

def kill_iperf(host, timeout: int=1):
    host.cmd("pkill -9 iperf3")
    time.sleep(timeout)

def start_server(server, port, timeout=1):
    cmd = f"iperf3 -s -p {port} -D"
    server.cmd(cmd)
    time.sleep(timeout)

def start_all_pair_server(net, pairs):
    for pair in pairs:
        server = net.get(pair['server'])
        client = net.get(pair['client'])
        port = pair['port']
        start_server(server, port)
        if not check_connectivity(client, server.IP(), port):
            error(f"Server {server.name} was not started correctly.")

def check_connectivity(client, server_ip, port) -> bool:
    cmd = f"nc -zv {server_ip} {port} 2>&1"
    result = client.cmd(cmd)
    result = result.lower()
    return "succeeded" in result or "connected" in result

def check_process(host) -> bool:
    """"Check if the given host has running any iperf process."""
    result = host.cmd("pgrep -f iperf3 2>/dev/null")
    return bool(result.strip())

def get_mbps(data):
    """Get the Mbps """
    mbps = data["end"]["sum_sent"]["bits_per_second"]
    if mbps is None:
        return None
    return mbps / 1e6

def save_results(fname, path, data):
    fpath = Path(path) / fname
    fpath.parent.mkdir(parents=True, exist_ok=True)
    fpath.write_text(data)
    return fpath

def compute_mean(values):
    """Compute the mean of an array ignoring the None elements."""
    clean = [v for v in values if v is not None]
    if not clean:
        return None
    return statistics.mean(clean)

def compute_rsd(values):
    """Compute the Relative Standard Deviation of an array ignoring the None elements."""
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = statistics.mean(clean)
    if mean == 0:
        return None
    return (statistics.stdev(clean) / mean) * 100

def _fmt(value, nd=2):
    if value is None:
        return "N/A"
    return f"{value:.{nd}f}"
    cmd = f"iperf3 -s -p {port} -D"
    server.cmd(cmd)
    time.sleep(timeout)

def _fmt_rsd(value):
    """Devuelve (texto, color) para un valor de RSD, o (N/A, None) si no aplica."""
    if value is None:
        return "N/A", None
    color = Color.GREEN if value <= RSD_TARGET else Color.RED
    return f"{value:.2f}%", color


def print_pkt_table(experiment: str, protocol: str, pkt_size: int, pair_rows: Dict[str, list]):
    """
    Imprime, para un pkt_size dado, una tabla con una fila por repeticion de
    cada par, mostrando TODOS los campos que produce extract_metrics() para
    el protocolo en uso (definidos en FIELDS_BY_PROTO). Al final de cada par
    se agregan dos filas de resumen:
      - PROM: el promedio de cada campo.
      - RSD:  el RSD, pero solo para el throughput (Mbps); el resto de las
              columnas queda en "-" ya que solo se pide RSD del Mbps.

    pair_rows: {pid: [ metrics_dict, ... ]} donde cada metrics_dict es tal
               cual lo devuelve extract_metrics() (una entrada por repeticion).
    """
    fields = FIELDS_BY_PROTO[protocol]
    mbps_key = next((key for key, _, _ in fields if key == "throughput_mbps"), None)

    headers = ["Par", "Rep"] + [label for _, label, _ in fields]
    col_w = [6, 5] + [max(len(label) + 2, 10) for _, label, _ in fields]

    def row_str(cols):
        return " | ".join(str(c).ljust(w) for c, w in zip(cols, col_w))

    print("  " + row_str(headers))
    print("  " + "-" * (sum(col_w) + 3 * (len(col_w) - 1)))

    for pid, rows in pair_rows.items():
        for i, r in enumerate(rows, start=1):
            cols = [pid, i]
            for key, _, nd in fields:
                cols.append(_fmt(r.get(key), nd=nd))
            print("  " + row_str(cols))

        # Fila de promedios: una media por campo.
        mean_cols = [pid, "PROM"]
        for key, _, nd in fields:
            mean_cols.append(_fmt(compute_mean([r.get(key) for r in rows]), nd=nd))
        print("  " + row_str(mean_cols))

        # Fila de RSD: solo se calcula para el Mbps, el resto va vacio.
        plain_cols = [pid, "RSD"]
        colors = [None, None]
        for key, _, _ in fields:
            if key == mbps_key:
                text, color = _fmt_rsd(compute_rsd([r.get(key) for r in rows]))
            else:
                text, color = "-", None
            plain_cols.append(text)
            colors.append(color)

        padded = [str(c).ljust(w) for c, w in zip(plain_cols, col_w)]
        colored = [
            f"{color}{text}{Color.END}" if color else text
            for text, color in zip(padded, colors)
        ]
        print("  " + " | ".join(colored))
        print()


def extract_metrics(data, proto):
    data = json.loads(data)
    m = {}
    end = data.get("end", {})
    if proto == "tcp":
        sum_sent = end.get("sum_sent", {})
        bps = sum_sent.get("bits_per_second")
        if bps is not None:
            m["throughput_mbps"] = round(bps / 1e6, 3)
        m["retransmits"] = sum_sent.get("retransmits")
        rtts_mean, rtts_min, rtts_max = [], [], []
        for s in end.get("streams", []):
            snd = s.get("sender", {})
            if snd.get("mean_rtt") is not None:
                rtts_mean.append(snd["mean_rtt"])
            if snd.get("min_rtt") is not None:
                rtts_min.append(snd["min_rtt"])
            if snd.get("max_rtt") is not None:
                rtts_max.append(snd["max_rtt"])
        if rtts_mean:
            m["mean_rtt_us"] = round(statistics.mean(rtts_mean), 1)
        if rtts_min:
            m["min_rtt_us"] = min(rtts_min)
        if rtts_max:
            m["max_rtt_us"] = max(rtts_max)
        cwnd_vals = []
        for iv in data.get("intervals", []):
            c = iv.get("sum", {}).get("snd_cwnd")
            if c is not None:
                cwnd_vals.append(c)
        if cwnd_vals:
            m["snd_cwnd_avg_bytes"] = round(statistics.mean(cwnd_vals), 0)
            m["snd_cwnd_max_bytes"] = round(max(cwnd_vals), 0)
    elif proto == "udp":
        summ = end.get("sum", {})
        bps = summ.get("bits_per_second")
        if bps is not None:
            m["throughput_mbps"] = round(bps / 1e6, 3)
        m["jitter_ms"]    = summ.get("jitter_ms")
        m["lost_packets"] = summ.get("lost_packets")
        m["lost_percent"] = summ.get("lost_percent")
        m["packets"]      = summ.get("packets")
    return m

def inject_meta(data: dict, pair: dict, server_ip, pkt_size: int, rep: int, topology: str, experiment: str, note: str, proto: str, metrics: Dict[str, Optional[float]]) -> dict:
    """
    Añade el bloque _meta al JSON crudo de iperf3.
    Incluye todo lo necesario para reconstruir la corrida sin depender del nombre
    de archivo, y las métricas ya extraídas listas para análisis.
    """
    data["_meta"] = {
        "experiment":             experiment,
        "topology":               topology,
        "protocol":               proto,
        "pair_id":                pair["id"],
        "client_host":            pair["client"],
        "server_host":            pair["server"],
        "server_ip":              server_ip,
        "pkt_size_b":             pkt_size,
        "rep":                    rep,
        "duration_s":             DURATION,
        "cooldown_s":             COOLDOWN,
        "tcp_congestion_control": CONGESTION_ALGORITHM,
        "bandwith":               BANDWIDTH,
        "timestamp_utc":          datetime.now(timezone.utc).isoformat(),
        "note":                   note,
        "metrics":                metrics,
    }
    return data

def preflight(hosts):
    status = True
    tools = ["netcat", "iperf3"]
    not_installed = {}
    for host in hosts:
        for tool in tools:
            output = host.cmd(f"{tool} --version 2>&1")
            status = bool(output.strip())
            if not status:
                not_installed["host.name"] = tool
    return status, not_installed

def run_pair(net, pair, pkt_size, protocol, results, fname_template, path):
    server = net.get(pair['server'])
    client = net.get(pair['client'])
    pid = pair['id']
    port = pair['port']
    if protocol == "tcp":
        flag = f"-Z -C {CONGESTION_ALGORITHM}"
    else:
        flag = "-u"
    cmd = f"iperf3 -c {server.IP()} -p {port} -t {DURATION} -l {pkt_size} -b {BANDWIDTH} {flag} -J"
    output, err, exitcode = client.pexec(cmd)
    if exitcode != 0:
        error(f"[{pid}] Exitcode {exitcode}, {err.strip()}")
        info("Verifing connection...")
        if check_connectivity(client, server.IP(), port):
            ok(f"[{pid}] Server running")
        else:
            error(f"[{pid}] Server is not running")
    if not output.strip():
        warn(f"{pid}: iperf3 empty output.")
    metrics = extract_metrics(output, protocol)
    fname = fname_template.replace("pid", pid)
    save_results(fname, path, output)
    results[pid] = metrics

def run_iteration(net, pairs, pkt_size, protocol, fname, path):
    results = {}
    threads = []
    for p in pairs:
        t = threading.Thread(target=run_pair, args=(net, p, pkt_size, protocol, results, fname, path))
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    return results

def run_protocol(net, experiment, protocol, topology):
    pairs = EXPERIMENTS[experiment]

    for pkt_size in PKT_SIZES:
        if pkt_size == 1400:
            pkt_size = 1460 if protocol == "TCP" else 1472
        title(f"Experiment {experiment.upper()}\n  Protocol {protocol.upper()}\n  Packet Size: {pkt_size}B")
        pair_rows = {p["id"]: [] for p in pairs}
        for rep in range(1, REPS_MAX+1):
            print(f"  REP {rep:02d}/{REPS_MAX:02d}")
            fname = f"{experiment}_{protocol}_pkt{pkt_size:04d}_pid__rep{rep:02d}.json"
            path = OUTPUT_DIR_BASE / topology
            metrics = run_iteration(net, pairs, pkt_size, protocol, fname, path)
            for pid, m in metrics.items():
                pair_rows[pid].append(m)

            if rep >= REPS_MIN:
                mbps_rsds = [
                    compute_rsd([r.get("throughput_mbps") for r in rows])
                    for rows in pair_rows.values()
                ]
                if mbps_rsds and all(v is not None and v <= RSD_TARGET for v in mbps_rsds):
                    ok(f"Mbps RSD <= {RSD_TARGET}% on {rep} reps.")
                    break

            if rep < REPS_MAX:
                time.sleep(COOLDOWN)

        print_pkt_table(experiment, protocol, pkt_size, pair_rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--experiment", default="all", help="Experiments to execute.")
    parser.add_argument("-t", "--topology", choices=["sl", "j3c"], default="sl", help="Topology to use.")
    parser.add_argument("-p", "--protocol", default="both", help="Select one protocol, default use both.")
    parser.add_argument("-d", "--dry-run", action="store_true")
    parser.add_argument("-s", "--skip-preflight", action="store_true")
    parser.add_argument("-c", "--controller-ip", default="172.17.0.2", help="IP address of the controller.")
    args = parser.parse_args()

    topology = args.topology
    experiments = EXPERIMENT_OPTIONS if args.experiment == "all" else [args.experiment]
    protocols = PROTOCOL_OPTIONS if args.protocol == "both" else [args.protocol]

    print("Running experiments with options: ")
    info(f"Topology: {topology}")
    info(f"Controller: {args.controller_ip}")
    info(f"Experiments: {experiments}")
    info(f"Procols: {protocols}")
    info(f"Package sizes: {PKT_SIZES}")
    info(f"Reps. {REPS_MIN}/{REPS_MAX}")
    info(f"Duration: {DURATION}")
    info(f"Cooldown: {COOLDOWN}")
    print()

    print("*** Building the network...")
    net = build_network(2, 3, 3, topology=topology, controller_ip=args.controller_ip)
    try:
        net.start()
        hosts = net.hosts

        # Preflight
        if not args.skip_preflight and not args.dry_run:
            status, _ = preflight(hosts)
            if not status:
                return

        for experiment in experiments:
            start_all_pair_server(net, EXPERIMENTS[experiment])
            for protocol in protocols:
                run_protocol(net, experiment, protocol, topology)
            info("Removing iperf3 servers")
            for host in hosts:
                kill_iperf(host)

    except Exception as e:
        error(str(e))
    finally:
        net.stop()

if __name__ == "__main__":
    main()
