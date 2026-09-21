#!/usr/bin/env python3

import argparse
import atexit
import json
import shlex
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN GLOBAL
# ══════════════════════════════════════════════════════════════════════════════

# --- Temporización ---
DURATION      = 30       # Duración de cada corrida iperf3 (s)
COOLDOWN      = 5        # Pausa entre reps dentro de un mismo pkt_size (s)
PKT_PAUSE     = 10       # Pausa al cambiar de pkt_size (s)
PROTO_PAUSE   = 20       # Pausa al cambiar de protocolo TCP -> UDP (s)

# --- Arranque sincronizado ---
# SSH viaja por la misma red que se está saturando, así que NUNCA se usa mientras
# corre el tráfico de prueba. Cada rep tiene 3 fases:
#   1) ARMAR   (red en reposo): se le manda a cada cliente el comando con una hora
#              de arranque absoluta; el cliente espera hasta esa hora.
#   2) CORRER  (sin SSH): todos los clientes disparan a la vez y guardan su JSON
#              en un archivo local en el propio cliente.
#   3) RECOGER (red otra vez en reposo): se lee el JSON de cada cliente por SSH.
START_LEAD    = 4.0      # Segundos entre "armar" y el disparo simultáneo (s)
COLLECT_GRACE = 30       # Espera máx. tras DURATION a que el cliente termine (s)
REMOTE_TMP    = "/tmp"   # Carpeta del cliente donde deja su JSON/código de salida

# --- Criterio de convergencia (RSD sobre throughput) ---
RSD_TARGET    = 10.0     # % objetivo de RSD para dar por convergido
REPS_MIN      = 15       # Mínimo de reps antes de evaluar convergencia
REPS_MAX      = 30       # Máximo de reps (corte duro aunque no converja)

# --- Barrido ---
PKT_SIZES     = [64, 128, 256, 512, 1024, 1400]   # Bytes de payload iperf3 (-l)
UDP_MAXSIZE = 1472
TCP_MAXSIZE = 1460
PROTOCOLS     = ["tcp", "udp"]              # Orden: TCP primero, luego UDP

# --- Parámetros por protocolo ---
TCP_CONGESTION = "cubic"    # -C cubic
UDP_BITRATE    = "1g"       # -b 1g  (line-rate físico del testbed) 1 Gb/s Igual que todos los links de la topologia

# --- Infraestructura ---
KEY_PATH    = Path.home() / ".ssh" / "id_rsa_testbed"
OUTPUT_BASE = Path.home() / "resultados" / datetime.now().strftime("%d%m%Y")

# ══════════════════════════════════════════════════════════════════════════════
#  DEFINICIÓN DE EXPERIMENTOS
# ══════════════════════════════════════════════════════════════════════════════

EXPERIMENTS = {
    "a1": {
        "desc": "1 par cross-leaf/edge — baseline de carga baja",
        "note": "a1: par único cruzando el punto central de la topología",
    },
    "a2": {
        "desc": "2 pares simultáneos cruzando el core/spine",
        "note": "a2: 2 pares concurrentes",
    },
    "a3": {
        "desc": "3 pares simultáneos cruzando el core/spine",
        "note": "a3: 3 pares concurrentes",
    },
    "a4": {
        "desc": "4 pares simultáneos cruzando el core/spine (carga alta)",
        "note": "a4: 4 pares concurrentes",
    },
}

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN POR TOPOLOGÍA
# ══════════════════════════════════════════════════════════════════════════════

TOPOLOGIES = {
    "sl": {
        "desc": "Spine-Leaf (SL) — port-pinning estático S1/S2",
        "hosts": {
            "H1": {"ip": "10.0.1.1", "user": "h1", "leaf": "L1"},
            "H2": {"ip": "10.0.1.2", "user": "becarios", "leaf": "L1"},
            "H3": {"ip": "10.0.1.3", "user": "becarios", "leaf": "L1"},
            "H4": {"ip": "10.0.2.1", "user": "h4", "leaf": "L2"},
            "H5": {"ip": "10.0.2.2", "user": "h5", "leaf": "L2"},
            "H6": {"ip": "10.0.2.3", "user": "h6", "leaf": "L2"},
            "H7": {"ip": "10.0.3.1", "user": "h7", "leaf": "L3"},
            "H8": {"ip": "10.0.3.2", "user": "h8", "leaf": "L3"},
        },
        "pairs": {
            "a1": [
                {"id": "p1", "client": "H1", "server": "H5", "port": 5201},  # L1 -> L2
            ],
            "a2": [
                {"id": "p1", "client": "H1", "server": "H5", "port": 5201},  # L1 -> L2
                {"id": "p2", "client": "H2", "server": "H6", "port": 5202},  # L1 -> L2
            ],
            "a3": [
                {"id": "p1", "client": "H1", "server": "H5", "port": 5201},  # L1 -> L2
                {"id": "p2", "client": "H2", "server": "H6", "port": 5202},  # L1 -> L2
                {"id": "p3", "client": "H3", "server": "H7", "port": 5203},  # L1 -> L3
            ],
            "a4": [
                {"id": "p1", "client": "H1", "server": "H5", "port": 5201},  # L1 -> L2
                {"id": "p2", "client": "H2", "server": "H6", "port": 5202},  # L1 -> L2
                {"id": "p3", "client": "H3", "server": "H7", "port": 5203},  # L1 -> L3
                {"id": "p4", "client": "H4", "server": "H8", "port": 5204},  # L2 -> L3
            ],
        },
    },
    "j3c": {
        "desc": "Jerárquica 3 Capas (J3C) — todos los pares cruzan Core1",
        "hosts": {
            "H1": {"ip": "10.0.1.1", "user": "h1", "edge": "E1"},
            "H2": {"ip": "10.0.1.2", "user": "becarios", "edge": "E1"},
            "H3": {"ip": "10.0.1.3", "user": "becarios", "edge": "E1"},
            "H4": {"ip": "10.0.1.4", "user": "h4", "edge": "E1"},
            "H5": {"ip": "10.0.2.1", "user": "h5", "edge": "E2"},
            "H6": {"ip": "10.0.2.2", "user": "h6", "edge": "E2"},
            "H7": {"ip": "10.0.2.3", "user": "h7", "edge": "E2"},
            "H8": {"ip": "10.0.2.4", "user": "h8", "edge": "E2"},
        },
        "pairs": {
            "a1": [
                {"id": "p1", "client": "H1", "server": "H5", "port": 5201},  # E1 -> E2 (Core1)
            ],
            "a2": [
                {"id": "p1", "client": "H1", "server": "H5", "port": 5201},  # E1 -> E2 (Core1)
                {"id": "p2", "client": "H2", "server": "H6", "port": 5202},  # E1 -> E2 (Core1)
            ],
            "a3": [
                {"id": "p1", "client": "H1", "server": "H5", "port": 5201},  # E1 -> E2 (Core1)
                {"id": "p2", "client": "H2", "server": "H6", "port": 5202},  # E1 -> E2 (Core1)
                {"id": "p3", "client": "H3", "server": "H7", "port": 5203},  # E1 -> E2 (Core1)
            ],
            "a4": [
                {"id": "p1", "client": "H1", "server": "H5", "port": 5201},  # E1 -> E2 (Core1)
                {"id": "p2", "client": "H2", "server": "H6", "port": 5202},  # E1 -> E2 (Core1)
                {"id": "p3", "client": "H3", "server": "H7", "port": 5203},  # E1 -> E2 (Core1)
                {"id": "p4", "client": "H4", "server": "H8", "port": 5204},  # E1 -> E2 (Core1)
            ],
        },
    },
}

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS SSH
# ══════════════════════════════════════════════════════════════════════════════

HOSTS: Dict[str, Dict[str, str]] = {}   # Se llena en main() desde TOPOLOGIES[topo]["hosts"]

# ControlMaster/ControlPersist: la 1ª conexión a cada host queda abierta y todas
# las siguientes viajan por ella. Así, durante la prueba no se negocia ningún
# handshake SSH nuevo (que es lo que fallaba con la red saturada).
_SSH_OPTS = [
    "ssh",
    "-i", str(KEY_PATH),
    "-o", "StrictHostKeyChecking=no",
    "-o", "ConnectTimeout=10",
    "-o", "BatchMode=yes",
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=6",
    "-o", "ControlMaster=auto",
    "-o", "ControlPath=/tmp/physical-ssh-%C",
    "-o", "ControlPersist=2h",
]


def ssh_run(host_key: str, cmd: str, timeout: int = 90) -> subprocess.CompletedProcess:
    """Ejecuta un comando por SSH y espera su salida. Bloqueante."""
    h = HOSTS[host_key]
    return subprocess.run(
        _SSH_OPTS + [f"{h['user']}@{h['ip']}", cmd],
        capture_output=True, text=True, timeout=timeout,
    )


def ssh_bg(host_key: str, cmd: str) -> None:
    """Lanza un comando por SSH en background (no espera salida)."""
    h = HOSTS[host_key]
    subprocess.Popen(
        _SSH_OPTS + [f"{h['user']}@{h['ip']}", f"nohup {cmd} >/dev/null 2>&1 &"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def kill_iperf(host_key: str) -> None:
    """Mata cualquier iperf3 corriendo en el host indicado."""
    ssh_run(host_key, "pkill -9 iperf3 2>/dev/null; true", timeout=10)


def kill_iperf_all(pairs: List[dict]) -> None:
    """Mata iperf3 en clientes Y servidores de los pares dados."""
    hosts = {p["client"] for p in pairs} | {p["server"] for p in pairs}
    for h in hosts:
        kill_iperf(h)


def close_masters() -> None:
    """Cierra las conexiones SSH persistentes (se registra con atexit en main)."""
    for h in HOSTS.values():
        try:
            subprocess.run(_SSH_OPTS + ["-O", "exit", f"{h['user']}@{h['ip']}"],
                           capture_output=True, timeout=10)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  SINCRONIZACIÓN DE RELOJES
# ══════════════════════════════════════════════════════════════════════════════

CLOCK_OFFSET: Dict[str, float] = {}   # reloj_remoto - reloj_local, en segundos


def measure_offset(host_key: str, samples: int = 7) -> Optional[float]:
    """
    Desfase (reloj del host - reloj de esta PC) en segundos, compensando la
    latencia de red: se toman varias muestras y se conserva la de menor RTT,
    que es la que menos error de ida/vuelta arrastra. Retorna None si ninguna
    muestra fue válida.

    No hace falta que los relojes estén sincronizados por NTP: el desfase medido
    se compensa al calcular la hora de arranque de cada cliente.
    """
    best = None   # (rtt, offset)
    for _ in range(samples):
        t0 = time.time()
        try:
            res = ssh_run(host_key, "date +%s.%N", timeout=10)
        except subprocess.TimeoutExpired:
            continue
        t1 = time.time()
        if res.returncode != 0:
            continue
        try:
            remote = float(res.stdout.strip())
        except ValueError:
            continue
        rtt = t1 - t0
        if best is None or rtt < best[0]:
            best = (rtt, remote - (t0 + t1) / 2)
    return best[1] if best else None


def refresh_offsets(pairs: List[dict], samples: int = 7) -> None:
    """Mide el desfase de reloj de cada cliente (llamar con la red en reposo)."""
    parts = []
    for name in sorted({p["client"] for p in pairs}):
        off = measure_offset(name, samples)
        if off is None:
            print(f"  WARN no se pudo medir el reloj de {name}; se asume desfase 0")
            off = 0.0
        CLOCK_OFFSET[name] = off
        parts.append(f"{name}={off * 1000:+.1f}")
    print(f"  Desfase de reloj vs esta PC (ms): {'  '.join(parts)}")


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS DE ESTADÍSTICAS Y EXTRACCIÓN DE MÉTRICAS
# ══════════════════════════════════════════════════════════════════════════════

def compute_rsd(values: List[float]) -> Optional[float]:
    """
    Relative Standard Deviation en %.
    = (stdev / mean) * 100
    Retorna None si hay menos de 2 valores válidos o media == 0.
    """
    clean = [v for v in values if v is not None]
    if len(clean) < 2:
        return None
    mean = statistics.mean(clean)
    if mean == 0:
        return None
    return (statistics.stdev(clean) / mean) * 100


def extract_metrics(data: dict, proto: str) -> Dict[str, Optional[float]]:
    """
    Extrae métricas homogéneas desde el JSON de iperf3, según protocolo.

    Estructura de retorno (mismas claves para TCP y UDP; None cuando no aplica):
      throughput_mbps       — Mbps útiles del flujo (sender-side TCP, receiver-side UDP)
      mean_rtt_us           — RTT medio en µs (TCP)
      min_rtt_us            — RTT mínimo en µs (TCP)
      max_rtt_us            — RTT máximo en µs (TCP)
      retransmits           — Segmentos retransmitidos totales (TCP)
      snd_cwnd_avg_bytes    — Ventana de congestión promedio (TCP)
      snd_cwnd_max_bytes    — Ventana de congestión máxima (TCP)
      jitter_ms             — Jitter en ms (UDP)
      lost_packets          — Paquetes perdidos (UDP)
      lost_percent          — % de pérdida (UDP)
      packets               — Paquetes totales enviados (UDP)
    """
    m: Dict[str, Optional[float]] = {
        "throughput_mbps":    None,
        "mean_rtt_us":        None,
        "min_rtt_us":         None,
        "max_rtt_us":         None,
        "retransmits":        None,
        "snd_cwnd_avg_bytes": None,
        "snd_cwnd_max_bytes": None,
        "jitter_ms":          None,
        "lost_packets":       None,
        "lost_percent":       None,
        "packets":            None,
    }

    try:
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

    except (KeyError, TypeError, ValueError, AttributeError):
        pass

    return m


def inject_meta(data: dict, pair: dict, pkt_size: int, rep: int,
                topology: str, experiment: str, note: str,
                proto: str, metrics: Dict[str, Optional[float]],
                sync: Optional[dict] = None) -> dict:
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
        "server_ip":              HOSTS[pair["server"]]["ip"],
        "pkt_size_b":             pkt_size,
        "rep":                    rep,
        "duration_s":             DURATION,
        "cooldown_s":             COOLDOWN,
        "tcp_congestion_control": TCP_CONGESTION if proto == "tcp" else None,
        "udp_bitrate":            UDP_BITRATE    if proto == "udp" else None,
        "timestamp_utc":          datetime.now(timezone.utc).isoformat(),
        "note":                   note,
        "metrics":                metrics,
        "sync":                   sync,
    }
    return data


# ══════════════════════════════════════════════════════════════════════════════
#  LÓGICA DE EXPERIMENTO
# ══════════════════════════════════════════════════════════════════════════════

def build_server_cmd(pair: dict, proto: str) -> str:
    """Comando iperf3 para el servidor."""
    return f"iperf3 -s -p {pair['port']}"


def build_client_cmd(pair: dict, pkt_size: int, proto: str) -> str:
    """Comando iperf3 para el cliente, según protocolo."""
    srv_ip = HOSTS[pair["server"]]["ip"]
    base = (f"iperf3 -c {srv_ip} -p {pair['port']}"
            f" -t {DURATION} -l {pkt_size} -J -Z")

    if proto == "tcp":
        # -Z: zero-copy (sendfile) para no ser cuello de botella en el kernel.
        # -C cubic: congestion control explícito.
        return f"{base} -C {TCP_CONGESTION}"
    elif proto == "udp":
        # -u: UDP. -b: bitrate objetivo (line-rate físico del testbed).
        return f"{base} -u -b {UDP_BITRATE}"
    else:
        raise ValueError(f"Protocolo desconocido: {proto}")


def _remote_tag(topology: str, experiment: str, proto: str, pair: dict) -> str:
    """Prefijo de los archivos temporales que el cliente deja en REMOTE_TMP."""
    return f"physical_{topology}_{experiment}_{proto}_{pair['id']}"


def build_armed_cmd(pair: dict, pkt_size: int, proto: str,
                    t_remote: float, tag: str) -> str:
    """
    Comando (para ssh_run) que deja al cliente ARMADO y vuelve de inmediato:
      - borra resultados de la rep anterior,
      - en background espera hasta `t_remote` (epoch según el reloj del CLIENTE),
      - lanza iperf3 volcando el JSON a {REMOTE_TMP}/{tag}.json,
      - al terminar escribe el código de salida en {REMOTE_TMP}/{tag}.rc.
    Nada de esto usa la red una vez armado, así que SSH no compite con el tráfico.
    """
    out = f"{REMOTE_TMP}/{tag}.json"
    err = f"{REMOTE_TMP}/{tag}.err"
    rc  = f"{REMOTE_TMP}/{tag}.rc"
    t_ns = int(round(t_remote * 1e9))

    script = (
        # espera activa (paso de 5 ms) hasta la hora de arranque, en ns enteros
        f'while [ "$(date +%s%N)" -lt {t_ns} ]; do sleep 0.005; done; '
        f"{build_client_cmd(pair, pkt_size, proto)} > {out} 2> {err}; "
        f"echo $? > {rc}"
    )
    return (f"rm -f {out} {err} {rc}; "
            f"nohup sh -c {shlex.quote(script)} >/dev/null 2>&1 </dev/null &")


def collect_result(host_key: str, tag: str, deadline: float):
    """
    Sondea al cliente hasta que exista su archivo .rc (o se cumpla `deadline`) y
    devuelve (rc_iperf3, json_crudo). Se llama con la red ya en reposo.
    """
    rc  = f"{REMOTE_TMP}/{tag}.rc"
    out = f"{REMOTE_TMP}/{tag}.json"
    cmd = f"[ -s {rc} ] && cat {rc} {out}"
    while time.time() < deadline:
        try:
            res = ssh_run(host_key, cmd, timeout=15)
            if res.returncode == 0 and res.stdout.strip():
                first, _, body = res.stdout.partition("\n")
                return int(first.strip()), body
        except subprocess.TimeoutExpired:
            pass
        time.sleep(1)
    raise TimeoutError(f"el cliente no terminó antes del deadline ({tag})")


def start_servers(pairs: List[dict], proto: str) -> None:
    """Mata iperf3 previo, lanza un server por par, espera a que escuchen."""
    kill_iperf_all(pairs)
    time.sleep(1)
    for p in pairs:
        ssh_bg(p["server"], build_server_cmd(p, proto))
    time.sleep(2)


def run_single_pair(pair: dict, pkt_size: int, rep: int, topology: str,
                    experiment: str, note: str, proto: str, out_dir: Path,
                    results: dict, errors: list,
                    t_start: float, tag: str, sync: dict) -> None:
    """
    Recoge el resultado de un par que YA fue armado por run_rep. Diseñado para
    correr en un thread propio junto con los demás pares del escenario.

    No toca la red mientras dura la prueba: duerme hasta t_start + DURATION y
    recién entonces (red en reposo) pregunta al cliente por su JSON.
    """
    fname = (
        f"{topology}_{experiment}_{proto}"
        f"_pkt{pkt_size:04d}"
        f"_{pair['id']}"
        f"_rep{rep:02d}.json"
    )
    fpath = out_dir / fname

    try:
        time.sleep(max(0.0, t_start + DURATION - time.time()))
        rc, body = collect_result(pair["client"], tag,
                                  deadline=t_start + DURATION + COLLECT_GRACE)

        if rc != 0 or not body.strip():
            # iperf3 -J reporta sus errores dentro del JSON ("error": "..."),
            # no por stderr; se intenta leer de ahí para saber la causa real.
            detail = ""
            try:
                detail = str(json.loads(body).get("error", "")) if body.strip() else ""
            except json.JSONDecodeError:
                pass
            if not detail:
                r = ssh_run(pair["client"],
                            f"head -c 200 {REMOTE_TMP}/{tag}.err", timeout=10)
                detail = r.stdout.strip()
            errors.append(f"{pair['id']} [{proto}]: iperf3 rc={rc} — {detail[:150]}")
            results[pair["id"]] = None
            return

        data = json.loads(body)
        metrics = extract_metrics(data, proto)
        data = inject_meta(data, pair, pkt_size, rep, topology,
                           experiment, note, proto, metrics, sync)
        fpath.write_text(json.dumps(data, indent=2))
        results[pair["id"]] = metrics.get("throughput_mbps")

    except TimeoutError as e:
        errors.append(f"{pair['id']} [{proto}]: {e}")
        results[pair["id"]] = None
        try:
            kill_iperf(pair["client"])
        except Exception:
            pass
    except json.JSONDecodeError as e:
        errors.append(f"{pair['id']} [{proto}]: JSON inválido — {e}")
        results[pair["id"]] = None
    except Exception as e:
        errors.append(f"{pair['id']} [{proto}]: {type(e).__name__} — {e}")
        results[pair["id"]] = None
        try:
            kill_iperf(pair["client"])
        except Exception:
            pass


def run_rep(pairs: List[dict], pkt_size: int, rep: int, topology: str,
            experiment: str, note: str, proto: str,
            out_dir: Path) -> List[Optional[float]]:
    """
    Ejecuta una repetición con arranque simultáneo:
      1) ARMAR: fija una hora común (t_start, reloj de esta PC) y deja a cada
         cliente esperándola, ya convertida a SU reloj con el offset medido.
      2) CORRER: los clientes disparan solos a la vez; aquí no se usa SSH.
      3) RECOGER: un thread por par lee el JSON cuando la red vuelve a estar libre.
    Devuelve la lista de throughputs en Mbps (None si falló el par).
    """
    results: Dict[str, Optional[float]] = {}
    errors: List[str] = []

    # --- 1) ARMAR (red en reposo) ---
    t_start = time.time() + START_LEAD
    armed: List[dict] = []
    tags: Dict[str, str] = {}
    syncs: Dict[str, dict] = {}

    for p in pairs:
        tag = _remote_tag(topology, experiment, proto, p)
        off = CLOCK_OFFSET.get(p["client"], 0.0)
        cmd = build_armed_cmd(p, pkt_size, proto, t_start + off, tag)
        try:
            res = ssh_run(p["client"], cmd, timeout=10)
            ok, why = res.returncode == 0, res.stderr.strip()[:100]
        except subprocess.TimeoutExpired:
            ok, why = False, "timeout SSH"
        if ok:
            armed.append(p)
            tags[p["id"]] = tag
            syncs[p["id"]] = {
                "start_target_utc": datetime.fromtimestamp(
                    t_start, timezone.utc).isoformat(),
                "clock_offset_s":   round(off, 6),
                "start_lead_s":     START_LEAD,
            }
        else:
            errors.append(f"{p['id']} [{proto}]: no se pudo armar el cliente ({why})")
            results[p["id"]] = None

    slack = t_start - time.time()
    if slack < 0.2:
        errors.append(
            f"arranque tardío ({-slack * 1000:.0f} ms pasada la hora): "
            f"los pares no arrancaron juntos; sube START_LEAD")

    # --- 2) y 3) CORRER + RECOGER ---
    threads = [
        threading.Thread(
            target=run_single_pair,
            args=(p, pkt_size, rep, topology, experiment, note, proto,
                  out_dir, results, errors, t_start, tags[p["id"]],
                  syncs[p["id"]]),
        )
        for p in armed
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=START_LEAD + DURATION + COLLECT_GRACE + 15)

    for err in errors:
        print(f"    WARN {err}")

    return [results.get(p["id"]) for p in pairs]


# ══════════════════════════════════════════════════════════════════════════════
#  LOOP PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def _print_header(experiment: str, topology: str, pairs: List[dict],
                  out_dir: Path) -> None:
    """Cabecera informativa al arrancar el experimento."""
    exp_config = EXPERIMENTS[experiment]
    topo_desc = TOPOLOGIES[topology]["desc"]

    print(f"\n{'='*70}")
    print(f"  {experiment.upper()}  |  Topología: {topology.upper()}")
    print(f"  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"  Descripción: {exp_config['desc']}")
    print(f"  Output: {out_dir}")
    print(f"  Protocolos:  {PROTOCOLS}  (orden de ejecución)")
    print(f"  TCP CC:      {TCP_CONGESTION}   |   UDP bitrate: {UDP_BITRATE}")
    print(f"  Cooldown:    {COOLDOWN}s  |  Pkt-pause: {PKT_PAUSE}s  ")
    print(f"  Pares:  " + "  ".join(
        f"{p['id']}:{p['client']}->{p['server']}" for p in pairs))
    print(f"  PKTs:   {PKT_SIZES}")
    print(f"  Reps:   {REPS_MIN}-{REPS_MAX}  (RSD < {RSD_TARGET}%)")
    print(f"{'='*70}\n")


def _print_summary_table(experiment: str, topology: str,
                         summary: Dict[str, Dict[int, dict]]) -> None:
    """Imprime la tabla resumen agrupada por protocolo."""
    print(f"\n{'='*70}")
    print(f"  RESUMEN F1-{experiment.upper()} — {topology.upper()}")
    print(f"{'='*70}")
    for proto in PROTOCOLS:
        if proto not in summary:
            continue
        print(f"\n  --- {proto.upper()} ---")
        print(f"  {'PKT(B)':>8}  {'Reps':>5}  {'Mbps':>10}  {'RSD%':>7}  {'LR%':>8}")
        print(f"  {'-'*8}  {'-'*5}  {'-'*10}  {'-'*7}  {'-'*8}")
        for pkt, s in summary[proto].items():
            rsd_str = f"{s['rsd_pct']:.1f}" if s["rsd_pct"] is not None else "N/A"
            print(f"  {pkt:>8}  {s['reps']:>5}  {s['mean_mbps']:>10.2f}"
                  f"  {rsd_str:>7}  {s['lr_pct']:>7.1f}%")
    print(f"{'='*70}\n")


def run_sweep(pairs: List[dict], topology: str, experiment: str, sizes: list[int],
              note: str, proto: str, out_dir: Path) -> Dict[int, dict]:
    """
    Barrido de PKT_SIZES para un protocolo dado. Para cada pkt_size:
      - lanza servers
      - itera reps hasta converger por RSD o agotar REPS_MAX
      - registra media, RSD y LR% en el summary del protocolo
    """
    summary: Dict[int, dict] = {}

    print(f"\n{'#'*70}")
    print(f"#  PROTOCOLO: {proto.upper()}")
    print(f"{'#'*70}\n")

    for pkt_size in sizes:
        if pkt_size == 1400:
            pkt_size = UDP_MAXSIZE if proto == "udp" else TCP_MAXSIZE
        print(f"-- PKT {pkt_size:>4d} B  [{proto}]  " + "-"*44)

        throughputs_per_rep: List[float] = []

        start_servers(pairs, proto)
        refresh_offsets(pairs)   # red en reposo: también calienta el SSH persistente

        rep = 1
        converged = False
        while rep <= REPS_MAX:
            print(f"  Rep {rep:02d}/{REPS_MAX}  ", end="", flush=True)

            mbps_list = run_rep(pairs, pkt_size, rep, topology,
                                experiment, note, proto, out_dir)
            valid = [v for v in mbps_list if v is not None]

            if valid:
                rep_mean = statistics.mean(valid)
                throughputs_per_rep.append(rep_mean)
                print(f"  {rep_mean:8.2f} Mbps  [{len(valid)}/{len(pairs)} ok]",
                      end="")
            else:
                print("  FAIL todos los pares fallaron", end="")

            if rep >= REPS_MIN and len(throughputs_per_rep) >= REPS_MIN:
                rsd = compute_rsd(throughputs_per_rep)
                if rsd is not None:
                    print(f"  RSD={rsd:.1f}%", end="")
                    if rsd < RSD_TARGET:
                        print("  OK converge")
                        converged = True
                        break

            print()
            if rep < REPS_MAX:
                time.sleep(COOLDOWN)
            rep += 1

        if not converged:
            print(f"\n  WARN RSD no convergió en {REPS_MAX} reps ({proto})")

        final_rsd = compute_rsd(throughputs_per_rep)
        final_mean = statistics.mean(throughputs_per_rep) if throughputs_per_rep else 0.0
        lr_pct = (final_mean / 1000.0) * 100

        summary[pkt_size] = {
            "reps":       rep,
            "mean_mbps":  round(final_mean, 2),
            "rsd_pct":    round(final_rsd, 2) if final_rsd is not None else None,
            "lr_pct":     round(lr_pct, 1),
            "converged":  converged,
        }

        rsd_str = f"{final_rsd:.1f}%" if final_rsd is not None else "N/A"
        print(f"  -> {rep} reps | {final_mean:.2f} Mbps "
              f"| RSD={rsd_str} | {lr_pct:.1f}% LR\n")

        kill_iperf_all(pairs)
        if pkt_size != PKT_SIZES[-1]:
            time.sleep(PKT_PAUSE)

    return summary


def run_experiment(experiment: str, topology: str, protocols: List[str], sizes: List[int],
                   output_dir_override: Optional[Path] = None) -> None:
    """
    Orquesta un experimento completo: itera todos los protocolos pedidos,
    y para cada uno hace el barrido de PKT_SIZES × reps.

    output_dir_override:
        Si se pasa, todos los JSON se escriben ahí.
        Si es None, se usa el default OUTPUT_BASE/{topology}/fase1_linerate.
    """
    pairs = TOPOLOGIES[topology]["pairs"][experiment]
    exp_config = EXPERIMENTS[experiment]
    note = exp_config["note"]
    desc = exp_config["desc"]
    topo_desc = TOPOLOGIES[topology]["desc"]

    # --- Resolver carpeta de salida ---
    if output_dir_override is not None:
        out_dir = output_dir_override
    else:
        out_dir = OUTPUT_BASE / topology
    out_dir.mkdir(parents=True, exist_ok=True)

    _print_header(experiment, topology, pairs, out_dir)

    summary: Dict[str, Dict[int, dict]] = {}

    for i, proto in enumerate(protocols):
        summary[proto] = run_sweep(pairs, topology, experiment, sizes,
                                   note, proto, out_dir)

        # Pausa entre protocolos (no después del último)
        if i < len(protocols) - 1:
            print(f"  ... proto-pause {PROTO_PAUSE}s antes del siguiente protocolo ...\n")
            time.sleep(PROTO_PAUSE)

    _print_summary_table(experiment, topology, summary)

    # --- Persistencia del summary ---
    summary_path = out_dir / f"{topology}_{experiment}_summary.json"
    summary_path.write_text(json.dumps({
        "experiment":             experiment,
        "topology":               topology,
        "protocols":              protocols,
        "rfc_reference":          "RFC8239 §2",
        "desc":                   desc,
        "topo_desc":              topo_desc,
        "cooldown_s":             COOLDOWN,
        "pkt_pause_s":            PKT_PAUSE,
        "proto_pause_s":          PROTO_PAUSE,
        "tcp_congestion_control": TCP_CONGESTION,
        "udp_bitrate":            UDP_BITRATE,
        "pairs": [{"id": p["id"], "client": p["client"],
                   "server": p["server"], "port": p["port"]} for p in pairs],
        "pkt_sizes":              PKT_SIZES,
        "rsd_target":             RSD_TARGET,
        "reps_min":               REPS_MIN,
        "reps_max":               REPS_MAX,
        "duration_s":             DURATION,
        "generated_utc":          datetime.now(timezone.utc).isoformat(),
        "results": {
            proto: {str(k): v for k, v in proto_summary.items()}
            for proto, proto_summary in summary.items()
        },
    }, indent=2))
    print(f"  Resumen: {summary_path}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  PREFLIGHT
# ══════════════════════════════════════════════════════════════════════════════

def preflight(experiment: str, topology: str) -> bool:
    """
    Valida por SSH que todos los hosts involucrados en el experimento tienen
    iperf3 disponible. Retorna True si todos responden.
    """
    pairs = TOPOLOGIES[topology]["pairs"][experiment]
    involved = sorted(
        {p["client"] for p in pairs} | {p["server"] for p in pairs}
    )
    print("-- Preflight check " + "-"*52)
    all_ok = True
    for name in involved:
        res = ssh_run(name, "iperf3 --version 2>&1 | head -1", timeout=10)
        ok = res.returncode == 0
        ver = res.stdout.strip()[:55] if ok else res.stderr.strip()[:55]
        status = "OK  " if ok else "FAIL"
        print(f"  {status} {name:4s}  {HOSTS[name]['ip']:15s}  {ver}")
        if not ok:
            all_ok = False
    print()
    return all_ok


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

ALL_EXPERIMENTS = ["a1", "a2", "a3", "a4"]


def _resolve_protocols(arg: str) -> List[str]:
    """Traduce --protocol {tcp,udp,both} a lista ordenada de protocolos."""
    if arg == "both":
        return list(PROTOCOLS)
    return [arg]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="F1: Line-Rate Testing — TCP + UDP  (SL vs J3C)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--experiment",
        choices=ALL_EXPERIMENTS + ["all"],
        required=True,
        help="Escenario a ejecutar (all = a1..a4 secuencialmente)",
    )
    parser.add_argument(
        "--topology",
        choices=list(TOPOLOGIES.keys()),
        default="sl",
        help="Topología: sl (spine-leaf) o j3c (jerárquica 3 capas)",
    )
    parser.add_argument(
        "--protocol",
        choices=["tcp", "udp", "both"],
        default="both",
        help="Protocolo(s) a evaluar. 'both' = TCP primero, luego UDP.",
    )
    parser.add_argument(
        '--sizes',
        default="64,128,256,512,1024,1400",
        type=str,
        help="List of the packet sizes to use."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Carpeta donde volcar TODOS los JSON (incluye summary). "
             "Si se omite, usa ~/experimentos/{topology}/fase1_linerate.",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Omite la validación SSH previa (usar solo si ya validaste manualmente).",
    )
    args = parser.parse_args()

    # --- Inicializar HOSTS globalmente según topología elegida ---
    global HOSTS
    HOSTS = TOPOLOGIES[args.topology]["hosts"]
    atexit.register(close_masters)

    # --- Resolver lista de experimentos y protocolos ---
    experiments = ALL_EXPERIMENTS if args.experiment == "all" else [args.experiment]
    protocols = _resolve_protocols(args.protocol)

    sizes = list(map(int, args.sizes.split(',')))
    for size in sizes:
        if size not in PKT_SIZES:
            print(f"Size {size} is not permitted")
            return

    # --- Banner inicial de la corrida completa ---
    print(f"\n{'#'*70}")
    print(f"#  F1 Line-Rate  —  Topología: {args.topology.upper()}")
    print(f"#  Experimentos: {experiments}")
    print(f"#  Sizes: {sizes}")
    print(f"#  Protocolos:   {protocols}")
    if args.output_dir is not None:
        print(f"#  Output dir:   {args.output_dir}")
    print(f"#  Inicio:       {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"{'#'*70}\n")

    t_start = time.time()

    for exp in experiments:
        if not args.skip_preflight:
            if not preflight(exp, args.topology):
                print("  FAIL: Preflight falló. Verifica SSH antes de continuar.")
                sys.exit(1)

        try:
            run_experiment(
                experiment=exp,
                topology=args.topology,
                protocols=protocols,
                sizes=sizes,
                output_dir_override=args.output_dir,
            )
        except KeyboardInterrupt:
            print(f"\n\n  Interrumpido por usuario durante F1-{exp.upper()}.")
            print("  Limpiando iperf3 en hosts involucrados...")
            pairs = TOPOLOGIES[args.topology]["pairs"][exp]
            kill_iperf_all(pairs)
            sys.exit(130)
        except Exception as e:
            print(f"\n  ERROR en F1-{exp.upper()}: {type(e).__name__} — {e}")
            print("  Continuando con el siguiente experimento...\n")
            continue

    elapsed = time.time() - t_start
    h, rem = divmod(int(elapsed), 3600)
    m, s = divmod(rem, 60)
    print(f"\n{'#'*70}")
    print( "#  Corrida F1 finalizada.")
    print(f"#  Tiempo total: {h:d}h {m:02d}m {s:02d}s")
    print(f"#  Fin:          {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"{'#'*70}\n")


if __name__ == "__main__":
    main()
