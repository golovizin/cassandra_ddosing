#!/usr/bin/env python3
"""
run_simple_overload.py

Записывает строки в Cassandra (~10 КБ каждая) с заданным лимитом скорости,
затем читает все ID обратно и считает пропуски — потерянные мутации.

Зависимости:
    pip install cassandra-driver   (или: uv add cassandra-driver)

Запуск:
    python run_simple_overload.py
    python run_simple_overload.py --rows 500000 --rate 1.5
"""

import argparse
import time
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args

# ── Подключение ───────────────────────────────────────────────────────────────
HOST = "minikube-docker"
PORT = 30000
USERNAME = "cassandra"
PASSWORD = "cassandra"
LOCAL_DC = "datacenter1"
KEYSPACE = "overload_ks"
TABLE = "overload_tbl"

# ── Нагрузка (можно переопределить через CLI) ─────────────────────────────────
DEFAULT_TOTAL_ROWS = 300_000  # строк по умолчанию
DEFAULT_RATE_LIMIT_MBPS = 30.0  # МБ/с, лимит скорости записи

# ── Внутренние параметры ──────────────────────────────────────────────────────
ROW_SIZE_BYTES = 10_000  # приблизительный размер одной строки (2 × 5000 байт текста)
CONCURRENCY = 64  # параллельных запросов в полёте одновременно
CHUNK_SIZE = 1_000  # строк за одну «порцию»; sleep считается после каждой порции

# ~5 КБ текста × 2 поля = ~10 КБ на строку (ASCII, 1 байт/символ)
_TEXT = "x" * 5_000


# ─────────────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cassandra write-overload demo")
    p.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_TOTAL_ROWS,
        help=f"Количество строк для записи (по умолчанию {DEFAULT_TOTAL_ROWS:,})",
    )
    p.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_RATE_LIMIT_MBPS,
        help=f"Лимит скорости записи, МБ/с (по умолчанию {DEFAULT_RATE_LIMIT_MBPS})",
    )
    return p.parse_args()


def connect() -> tuple:
    auth = PlainTextAuthProvider(USERNAME, PASSWORD)
    cluster = Cluster(
        contact_points=[HOST],
        port=PORT,
        auth_provider=auth,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=LOCAL_DC),
        protocol_version=4,
    )
    session = cluster.connect()
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    return cluster, session


def wait_for_table(session, keyspace: str, table: str, timeout: float = 30.0) -> None:
    """Ждёт, пока таблица станет доступна на всех нодах (schema propagation)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = list(session.execute(
            "SELECT table_name FROM system_schema.tables "
            "WHERE keyspace_name=%s AND table_name=%s",
            (keyspace, table),
        ))
        if rows:
            return
        time.sleep(0.2)
    raise TimeoutError(f"Таблица {keyspace}.{table} не появилась за {timeout:.0f}с")


def setup_schema(session) -> None:
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        f"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}"
    )
    session.set_keyspace(KEYSPACE)
    session.execute(f"DROP TABLE IF EXISTS {TABLE}")
    session.execute(
        f"""
        CREATE TABLE {TABLE} (
            id    bigint PRIMARY KEY,
            num1  int,
            num2  int,
            num3  int,
            data1 text,
            data2 text
        )
    """
    )
    wait_for_table(session, KEYSPACE, TABLE)


def write_rows(session, total_rows: int, rate_mbps: float) -> int:
    """
    Записывает total_rows строк с ограничением скорости rate_mbps МБ/с.
    Возвращает количество ошибок (dropped mutations).
    """
    rate_bps = rate_mbps * 1024 * 1024  # байт/с
    ops_per_sec = rate_bps / ROW_SIZE_BYTES  # оп/с при заданном лимите
    chunk_target_s = CHUNK_SIZE / ops_per_sec  # желаемое время на одну порцию

    print(
        f"  Лимит: {rate_mbps} МБ/с  →  {ops_per_sec:.0f} оп/с  "
        f"→  {CHUNK_SIZE} строк / порцию за {chunk_target_s:.2f} с"
    )

    stmt = session.prepare(f"INSERT INTO {TABLE} (id, num1, num2, num3, data1, data2)" f" VALUES (?, ?, ?, ?, ?, ?)")
    stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM

    total_errors = 0
    t0 = time.monotonic()
    last_report = t0

    for chunk_start in range(0, total_rows, CHUNK_SIZE):
        chunk_end = min(chunk_start + CHUNK_SIZE, total_rows)
        chunk_size = chunk_end - chunk_start

        # ── Время начала порции — для расчёта sleep ───────────────────────────
        chunk_t0 = time.monotonic()

        args = [(i, i % 1_000, i % 500, i % 100, _TEXT, _TEXT) for i in range(chunk_start, chunk_end)]
        results = execute_concurrent_with_args(session, stmt, args, concurrency=CONCURRENCY, raise_on_first_error=False)
        first_exc = None
        chunk_errors = 0
        for ok, val in results:
            if not ok:
                chunk_errors += 1
                if first_exc is None:
                    first_exc = val
        if first_exc is not None:
            print(f"    [exc] {type(first_exc).__name__}: {first_exc}")
        total_errors += chunk_errors

        # ── Ограничение скорости: ждём до конца расчётного интервала ─────────
        chunk_elapsed = time.monotonic() - chunk_t0
        # chunk_target_s масштабируем на фактический размер последней порции
        target = chunk_target_s * (chunk_size / CHUNK_SIZE)
        sleep_s = target - chunk_elapsed
        if sleep_s > 0:
            time.sleep(sleep_s)

        # ── Прогресс каждые 5 секунд ──────────────────────────────────────────
        now = time.monotonic()
        if now - last_report >= 5.0:
            elapsed = now - t0
            rate = chunk_end / elapsed
            pct = chunk_end / total_rows * 100
            print(f"  {chunk_end:>9,} / {total_rows:,}  ({pct:5.1f}%)" f"  {rate:6.0f} оп/с  ошибки: {total_errors:,}")
            last_report = now

    elapsed = time.monotonic() - t0
    written = total_rows - total_errors
    print(f"\n  Итого за {elapsed:.1f} с:")
    print(f"    Записано:  {written:,}")
    print(f"    Ошибок:    {total_errors:,}  ({total_errors / total_rows * 100:.2f}%)" f"  ← dropped mutations")
    return total_errors


def verify_rows(session, total_rows: int) -> None:
    """Читает все ID из таблицы и выводит пропуски."""
    print(f"  SELECT id FROM {TABLE}  (постраничное чтение)...")
    t0 = time.monotonic()

    found = bytearray(total_rows)  # found[i] == 1 → строка с id=i найдена
    extra_count = 0

    rows = session.execute(f"SELECT id FROM {KEYSPACE}.{TABLE}", timeout=600)
    for row in rows:
        if 0 <= row.id < total_rows:
            found[row.id] = 1
        else:
            extra_count += 1

    elapsed = time.monotonic() - t0
    found_count = sum(found)
    missing = [i for i, f in enumerate(found) if not f]

    print(f"  Чтение: {elapsed:.1f} с\n")
    print(f"  Ожидалось строк:  {total_rows:,}")
    print(f"  Найдено:          {found_count:,}")
    print(f"  Пропусков:        {len(missing):,}  ← потерянные мутации")
    if extra_count:
        print(f"  Лишних ID:        {extra_count:,}  (вне диапазона 0..{total_rows - 1})")
    if missing:
        print(f"\n  Первые пропущенные ID: {missing[:30]}")


# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    args = parse_args()

    print("=== Cassandra overload demo ===\n")
    print(f"  Строк:   {args.rows:,}")
    print(f"  Лимит:   {args.rate} МБ/с\n")

    print(f"[connect] {HOST}:{PORT}  dc={LOCAL_DC}")
    cluster, session = connect()
    print("[connect] OK\n")

    print("[1/3] Создание схемы (DROP + CREATE TABLE)...")
    setup_schema(session)
    print("[1/3] OK\n")

    print(f"[2/3] Запись {args.rows:,} строк (~{ROW_SIZE_BYTES // 1024} КБ каждая)...")
    write_rows(session, args.rows, args.rate)

    print(f"\n[3/3] Верификация...")
    verify_rows(session, args.rows)

    cluster.shutdown()
    print("\nГотово.")


if __name__ == "__main__":
    main()
