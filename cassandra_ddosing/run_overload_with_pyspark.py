#!/usr/bin/env python3
"""
run_overload_with_pyspark.py

Нагрузочный тест Cassandra через PySpark + Spark Cassandra Connector.

Запуск:
    python cassandra_ddosing/run_overload_with_pyspark.py
    python cassandra_ddosing/run_overload_with_pyspark.py --batches 5 --batch-size 50000
"""

import argparse
import os
import time

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ── Подключение ───────────────────────────────────────────────────────────────
HOST = "minikube-docker"
PORT = 30000
USERNAME = "cassandra"
PASSWORD = "cassandra"
LOCAL_DC = "datacenter1"
KEYSPACE = "overload_ks"
TABLE = "overload_tbl"

# ── Параметры нагрузки ────────────────────────────────────────────────────────
DEFAULT_DF_BATCH_SIZE = 100_000
DEFAULT_DF_COUNT = 10

# ~490 символов × 2 поля ≈ 980 байт текста + числовые поля ≈ 1 КБ на строку
_STR1 = "a" * 490
_STR2 = "b" * 490


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Cassandra PySpark overload test")
    p.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_DF_BATCH_SIZE,
        help=f"Строк в каждом датафрейме (по умолчанию {DEFAULT_DF_BATCH_SIZE:,})",
    )
    p.add_argument(
        "--batches",
        type=int,
        default=DEFAULT_DF_COUNT,
        help=f"Количество датафреймов (по умолчанию {DEFAULT_DF_COUNT})",
    )
    return p.parse_args()


# ── Шаг 1: создание схемы через Python Cassandra Driver ───────────────────────


def cassandra_connect():
    auth = PlainTextAuthProvider(USERNAME, PASSWORD)
    cluster = Cluster(
        contact_points=[HOST],
        port=PORT,
        auth_provider=auth,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=LOCAL_DC),
        protocol_version=4,
        # При дросселировании диска Cassandra отвечает медленно;
        # отключаем heartbeat чтобы соединение не признавалось defunct.
        idle_heartbeat_interval=0,
        connect_timeout=60,
    )
    session = cluster.connect()
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    session.default_timeout = 300  # 5 минут — DDL на медленном диске
    return cluster, session


DDL_TIMEOUT = 300  # секунд — DDL под дросселированием диска выполняется долго


def setup_schema(session) -> None:
    session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}", timeout=DDL_TIMEOUT)
    session.execute(
        f"CREATE KEYSPACE {KEYSPACE} "
        f"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 2}}",
        timeout=DDL_TIMEOUT,
    )
    session.set_keyspace(KEYSPACE)
    session.execute(f"DROP TABLE IF EXISTS {TABLE}", timeout=DDL_TIMEOUT)
    session.execute(
        f"""
        CREATE TABLE {TABLE} (
            id    bigint PRIMARY KEY,
            num1  int,
            num2  int,
            num3  int,
            str1  text,
            str2  text
        )
        """,
        timeout=DDL_TIMEOUT,
    )
    # Ждём schema propagation по всем нодам
    deadline = time.monotonic() + DDL_TIMEOUT
    while time.monotonic() < deadline:
        rows = list(
            session.execute(
                "SELECT table_name FROM system_schema.tables " "WHERE keyspace_name=%s AND table_name=%s",
                (KEYSPACE, TABLE),
            )
        )
        if rows:
            return
        time.sleep(0.3)
    raise TimeoutError(f"Таблица {KEYSPACE}.{TABLE} не появилась за {DDL_TIMEOUT} с")


# ── Шаг 2: создание Spark-сессии ──────────────────────────────────────────────


def build_spark() -> SparkSession:
    jars_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars")
    jars = ",".join(os.path.join(jars_dir, f) for f in sorted(os.listdir(jars_dir)) if f.endswith(".jar"))

    # SCC поддерживает список хостов через запятую, но один порт для всех.
    # Все три NodePort (30000/30001/30002) ведут в один кластер — используем
    # первый как контактную точку; остальные ноды драйвер обнаружит сам.
    return (
        SparkSession.builder.master("local[8]")
        .appName("CassandraOverload")
        .enableHiveSupport()
        .config("spark.jars", jars)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Spark Cassandra Connector
        .config("spark.cassandra.connection.host", HOST)
        .config("spark.cassandra.connection.port", str(PORT))
        .config("spark.cassandra.output.concurrent.writes", "8")
        .config("spark.cassandra.output.consistency.level", "LOCAL_QUORUM")
        .config("spark.cassandra.input.consistency.level", "LOCAL_QUORUM")
        .config("spark.cassandra.auth.username", USERNAME)
        .config("spark.cassandra.auth.password", PASSWORD)
        # read.timeout_ms управляет таймаутом и записи тоже (10 минут)
        .config("spark.cassandra.read.timeout_ms", "600000")
        .getOrCreate()
    )


# ── Шаг 3: генерация датафреймов ──────────────────────────────────────────────


def make_dataframe(spark: SparkSession, batch_idx: int, batch_size: int):
    """
    Датафрейм batch_size строк.
    id: batch_idx*batch_size+1 .. (batch_idx+1)*batch_size (включительно).
    """
    start_id = batch_idx * batch_size + 1
    return (
        spark.range(start_id, start_id + batch_size)
        .withColumn("num1", (F.col("id") % 1000).cast(IntegerType()))
        .withColumn("num2", (F.col("id") % 500).cast(IntegerType()))
        .withColumn("num3", (F.col("id") % 100).cast(IntegerType()))
        .withColumn("str1", F.lit(_STR1))
        .withColumn("str2", F.lit(_STR2))
        .select("id", "num1", "num2", "num3", "str1", "str2")
    )


# ── main ──────────────────────────────────────────────────────────────────────


def main():
    args = parse_args()
    df_batch_size: int = args.batch_size
    df_count: int = args.batches
    total_rows = df_batch_size * df_count

    print("=== Cassandra PySpark overload ===\n")
    print(f"  Батчей:    {df_count}")
    print(f"  Строк/батч:{df_batch_size:>12,}")
    print(f"  Итого:     {total_rows:>12,}\n")

    # 1) Схема
    print("[1/4] Подключение к Cassandra и создание схемы...")
    cluster, session = cassandra_connect()
    setup_schema(session)
    cluster.shutdown()
    print("[1/4] OK\n")

    # 2) Spark
    print("[2/4] Создание Spark-сессии...")
    spark = build_spark()
    print(f"[2/4] OK  (Spark {spark.version})\n")

    # 3) Список датафреймов (ленивые планы — данные не материализуются)
    print("[3/4] Формирование списка датафреймов...")
    dataframes = [make_dataframe(spark, i, df_batch_size) for i in range(df_count)]
    print(f"[3/4] Сформировано {len(dataframes)} датафреймов\n")

    # 4) Запись в Cassandra
    print("[4/4a] Запись датафреймов в Cassandra (append)...")
    t_write_start = time.monotonic()
    for i, df in enumerate(dataframes):
        t0 = time.monotonic()
        (
            df.write.format("org.apache.spark.sql.cassandra")
            .options(table=TABLE, keyspace=KEYSPACE)
            .mode("append")
            .save()
        )
        elapsed = time.monotonic() - t0
        print(f"  Батч {i + 1}/{df_count}: {df_batch_size:,} строк за {elapsed:.1f} с")

    total_write = time.monotonic() - t_write_start
    print(f"  Всего запись: {total_write:.1f} с\n")

    # 5) Верификация
    print("[4/4b] Чтение и верификация...")
    t0 = time.monotonic()
    result_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=TABLE, keyspace=KEYSPACE).load()
    actual = result_df.count()
    elapsed = time.monotonic() - t0

    expected = total_rows
    ok = actual == expected
    print(f"  Чтение: {elapsed:.1f} с")
    print(f"  Ожидалось: {expected:,}")
    print(f"  Получено:  {actual:,}")
    if ok:
        print("  Статус: OK — все строки на месте")
    else:
        lost = expected - actual
        print(f"  Статус: НЕСООТВЕТСТВИЕ — потеряно {lost:,} строк ({lost / expected * 100:.2f}%)")

    spark.stop()
    print("\nГотово.")


if __name__ == "__main__":
    main()
