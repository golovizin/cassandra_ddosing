#!/usr/bin/env python3
"""
run_overload_with_pyspark.py

Нагрузочный тест Cassandra через PySpark + Spark Cassandra Connector.

Команды:
    # Создать keyspace
    uv run cassandra_ddosing/run_overload_with_pyspark.py create-keyspace overload_ks
    
    # Создать таблицу
    uv run cassandra_ddosing/run_overload_with_pyspark.py create-table overload_ks.overload_tbl
    
    # Запустить нагрузку
    uv run cassandra_ddosing/run_overload_with_pyspark.py run-overload overload_ks.overload_tbl \
        --batches 10 --batch-size 100000 --consistency-level LOCAL_QUORUM --concurrent-writes 5
"""

import os
import sys
import time

import click
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

# ── Параметры нагрузки ────────────────────────────────────────────────────────
DEFAULT_DF_BATCH_SIZE = 100_000
DEFAULT_DF_COUNT = 10
DDL_TIMEOUT = 300  # секунд — DDL под дросселированием диска выполняется долго

# ~490 символов × 2 поля ≈ 980 байт текста + числовые поля ≈ 1 КБ на строку
_STR1 = "a" * 490
_STR2 = "b" * 490


# ── Подключение к Cassandra ───────────────────────────────────────────────────


def cassandra_connect():
    """Создаёт подключение к Cassandra."""
    auth = PlainTextAuthProvider(USERNAME, PASSWORD)
    cluster = Cluster(
        contact_points=[HOST],
        port=PORT,
        auth_provider=auth,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=LOCAL_DC),
        protocol_version=4,
        idle_heartbeat_interval=0,
        connect_timeout=60,
    )
    session = cluster.connect()
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    session.default_timeout = DDL_TIMEOUT
    return cluster, session


def wait_for_table(session, keyspace: str, table: str, timeout: float = DDL_TIMEOUT) -> None:
    """Ждёт schema propagation таблицы по всем нодам."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = list(
            session.execute(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s AND table_name=%s",
                (keyspace, table),
            )
        )
        if rows:
            return
        time.sleep(0.3)
    raise TimeoutError(f"Таблица {keyspace}.{table} не появилась за {timeout:.0f} с")


# ── Шаг 2: создание Spark-сессии ──────────────────────────────────────────────


def build_spark(consistency_level: str = "LOCAL_QUORUM", concurrent_writes: int = 8) -> SparkSession:
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
        .config("spark.cassandra.output.concurrent.writes", str(concurrent_writes))
        .config("spark.cassandra.output.consistency.level", consistency_level)
        .config("spark.cassandra.input.consistency.level", consistency_level)
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


# ── CLI ───────────────────────────────────────────────────────────────────────


@click.group()
def cli():
    """Нагрузочный тест Cassandra через PySpark + Spark Cassandra Connector."""
    pass


@cli.command(name="create-keyspace")
@click.argument("keyspace_name")
@click.option("--replication-factor", default=2, help="Replication factor (по умолчанию 2)")
@click.option("--drop-if-exists", is_flag=True, help="Удалить keyspace если существует")
def create_keyspace(keyspace_name: str, replication_factor: int, drop_if_exists: bool):
    """Создаёт keyspace в Cassandra."""
    click.echo(f"{'=' * 70}")
    click.echo(f"Создание keyspace: {keyspace_name}")
    click.echo(f"{'=' * 70}")
    click.echo(f"Replication factor: {replication_factor}")
    click.echo(f"Drop if exists: {drop_if_exists}")
    click.echo(f"{'=' * 70}\n")

    cluster, session = cassandra_connect()
    
    try:
        if drop_if_exists:
            click.echo(f"Удаление keyspace {keyspace_name} (если существует)...")
            session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_name}", timeout=DDL_TIMEOUT)
            click.echo("✓ Удалён\n")
        
        click.echo(f"Создание keyspace {keyspace_name}...")
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} "
            f"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}",
            timeout=DDL_TIMEOUT,
        )
        click.echo("✓ Создан\n")
        
        click.echo(f"{'=' * 70}")
        click.echo(f"✓ Keyspace {keyspace_name} готов")
        click.echo(f"{'=' * 70}")
    finally:
        cluster.shutdown()


@cli.command(name="create-table")
@click.argument("table_path")
@click.option("--drop-if-exists", is_flag=True, help="Удалить таблицу если существует")
def create_table(table_path: str, drop_if_exists: bool):
    """
    Создаёт таблицу в Cassandra.
    
    TABLE_PATH в формате keyspace.table (например, overload_ks.overload_tbl)
    """
    if "." not in table_path:
        click.echo(f"✗ Ошибка: TABLE_PATH должен быть в формате keyspace.table", err=True)
        sys.exit(1)
    
    keyspace, table = table_path.split(".", 1)
    
    click.echo(f"{'=' * 70}")
    click.echo(f"Создание таблицы: {keyspace}.{table}")
    click.echo(f"{'=' * 70}")
    click.echo(f"Drop if exists: {drop_if_exists}")
    click.echo(f"{'=' * 70}\n")

    cluster, session = cassandra_connect()
    
    try:
        session.set_keyspace(keyspace)
        
        if drop_if_exists:
            click.echo(f"Удаление таблицы {table} (если существует)...")
            session.execute(f"DROP TABLE IF EXISTS {table}", timeout=DDL_TIMEOUT)
            click.echo("✓ Удалена\n")
        
        click.echo(f"Создание таблицы {table}...")
        session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
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
        click.echo("✓ Создана\n")
        
        click.echo("Ожидание schema propagation...")
        wait_for_table(session, keyspace, table)
        click.echo("✓ Schema propagated\n")
        
        click.echo(f"{'=' * 70}")
        click.echo(f"✓ Таблица {keyspace}.{table} готова")
        click.echo(f"{'=' * 70}")
    finally:
        cluster.shutdown()


@cli.command(name="run-overload")
@click.argument("table_path")
@click.option("--batches", default=DEFAULT_DF_COUNT, help=f"Количество батчей (по умолчанию {DEFAULT_DF_COUNT})")
@click.option("--batch-size", default=DEFAULT_DF_BATCH_SIZE, help=f"Строк в батче (по умолчанию {DEFAULT_DF_BATCH_SIZE:,})")
@click.option(
    "--consistency-level",
    type=click.Choice(["LOCAL_QUORUM", "QUORUM", "ONE", "ALL"], case_sensitive=False),
    default="LOCAL_QUORUM",
    help="Consistency level для записи",
)
@click.option("--concurrent-writes", default=8, help="Параллельных записей в Spark Cassandra Connector")
def run_overload(table_path: str, batches: int, batch_size: int, consistency_level: str, concurrent_writes: int):
    """
    Запускает нагрузочный тест записи в Cassandra.
    
    TABLE_PATH в формате keyspace.table (например, overload_ks.overload_tbl)
    """
    if "." not in table_path:
        click.echo(f"✗ Ошибка: TABLE_PATH должен быть в формате keyspace.table", err=True)
        sys.exit(1)
    
    keyspace, table = table_path.split(".", 1)
    total_rows = batches * batch_size

    click.echo("=" * 70)
    click.echo("Cassandra PySpark overload")
    click.echo("=" * 70)
    click.echo(f"Таблица:           {keyspace}.{table}")
    click.echo(f"Батчей:            {batches}")
    click.echo(f"Строк/батч:        {batch_size:>12,}")
    click.echo(f"Итого:             {total_rows:>12,}")
    click.echo(f"Consistency level: {consistency_level}")
    click.echo(f"Concurrent writes: {concurrent_writes}")
    click.echo("=" * 70)
    click.echo()

    # Spark
    click.echo("[1/3] Создание Spark-сессии...")
    spark = build_spark(consistency_level=consistency_level, concurrent_writes=concurrent_writes)
    click.echo(f"[1/3] ✓ OK  (Spark {spark.version})\n")

    # Список датафреймов
    click.echo("[2/3] Формирование списка датафреймов...")
    dataframes = [make_dataframe(spark, i, batch_size) for i in range(batches)]
    click.echo(f"[2/3] ✓ Сформировано {len(dataframes)} датафреймов\n")

    # Запись в Cassandra
    click.echo("[3/3a] Запись датафреймов в Cassandra (append)...")
    t_write_start = time.monotonic()
    for i, df in enumerate(dataframes):
        t0 = time.monotonic()
        df.write.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).mode("append").save()
        elapsed = time.monotonic() - t0
        click.echo(f"  Батч {i + 1}/{batches}: {batch_size:,} строк за {elapsed:.1f} с")

    total_write = time.monotonic() - t_write_start
    click.echo(f"  Всего запись: {total_write:.1f} с\n")

    # Верификация
    click.echo("[3/3b] Чтение и верификация...")
    t0 = time.monotonic()
    result_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    actual = result_df.count()
    elapsed = time.monotonic() - t0

    expected = total_rows
    ok = actual == expected
    click.echo(f"  Чтение: {elapsed:.1f} с")
    click.echo(f"  Ожидалось: {expected:,}")
    click.echo(f"  Получено:  {actual:,}")
    
    if ok:
        click.echo("  Статус: ✓ OK — все строки на месте")
    else:
        lost = expected - actual
        loss_pct = lost / expected * 100
        click.echo(f"  Статус: ✗ НЕСООТВЕТСТВИЕ — потеряно {lost:,} строк ({loss_pct:.2f}%)")
        click.echo(f"\n  ⚠️  ВОСПРОИЗВЕДЕНА ПРОБЛЕМА: данные потеряны при записи с {consistency_level}!")
        
        # Детальная диагностика
        click.echo("\n  Анализ пропущенных строк...")
        all_ids = result_df.select("id").rdd.map(lambda r: r.id).collect()
        all_ids_set = set(all_ids)
        expected_ids = set(range(1, total_rows + 1))
        missing_ids = sorted(expected_ids - all_ids_set)
        
        if missing_ids:
            click.echo(f"  Первые 20 пропущенных ID: {missing_ids[:20]}")
            click.echo(f"  Последние 20 пропущенных ID: {missing_ids[-20:]}")
            
            gaps = []
            for i in range(1, min(len(missing_ids), 100)):
                gaps.append(missing_ids[i] - missing_ids[i - 1])
            if gaps:
                avg_gap = sum(gaps) / len(gaps)
                click.echo(f"  Средний интервал между пропусками (первые 100): {avg_gap:.1f}")

    spark.stop()
    click.echo("\nГотово.")


if __name__ == "__main__":
    cli()
