# Руководство по воспроизведению проблемы потери данных в Cassandra

## Описание проблемы

В продакшене наблюдалась следующая ситуация:

- **Кластер**: Cassandra 4.0.3, 2 DC × 3 ноды
- **Запись**: PySpark + Spark Cassandra Connector, `LOCAL_QUORUM`/`QUORUM`, `max.concurrent.writes=5-20`
- **Симптомы**: 
  - Запись завершалась успешно без исключений
  - При чтении обнаруживалась потеря части строк
  - Ноды не падали
  - В логах были странные сообщения (не указаны в описании)

**Гипотеза**: Перегруженная Cassandra молча дропает мутации (dropped mutations), но Spark Cassandra Connector не видит ошибок из-за асинхронной природы записи.

## Архитектура тестового окружения

```
┌─────────────────────────────────────────────────────────────────┐
│ minikube                                                        │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Cassandra Cluster (3 ноды)                                │  │
│  │  - cassandra-0, cassandra-1, cassandra-2                  │  │
│  │  - Cassandra 5.0.8 (из kubelauncher/cassandra chart)     │  │
│  │  - SimpleStrategy, RF=3                                   │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Ограничения (для создания перегрузки):                        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ • Disk throttling: 1 МБ/с (cgroups blkio)                │  │
│  │ • Network throttling: 0.5 МБ/с на порт 7000 (Calico)     │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                          ▲
                          │ NodePort 30000-30002
                          │
┌─────────────────────────┴───────────────────────────────────────┐
│ Хост-система                                                    │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Несколько параллельных процессов PySpark                 │  │
│  │  - Каждый пишет 100k-1M строк                            │  │
│  │  - Spark Cassandra Connector                              │  │
│  │  - LOCAL_QUORUM / QUORUM                                  │  │
│  │  - concurrent.writes = 5-20                               │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Предварительные требования

1. **Установленное ПО**:
   - `minikube`
   - `kubectl`
   - `helm`
   - `uv` (Python package manager)

2. **Запущенный minikube**:
   ```bash
   minikube start --cpus=4 --memory=8192
   ```

3. **Установленные зависимости Python**:
   ```bash
   cd /home/felix/Projects/cassandra_ddosing
   uv sync
   ```

## Сценарии воспроизведения

### Сценарий 1: Базовый (только дисковое дросселирование)

**Цель**: Воспроизвести потерю данных при перегрузке дисковой подсистемы.

```bash
# 1. Запустите Cassandra-кластер
cd cassandra/
./setup_cassandra.sh
cd ..

# 2. Дождитесь готовности всех нод
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=cassandra -n cassandra --timeout=300s

# 3. Установите жёсткое дисковое ограничение (1 МБ/с)
uv run cassandra_ddosing/manage_cassandra_limits.py limit-disk-speed 1

# 4. Запустите параллельную нагрузку (2 процесса по умолчанию)
./cassandra_ddosing/run_parallel_overload.sh

# 5. Проанализируйте результаты в выводе скрипта
```

**Ожидаемый результат**: Потеря 5-30% данных при записи с `LOCAL_QUORUM`.

### Сценарий 2: Усиленный (диск + сеть)

**Цель**: Максимально приблизиться к продакшн-условиям с ограничениями и диска, и сети.

```bash
# 1-2. Запустите кластер (как в Сценарии 1)

# 3. Установите Calico CNI (если ещё не установлен)
cd calico/
./install_calico.sh
cd ..

# 4. Установите ограничения диска и сети
uv run cassandra_ddosing/manage_cassandra_limits.py limit-disk-speed 1
uv run cassandra_ddosing/manage_cassandra_limits.py limit-net-speed 0.5

# 5. Запустите усиленную нагрузку (больше процессов, больше данных)
./cassandra_ddosing/run_parallel_overload.sh \
  --processes 3 \
  --batches 15 \
  --batch-size 100000 \
  --concurrent-writes 10

# 6. Проанализируйте результаты
```

**Ожидаемый результат**: Потеря 10-50% данных, более выраженная проблема.

### Сценарий 3: Вариация consistency level

**Цель**: Проверить, влияет ли уровень консистентности на потерю данных.

```bash
# 1-3. Запустите кластер и установите ограничения

# 4a. Тест с LOCAL_QUORUM (как в проде)
./cassandra_ddosing/run_parallel_overload.sh \
  --consistency-level LOCAL_QUORUM \
  --concurrent-writes 5

# 4b. Тест с QUORUM
./cassandra_ddosing/run_parallel_overload.sh \
  --consistency-level QUORUM \
  --concurrent-writes 5

# 4c. Тест с ALL (для сравнения)
./cassandra_ddosing/run_parallel_overload.sh \
  --consistency-level ALL \
  --concurrent-writes 5
```

**Ожидаемый результат**: 
- `LOCAL_QUORUM` и `QUORUM`: потери данных
- `ALL`: либо ошибки записи, либо меньше потерь (но медленнее)

### Сценарий 4: Одиночный процесс (контрольный)

**Цель**: Убедиться, что проблема возникает именно при параллельной нагрузке.

```bash
# 1-3. Запустите кластер и установите ограничения

# 4. Запустите один процесс
uv run cassandra_ddosing/run_overload_with_pyspark.py \
  --batches 20 \
  --batch-size 100000 \
  --consistency-level LOCAL_QUORUM \
  --concurrent-writes 5
```

**Ожидаемый результат**: Возможно, меньше потерь или их отсутствие.

## Параметры нагрузки

### `run_parallel_overload.sh`

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `--processes` | 2 | Количество параллельных процессов записи |
| `--batches` | 10 | Батчей данных на процесс |
| `--batch-size` | 100000 | Строк в одном батче |
| `--consistency-level` | LOCAL_QUORUM | Уровень консистентности (LOCAL_QUORUM, QUORUM, ONE, ALL) |
| `--concurrent-writes` | 5 | Параллельных записей в Spark Cassandra Connector |

### `run_overload_with_pyspark.py`

Те же параметры, но для одиночного процесса:

```bash
uv run cassandra_ddosing/run_overload_with_pyspark.py \
  --batches 10 \
  --batch-size 100000 \
  --consistency-level LOCAL_QUORUM \
  --concurrent-writes 5
```

## Диагностика и анализ

### 1. Проверка логов Cassandra

```bash
# Dropped mutations
kubectl logs -n cassandra cassandra-0 | grep -i "drop\|mutation"

# Timeouts
kubectl logs -n cassandra cassandra-0 | grep -i "timeout"

# Write failures
kubectl logs -n cassandra cassandra-0 | grep -i "write.*fail\|error"

# Все логи за последние 10 минут
kubectl logs -n cassandra cassandra-0 --since=10m
```

### 2. Метрики Cassandra

```bash
# Thread pool stats (показывает dropped mutations)
kubectl exec -n cassandra cassandra-0 -- nodetool tpstats

# Compaction stats
kubectl exec -n cassandra cassandra-0 -- nodetool compactionstats

# Статус кластера
kubectl exec -n cassandra cassandra-0 -- nodetool status

# Информация о таблице
kubectl exec -n cassandra cassandra-0 -- nodetool tablestats overload_ks.overload_tbl
```

### 3. Проверка текущих ограничений

```bash
# Disk throttling
minikube ssh -- "pgrep -f CassandraDaemon | while read pid; do \
  CG=\$(awk -F: '/blkio/{print \$3}' /proc/\$pid/cgroup | head -1); \
  cat /sys/fs/cgroup/blkio\$CG/blkio.throttle.write_bps_device; \
done"

# Network policy (Calico)
kubectl get globalnetworkpolicy cassandra-internode-bandwidth-limit -o yaml
```

### 4. Мониторинг в реальном времени

```bash
# Логи Cassandra в реальном времени
kubectl logs -n cassandra cassandra-0 -f

# Метрики I/O
minikube ssh -- "iostat -x 1"

# Метрики сети
minikube ssh -- "iftop -i eth0"

# Метрики CPU/Memory подов
kubectl top pods -n cassandra
```

## Анализ результатов

### Признаки успешного воспроизведения

1. **В выводе `run_parallel_overload.sh`**:
   ```
   ⚠️  ВОСПРОИЗВЕДЕНА ПРОБЛЕМА!
   Данные потеряны при записи с consistency level LOCAL_QUORUM
   ```

2. **В логах процессов** (`cassandra_ddosing/logs/*.log`):
   ```
   Статус: ✗ НЕСООТВЕТСТВИЕ — потеряно 15,234 строк (15.23%)
   ```

3. **В логах Cassandra**:
   ```
   WARN  [MutationStage-*] ... Dropped mutation ...
   ```

4. **В `nodetool tpstats`**:
   ```
   Pool Name                    Active   Pending      Completed   Blocked  All time blocked
   MutationStage                     0         0        1234567         0              5678
                                                                         ↑ Dropped mutations
   ```

### Что искать в логах

1. **Dropped mutations** — основной индикатор проблемы
2. **Write timeouts** — вторичный индикатор
3. **Compaction lag** — может указывать на перегрузку диска
4. **Heap pressure** — может вызывать GC паузы и дропы

### Типичные паттерны потерь

- **Равномерные пропуски**: Потери распределены равномерно по всему диапазону ID
- **Кластерные пропуски**: Потери сгруппированы в определённых диапазонах (указывает на временные проблемы)
- **Начальные/конечные пропуски**: Потери в начале или конце записи (указывает на проблемы при старте/завершении)

## Очистка окружения

### После каждого теста

```bash
# Снять ограничения
uv run cassandra_ddosing/manage_cassandra_limits.py unlimit-disk-speed
uv run cassandra_ddosing/manage_cassandra_limits.py unlimit-net-speed

# Очистить данные в Cassandra (опционально)
kubectl exec -n cassandra cassandra-0 -- cqlsh -u cassandra -p cassandra \
  -e "DROP KEYSPACE IF EXISTS overload_ks;"
```

### Полная очистка

```bash
# Удалить Cassandra-кластер
cd cassandra/
./delete_cassandra.sh
cd ..

# Удалить Calico (если установлен)
cd calico/
./delete_calico.sh
cd ..

# Удалить логи
rm -rf cassandra_ddosing/logs/*
```

## Рекомендуемая последовательность экспериментов

1. **Базовый тест** (Сценарий 1):
   - Убедиться, что инфраструктура работает
   - Получить baseline метрики

2. **Вариация нагрузки**:
   - Увеличить количество процессов (2 → 3 → 4)
   - Увеличить `concurrent-writes` (5 → 10 → 20)
   - Изменить размер батчей

3. **Вариация ограничений**:
   - Изменить disk throttling (1 → 0.5 → 0.1 МБ/с)
   - Добавить network throttling (Сценарий 2)

4. **Вариация consistency level** (Сценарий 3):
   - Сравнить поведение разных уровней

5. **Анализ логов**:
   - Корреляция между dropped mutations и потерянными строками
   - Временные паттерны (когда происходят дропы)

## Известные проблемы и ограничения

1. **Cassandra 5.0.8 vs 4.0.3**:
   - Тестовый кластер использует 5.0.8, продакшн — 4.0.3
   - Поведение может отличаться

2. **Single DC vs Multi DC**:
   - Тестовый кластер — single DC
   - Продакшн — 2 DC
   - `LOCAL_QUORUM` ведёт себя по-разному

3. **Размер кластера**:
   - Тест: 3 ноды
   - Продакшн: 6 нод (2×3)

4. **Ресурсы**:
   - minikube ограничен ресурсами хост-системы
   - Может потребоваться увеличить CPU/Memory для minikube

## Дальнейшие шаги

После успешного воспроизведения проблемы:

1. **Изучить конфигурацию Cassandra**:
   - Сравнить `cassandra.yaml` из прода с тестовым
   - Особое внимание на:
     - `concurrent_writes` (32 в проде)
     - `write_request_timeout_in_ms` (2000 в проде)
     - `commitlog_sync_period_in_ms` (10000 в проде)

2. **Тестировать решения**:
   - Увеличить `write_request_timeout_in_ms`
   - Уменьшить `concurrent_writes` в Spark
   - Добавить retry логику в Spark
   - Использовать `ALL` вместо `LOCAL_QUORUM` (с компромиссом по производительности)

3. **Мониторинг в проде**:
   - Настроить алерты на dropped mutations
   - Мониторить `nodetool tpstats`
   - Логировать все write timeouts

## Полезные ссылки

- [Cassandra Dropped Mutations](https://cassandra.apache.org/doc/latest/troubleshooting/dropped_mutations.html)
- [Spark Cassandra Connector Configuration](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/reference.md)
- [Cassandra Write Path](https://cassandra.apache.org/doc/latest/architecture/dynamo.html#dataset-partitioning-consistent-hashing)
