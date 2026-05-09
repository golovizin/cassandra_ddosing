#!/usr/bin/env bash
#
# run_overload.sh
#
# Демонстрирует потерю данных в Cassandra при commitlog_sync=periodic
# и дисковом лимите 0.2 МБ/с — без краша нод.
#
# Механизм:
#   - Широкие строки ~7 КБ (5×1 КБ текст + 1 КБ blob + числа)
#   - 16 потоков @ CYCLERATE оп/с >> пропускная способность диска
#   - commitlog_sync=periodic → ACK клиенту до записи на диск
#   → часть мутаций отбрасывается из native transport queue (MUTATION_REQ dropped)
#   → ноды выглядят здоровыми (UN, нет рестартов)
#
# Подключение: minikube-docker:30000 (cassandra/cassandra)
#

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NB5="${SCRIPT_DIR}/nb5"
WORKLOAD="${SCRIPT_DIR}/overload_workload.yaml"
LOG_DIR="${SCRIPT_DIR}/logs"

HOST="minikube-docker"
PORT="30000"
LOCAL_DC="datacenter1"
USERNAME="cassandra"
PASSWORD="cassandra"

# Количество строк для записи и последующей верификации.
WRITE_CYCLES=500000
# Ограничитель скорости записи (оп/с).
# CYCLERATE × 7 КБ × 3 реплики должно >> дискового лимита (0.2 МБ/с),
# чтобы native transport queue переполнялась → MUTATION_REQ dropped.
CYCLERATE=200
# Параллельные потоки nb5 на стороне клиента.
THREADS=16

mkdir -p "${LOG_DIR}"

if [[ ! -f "${WORKLOAD}" ]]; then
    echo "✗ Файл рабочей нагрузки не найден: ${WORKLOAD}" >&2
    exit 1
fi

# Общие аргументы подключения для всех вызовов nb5
NB5_CONN=(
    driver=cqld4
    workload="${WORKLOAD}"
    host="${HOST}"
    port="${PORT}"
    localdc="${LOCAL_DC}"
    username="${USERNAME}"
    password="${PASSWORD}"
    --logs-dir "${LOG_DIR}"
)

# ─────────────────────────────────────────────────────────────────────────────
# 1. Ожидание готовности кластера
# ─────────────────────────────────────────────────────────────────────────────
echo ">>> Ожидание готовности кластера Cassandra..."
until [ "$(kubectl get pods -n cassandra -l app.kubernetes.io/name=cassandra \
    -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null \
    | grep -o True | wc -l)" -eq 3 ]; do
    echo "  $(date +%H:%M:%S) ноды ещё не готовы..."
    sleep 5
done
echo "✓ Кластер готов"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 2. DDL: создание keyspace
# ─────────────────────────────────────────────────────────────────────────────
echo ">>> [1/4] Создание keyspace overload_ks (RF=3)..."
"${NB5}" run "${NB5_CONN[@]}" \
    tags==block:schema_ks \
    cycles=1 \
    threads=1 \
    request_timeout=30
echo "✓ Keyspace создан"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 3. DDL: создание таблицы
# ─────────────────────────────────────────────────────────────────────────────
echo ">>> [2/4] Создание таблицы overload_ks.wide_table..."
"${NB5}" run "${NB5_CONN[@]}" \
    tags==block:schema_tbl \
    cycles=1 \
    threads=1 \
    request_timeout=30
echo "✓ Таблица создана"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 4. Нагрузка: запись строк с ограничением скорости
# ─────────────────────────────────────────────────────────────────────────────
echo ">>> [3/4] OVERLOAD: ${WRITE_CYCLES} строк @ ${CYCLERATE} оп/с"
echo "    ${CYCLERATE} × 7 КБ × 3 реплики = ~$(( CYCLERATE * 7 * 3 / 1024 )) МБ/с >> 0.2 МБ/с дискового лимита"
echo "    Ожидаемое время: ~$(( WRITE_CYCLES / CYCLERATE / 60 )) мин"
echo ""
"${NB5}" run "${NB5_CONN[@]}" \
    tags==block:main \
    cycles="${WRITE_CYCLES}" \
    threads="${THREADS}" \
    cyclerate="${CYCLERATE}" \
    errors=warn \
    --progress console:30s
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 5. Верификация: читаем те же ключи обратно
# ─────────────────────────────────────────────────────────────────────────────
echo ">>> [4/4] VERIFY: читаем ${WRITE_CYCLES} ключей обратно..."
echo "    Строки, потерянные из-за MUTATION_REQ drop, вернут пустой результат (errors=count)."
echo ""
"${NB5}" run "${NB5_CONN[@]}" \
    tags==block:read \
    cycles="${WRITE_CYCLES}" \
    threads=8 \
    errors=count \
    --progress console:30s
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 6. Итоговое состояние таблицы
# ─────────────────────────────────────────────────────────────────────────────
echo ">>> Итоговое состояние таблицы:"
kubectl exec -n cassandra cassandra-0 -- nodetool tablestats overload_ks 2>&1 \
    | grep -E "Write Count|Read Count|Memtable|SSTable count|partitions" || true
