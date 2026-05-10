#!/usr/bin/env bash
# run_parallel_overload.sh
#
# Запускает несколько параллельных процессов записи в Cassandra через PySpark
# для воспроизведения проблемы потери данных в перегруженном кластере.
#
# Стратегия:
#   1. Создаёт один keyspace
#   2. Создаёт N таблиц (по одной на процесс)
#   3. Запускает N параллельных процессов записи (каждый в свою таблицу)
#
# Использование:
#   ./cassandra_ddosing/run_parallel_overload.sh
#   ./cassandra_ddosing/run_parallel_overload.sh --processes 3 --batches 5 --batch-size 50000

set -euo pipefail

# ── Параметры по умолчанию ────────────────────────────────────────────────────
KEYSPACE="overload_ks"         # Keyspace для тестов
PROCESSES=2                    # Количество параллельных процессов записи
BATCHES=10                     # Батчей на процесс
BATCH_SIZE=100000              # Строк в батче
CONSISTENCY_LEVEL="LOCAL_QUORUM"  # Уровень консистентности
CONCURRENT_WRITES=5            # Параллельных записей в Spark Cassandra Connector
REPLICATION_FACTOR=2           # Replication factor для keyspace

# ── Парсинг аргументов ────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --keyspace)
      KEYSPACE="$2"
      shift 2
      ;;
    --processes)
      PROCESSES="$2"
      shift 2
      ;;
    --batches)
      BATCHES="$2"
      shift 2
      ;;
    --batch-size)
      BATCH_SIZE="$2"
      shift 2
      ;;
    --consistency-level)
      CONSISTENCY_LEVEL="$2"
      shift 2
      ;;
    --concurrent-writes)
      CONCURRENT_WRITES="$2"
      shift 2
      ;;
    --replication-factor)
      REPLICATION_FACTOR="$2"
      shift 2
      ;;
    *)
      echo "Неизвестный параметр: $1"
      echo "Использование: $0 [--keyspace KS] [--processes N] [--batches N] [--batch-size N] [--consistency-level LEVEL] [--concurrent-writes N] [--replication-factor N]"
      exit 1
      ;;
  esac
done

# ── Расчёт общей нагрузки ─────────────────────────────────────────────────────
TOTAL_ROWS=$((PROCESSES * BATCHES * BATCH_SIZE))
TOTAL_MB=$((TOTAL_ROWS * 1 / 1024))  # ~1 КБ на строку

echo "═══════════════════════════════════════════════════════════════════════════"
echo "  Параллельная нагрузка на Cassandra через PySpark"
echo "═══════════════════════════════════════════════════════════════════════════"
echo "  Keyspace:          $KEYSPACE"
echo "  Replication factor:$REPLICATION_FACTOR"
echo "  Процессов:         $PROCESSES"
echo "  Батчей/процесс:    $BATCHES"
echo "  Строк/батч:        $(printf "%'d" $BATCH_SIZE)"
echo "  Consistency level: $CONSISTENCY_LEVEL"
echo "  Concurrent writes: $CONCURRENT_WRITES"
echo "───────────────────────────────────────────────────────────────────────────"
echo "  Итого строк:       $(printf "%'d" $TOTAL_ROWS)"
echo "  Примерный объём:   ~${TOTAL_MB} МБ"
echo "═══════════════════════════════════════════════════════════════════════════"
echo ""

# ── Создание директории для логов ─────────────────────────────────────────────
LOG_DIR="cassandra_ddosing/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SESSION_ID="${TIMESTAMP}_p${PROCESSES}_b${BATCHES}_bs${BATCH_SIZE}"

echo "📁 Логи будут сохранены в: $LOG_DIR/"
echo "   Префикс сессии: $SESSION_ID"
echo ""

# ── Создание keyspace ─────────────────────────────────────────────────────────
echo "🔧 [1/2] Создание keyspace и таблиц..."
echo ""

echo "  Создание keyspace $KEYSPACE (RF=$REPLICATION_FACTOR)..."
uv run cassandra_ddosing/run_overload_with_pyspark.py create-keyspace "$KEYSPACE" \
  --replication-factor "$REPLICATION_FACTOR" \
  --drop-if-exists

if [ $? -ne 0 ]; then
  echo "✗ Ошибка при создании keyspace"
  exit 1
fi

echo ""

# ── Создание таблиц ───────────────────────────────────────────────────────────
echo "  Создание $PROCESSES таблиц..."
for i in $(seq 1 $PROCESSES); do
  TABLE_NAME="overload_tbl_${i}"
  echo "    [$i/$PROCESSES] Создание таблицы $KEYSPACE.$TABLE_NAME..."
  
  uv run cassandra_ddosing/run_overload_with_pyspark.py create-table "$KEYSPACE.$TABLE_NAME" \
    --drop-if-exists > /dev/null 2>&1
  
  if [ $? -ne 0 ]; then
    echo "    ✗ Ошибка при создании таблицы $KEYSPACE.$TABLE_NAME"
    exit 1
  fi
  
  echo "    ✓ Таблица $KEYSPACE.$TABLE_NAME создана"
done

echo ""
echo "✓ Keyspace и таблицы готовы"
echo ""

# ── Запуск параллельных процессов ─────────────────────────────────────────────
echo "🚀 [2/2] Запуск $PROCESSES параллельных процессов записи..."
echo ""

PIDS=()
for i in $(seq 1 $PROCESSES); do
  TABLE_NAME="overload_tbl_${i}"
  LOG_FILE="$LOG_DIR/${SESSION_ID}_process_${i}.log"
  
  echo "  [Процесс $i/$PROCESSES] Запуск записи в $KEYSPACE.$TABLE_NAME..."
  echo "    Лог: $LOG_FILE"
  
  uv run cassandra_ddosing/run_overload_with_pyspark.py run-overload "$KEYSPACE.$TABLE_NAME" \
    --batches "$BATCHES" \
    --batch-size "$BATCH_SIZE" \
    --consistency-level "$CONSISTENCY_LEVEL" \
    --concurrent-writes "$CONCURRENT_WRITES" \
    > "$LOG_FILE" 2>&1 &
  
  PID=$!
  PIDS+=($PID)
  echo "    PID: $PID"
done

echo ""
echo "✓ Все процессы запущены"
echo "  PIDs: ${PIDS[*]}"
echo ""

# ── Мониторинг выполнения ─────────────────────────────────────────────────────
echo "⏳ Ожидание завершения процессов..."
echo "   (Ctrl+C для прерывания)"
echo ""

START_TIME=$(date +%s)

# Функция для проверки статуса процессов
check_processes() {
  local running=0
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      running=$((running + 1))
    fi
  done
  echo $running
}

# Периодически выводим статус
while [ $(check_processes) -gt 0 ]; do
  RUNNING=$(check_processes)
  ELAPSED=$(($(date +%s) - START_TIME))
  
  echo "  [$(date +%H:%M:%S)] Активных процессов: $RUNNING/$PROCESSES  (прошло ${ELAPSED}s)"
  
  sleep 5
done

END_TIME=$(date +%s)
TOTAL_TIME=$((END_TIME - START_TIME))

echo ""
echo "═══════════════════════════════════════════════════════════════════════════"
echo "  ✓ Все процессы завершены"
echo "═══════════════════════════════════════════════════════════════════════════"
echo "  Общее время: ${TOTAL_TIME}s"
echo ""

# ── Анализ результатов ────────────────────────────────────────────────────────
echo "📊 Анализ результатов..."
echo ""

SUCCESS_COUNT=0
FAIL_COUNT=0
TOTAL_LOST=0

for i in $(seq 1 $PROCESSES); do
  LOG_FILE="$LOG_DIR/${SESSION_ID}_process_${i}.log"
  
  echo "  [Процесс $i/$PROCESSES]"
  
  # Проверяем, завершился ли процесс успешно
  if grep -q "Готово\." "$LOG_FILE" 2>/dev/null; then
    # Проверяем, были ли потери данных
    if grep -q "НЕСООТВЕТСТВИЕ" "$LOG_FILE" 2>/dev/null; then
      FAIL_COUNT=$((FAIL_COUNT + 1))
      
      # Извлекаем количество потерянных строк
      LOST=$(grep "потеряно" "$LOG_FILE" | grep -oP '\d+(?= строк)' | head -1)
      if [ -n "$LOST" ]; then
        TOTAL_LOST=$((TOTAL_LOST + LOST))
        echo "    ✗ ПОТЕРЯНО ДАННЫХ: $LOST строк"
      else
        echo "    ✗ ПОТЕРЯНО ДАННЫХ (количество не определено)"
      fi
    else
      SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
      echo "    ✓ OK - все строки на месте"
    fi
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    echo "    ✗ ОШИБКА - процесс не завершился корректно"
    
    # Показываем последние строки лога
    echo "    Последние строки лога:"
    tail -n 3 "$LOG_FILE" | sed 's/^/      /'
  fi
done

echo ""
echo "═══════════════════════════════════════════════════════════════════════════"
echo "  ИТОГОВАЯ СТАТИСТИКА"
echo "═══════════════════════════════════════════════════════════════════════════"
echo "  Успешных процессов:  $SUCCESS_COUNT/$PROCESSES"
echo "  С потерями данных:   $FAIL_COUNT/$PROCESSES"

if [ $TOTAL_LOST -gt 0 ]; then
  LOSS_PCT=$(awk "BEGIN {printf \"%.2f\", ($TOTAL_LOST / $TOTAL_ROWS) * 100}")
  echo "  Всего потеряно строк: $(printf "%'d" $TOTAL_LOST) из $(printf "%'d" $TOTAL_ROWS) ($LOSS_PCT%)"
  echo ""
  echo "  ⚠️  ПРОБЛЕМА ВОСПРОИЗВЕДЕНА!"
  echo "     Данные потеряны при записи с consistency level $CONSISTENCY_LEVEL"
else
  echo "  Всего потеряно строк: 0"
  echo ""
  echo "  ✓ Проблема НЕ воспроизведена (все данные на месте)"
fi

echo "═══════════════════════════════════════════════════════════════════════════"
echo ""
echo "📁 Подробные логи: $LOG_DIR/${SESSION_ID}_process_*.log"
echo ""

# ── Рекомендации по анализу ───────────────────────────────────────────────────
if [ $TOTAL_LOST -gt 0 ]; then
  echo "🔍 Рекомендации по дальнейшему анализу:"
  echo ""
  echo "  1. Проверьте логи Cassandra на наличие dropped mutations:"
  echo "     kubectl logs -n cassandra cassandra-0 | grep -i 'drop\\|mutation\\|timeout'"
  echo ""
  echo "  2. Проверьте метрики Cassandra:"
  echo "     kubectl exec -n cassandra cassandra-0 -- nodetool tpstats"
  echo ""
  echo "  3. Проверьте текущие ограничения диска:"
  echo "     uv run cassandra_ddosing/manage_cassandra_limits.py --help"
  echo ""
  echo "  4. Изучите детальные логи процессов:"
  echo "     less $LOG_DIR/${SESSION_ID}_process_1.log"
  echo ""
fi
