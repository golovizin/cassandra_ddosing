# Быстрый старт: Воспроизведение проблемы потери данных в Cassandra

## TL;DR

```bash
# 1. Запустите Cassandra
cd cassandra/ && ./setup_cassandra.sh && cd ..

# 2. Установите ограничения
uv run cassandra_ddosing/manage_cassandra_limits.py limit-disk-speed 1

# 3. Запустите нагрузку
./cassandra_ddosing/run_parallel_overload.sh

# 4. Проверьте результаты в выводе
```

## Основные команды

### Управление кластером

```bash
# Запуск Cassandra (3 ноды)
cd cassandra/ && ./setup_cassandra.sh && cd ..

# Проверка статуса
kubectl get pods -n cassandra
kubectl exec -n cassandra cassandra-0 -- nodetool status

# Удаление кластера
cd cassandra/ && ./delete_cassandra.sh && cd ..
```

### Управление ограничениями

```bash
# Установить disk throttling (1 МБ/с)
uv run cassandra_ddosing/manage_cassandra_limits.py limit-disk-speed 1

# Снять disk throttling
uv run cassandra_ddosing/manage_cassandra_limits.py unlimit-disk-speed

# Установить network throttling (требует Calico)
cd calico/ && ./install_calico.sh && cd ..
uv run cassandra_ddosing/manage_cassandra_limits.py limit-net-speed 0.5

# Снять network throttling
uv run cassandra_ddosing/manage_cassandra_limits.py unlimit-net-speed
```

### Запуск нагрузки

```bash
# Базовый тест (2 процесса, LOCAL_QUORUM, concurrent_writes=5)
./cassandra_ddosing/run_parallel_overload.sh

# Усиленный тест (3 процесса, больше данных)
./cassandra_ddosing/run_parallel_overload.sh \
  --processes 3 \
  --batches 15 \
  --batch-size 100000 \
  --concurrent-writes 10

# Тест с QUORUM
./cassandra_ddosing/run_parallel_overload.sh \
  --consistency-level QUORUM

# Одиночный процесс (для отладки)
uv run cassandra_ddosing/run_overload_with_pyspark.py \
  --batches 10 \
  --batch-size 100000 \
  --consistency-level LOCAL_QUORUM \
  --concurrent-writes 5
```

### Диагностика

```bash
# Dropped mutations в логах
kubectl logs -n cassandra cassandra-0 | grep -i "drop\|mutation"

# Thread pool stats (показывает dropped mutations)
kubectl exec -n cassandra cassandra-0 -- nodetool tpstats

# Логи в реальном времени
kubectl logs -n cassandra cassandra-0 -f

# Просмотр логов тестов
ls -lh cassandra_ddosing/logs/
less cassandra_ddosing/logs/$(ls -t cassandra_ddosing/logs/ | head -1)
```

## Типичные сценарии

### Сценарий 1: Первый запуск

```bash
# Убедитесь, что minikube запущен
minikube status

# Установите зависимости
uv sync

# Запустите базовый тест
cd cassandra/ && ./setup_cassandra.sh && cd ..
uv run cassandra_ddosing/manage_cassandra_limits.py limit-disk-speed 1
./cassandra_ddosing/run_parallel_overload.sh
```

### Сценарий 2: Максимальная нагрузка

```bash
# Установите и диск, и сеть
cd calico/ && ./install_calico.sh && cd ..
uv run cassandra_ddosing/manage_cassandra_limits.py limit-disk-speed 1
uv run cassandra_ddosing/manage_cassandra_limits.py limit-net-speed 0.5

# Запустите 4 процесса с высоким concurrent_writes
./cassandra_ddosing/run_parallel_overload.sh \
  --processes 4 \
  --batches 10 \
  --batch-size 100000 \
  --concurrent-writes 20
```

### Сценарий 3: Отладка

```bash
# Запустите один процесс с подробным выводом
uv run cassandra_ddosing/run_overload_with_pyspark.py \
  --batches 5 \
  --batch-size 50000 \
  --consistency-level LOCAL_QUORUM \
  --concurrent-writes 5

# Одновременно смотрите логи Cassandra
kubectl logs -n cassandra cassandra-0 -f
```

## Признаки успешного воспроизведения

✅ **Проблема воспроизведена**, если видите:

```
═══════════════════════════════════════════════════════════════════════════
  ИТОГОВАЯ СТАТИСТИКА
═══════════════════════════════════════════════════════════════════════════
  Успешных процессов:  0/2
  С потерями данных:   2/2
  Всего потеряно строк: 45,678 из 200,000 (22.84%)

  ⚠️  ПРОБЛЕМА ВОСПРОИЗВЕДЕНА!
     Данные потеряны при записи с consistency level LOCAL_QUORUM
═══════════════════════════════════════════════════════════════════════════
```

И в логах Cassandra:

```bash
kubectl exec -n cassandra cassandra-0 -- nodetool tpstats
# Смотрите колонку "All time blocked" в MutationStage — там будут dropped mutations
```

## Очистка после тестов

```bash
# Снять ограничения
uv run cassandra_ddosing/manage_cassandra_limits.py unlimit-disk-speed
uv run cassandra_ddosing/manage_cassandra_limits.py unlimit-net-speed

# Удалить данные (опционально)
kubectl exec -n cassandra cassandra-0 -- cqlsh -u cassandra -p cassandra \
  -e "DROP KEYSPACE IF EXISTS overload_ks;"

# Полная очистка
cd cassandra/ && ./delete_cassandra.sh && cd ..
cd calico/ && ./delete_calico.sh && cd ..
rm -rf cassandra_ddosing/logs/*
```

## Troubleshooting

### Cassandra не запускается

```bash
# Проверьте ресурсы minikube
minikube status
kubectl top nodes

# Увеличьте ресурсы
minikube delete
minikube start --cpus=4 --memory=8192
```

### Нет потерь данных

Попробуйте:
1. Уменьшить disk throttling: `0.5` или `0.1` МБ/с
2. Увеличить количество процессов: `--processes 4`
3. Увеличить `concurrent-writes`: `--concurrent-writes 20`
4. Добавить network throttling

### Ошибки при записи

Это нормально при очень жёстких ограничениях. Попробуйте:
1. Увеличить disk throttling до `2-5` МБ/с
2. Уменьшить `concurrent-writes` до `3-5`
3. Уменьшить количество процессов

## Дополнительная информация

Подробное руководство: `REPRODUCTION_GUIDE.md`
