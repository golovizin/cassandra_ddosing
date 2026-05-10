# cassandra_ddosing

Проект для воспроизведения проблемы потери данных (dropped mutations) в перегруженной Cassandra DB.

## Описание проблемы

В продакшене наблюдалась ситуация:
- Запись данных через PySpark + Spark Cassandra Connector с `LOCAL_QUORUM`/`QUORUM`
- Запись завершалась успешно без исключений
- При чтении обнаруживалась потеря части строк
- Cassandra-ноды не падали

**Цель проекта**: Воспроизвести эту проблему в контролируемом окружении (minikube) путём создания перегрузки дисковой и сетевой подсистем.

## Быстрый старт

```bash
# 1. Запустите Cassandra-кластер (3 ноды)
cd cassandra/ && ./setup_cassandra.sh && cd ..

# 2. Установите дисковое ограничение (1 МБ/с)
uv run cassandra_ddosing/manage_cassandra_limits.py limit-disk-speed 1

# 3. Запустите параллельную нагрузку
./cassandra_ddosing/run_parallel_overload.sh

# 4. Проверьте результаты в выводе
```

📖 **Подробнее**: см. [`QUICKSTART.md`](QUICKSTART.md) и [`REPRODUCTION_GUIDE.md`](REPRODUCTION_GUIDE.md)

## Структура проекта

```
cassandra_ddosing/
├── README.md                          # Этот файл
├── QUICKSTART.md                      # Быстрый старт (основные команды)
├── REPRODUCTION_GUIDE.md              # Подробное руководство по воспроизведению проблемы
├── cassandra/                         # Конфигурация Cassandra-кластера для minikube
│   ├── cassandra_values.yaml          # Helm values для настройки Cassandra (ресурсы, persistence, JVM)
│   ├── cassandra_nodeports.yaml       # Манифест NodePort-сервисов для доступа к каждой ноде извне minikube
│   ├── setup_cassandra.sh             # Скрипт установки Cassandra-кластера (3 ноды) в minikube
│   ├── delete_cassandra.sh            # Скрипт удаления Cassandra-кластера из minikube
│   ├── setup_minikube.sh              # Скрипт для первоначальной настройки minikube (если нужно)
│   └── delete_minikube.sh             # Скрипт для удаления minikube (если нужно)
├── calico/                            # Calico CNI для network throttling
│   ├── install_calico.sh              # Установка Calico в minikube
│   └── delete_calico.sh               # Удаление Calico
└── cassandra_ddosing/                 # Скрипты нагрузочного тестирования
    ├── manage_cassandra_limits.py     # Управление disk/network throttling
    ├── run_overload_with_pyspark.py   # Одиночный процесс записи через PySpark
    ├── run_parallel_overload.sh       # Параллельная нагрузка (несколько процессов)
    ├── run_simple_overload.py         # Простая запись через Python Cassandra Driver
    └── logs/                          # Логи тестов
```

### Описание файлов в `cassandra/`

- **`cassandra_values.yaml`** — конфигурация Helm chart для Cassandra:
  - Настройки кластера (имя, дата-центр, rack)
  - Количество реплик (3 ноды)
  - Аутентификация (логин/пароль: cassandra/cassandra)
  - Ресурсы CPU/Memory
  - Настройки персистентного хранилища (8Gi на ноду)
  - Настройки JVM (heap size)

- **`cassandra_nodeports.yaml`** — Kubernetes-манифест для создания NodePort-сервисов:
  - Создаёт отдельный NodePort для каждой ноды Cassandra (порты 30000-30002)
  - Позволяет подключаться к конкретной ноде извне minikube

- **`setup_cassandra.sh`** — скрипт установки Cassandra:
  - Проверяет статус minikube
  - Создаёт namespace `cassandra`
  - Устанавливает Cassandra через Helm chart (kubelauncher/cassandra)
  - Применяет NodePort-сервисы
  - Ожидает готовности всех подов
  - Проверяет доступность CQL
  - Выводит информацию о кластере и способы подключения

- **`delete_cassandra.sh`** — скрипт удаления Cassandra:
  - Удаляет NodePort-сервисы
  - Удаляет Helm release
  - Удаляет PVC (Persistent Volume Claims)
  - Удаляет namespace

- **`setup_minikube.sh`** — скрипт для настройки minikube (опционально)

- **`delete_minikube.sh`** — скрипт для удаления minikube (опционально)

### Описание файлов в `cassandra_ddosing/`

- **`manage_cassandra_limits.py`** — управление ограничениями диска и сети:
  - `limit-disk-speed <MB/s>` — установить disk throttling через cgroups
  - `unlimit-disk-speed` — снять disk throttling
  - `limit-net-speed <MB/s>` — установить network throttling через Calico
  - `unlimit-net-speed` — снять network throttling

- **`run_overload_with_pyspark.py`** — одиночный процесс записи через PySpark:
  - Использует Spark Cassandra Connector
  - Поддерживает настройку consistency level и concurrent writes
  - Встроенная верификация данных (проверяет потери)
  - Детальная диагностика пропущенных строк

- **`run_parallel_overload.sh`** — параллельная нагрузка (несколько процессов):
  - Запускает несколько процессов `run_overload_with_pyspark.py` одновременно
  - Имитирует продакшн-сценарий с конкурентной записью
  - Автоматический анализ результатов и статистика потерь
  - Сохраняет логи каждого процесса

- **`run_simple_overload.py`** — простая запись через Python Cassandra Driver:
  - Альтернатива PySpark для отладки
  - Прямая работа с Cassandra через cassandra-driver
  - Контроль скорости записи

## Как запускать

### Базовый сценарий (рекомендуется для первого запуска)

```bash
# 1. Установите зависимости
uv sync

# 2. Запустите Cassandra
cd cassandra/
./setup_cassandra.sh
cd ..

# 3. Установите дисковое ограничение
uv run cassandra_ddosing/manage_cassandra_limits.py limit-disk-speed 1

# 4. Запустите параллельную нагрузку
./cassandra_ddosing/run_parallel_overload.sh

# 5. Проверьте результаты
# Скрипт автоматически покажет статистику потерь данных
```

### Продвинутые сценарии

См. подробное руководство в [`REPRODUCTION_GUIDE.md`](REPRODUCTION_GUIDE.md)

## Как запускать (старая инструкция для справки)

### Предварительные требования

- Установленный `minikube`
- Установленный `kubectl`
- Установленный `helm`

### Порядок запуска

1. **Запуск minikube** (если ещё не запущен):
   ```bash
   minikube start
   ```

2. **Установка Cassandra-кластера**:
   ```bash
   cd cassandra/
   ./setup_cassandra.sh
   ```
   
   Скрипт выполнит:
   - Проверку статуса minikube
   - Установку Cassandra (3 ноды) через Helm
   - Создание NodePort-сервисов
   - Ожидание готовности всех подов
   - Проверку доступности CQL
   - Вывод информации о кластере

3. **Проверка статуса кластера**:
   ```bash
   kubectl get pods -n cassandra
   kubectl exec -n cassandra cassandra-0 -- nodetool status
   ```

4. **Подключение к Cassandra**:
   
   Из minikube:
   ```bash
   kubectl exec -it -n cassandra cassandra-0 -- cqlsh -u cassandra -p cassandra
   ```
   
   Извне minikube (через NodePort):
   ```bash
   MINIKUBE_IP=$(minikube ip)
   cqlsh $MINIKUBE_IP 30000 -u cassandra -p cassandra
   ```

5. **Удаление Cassandra-кластера** (когда закончите):
   ```bash
   cd cassandra/
   ./delete_cassandra.sh
   ```

### Полезные команды

- Просмотр логов ноды:
  ```bash
  kubectl logs -n cassandra cassandra-0 -f
  ```

- Просмотр всех ресурсов в namespace:
  ```bash
  kubectl get all -n cassandra
  ```

- Проверка статуса кластера:
  ```bash
  kubectl exec -n cassandra cassandra-0 -- nodetool status
  ```

- Просмотр информации о NodePort-сервисах:
  ```bash
  kubectl get svc -n cassandra | grep NodePort
  ```

### Доступ к нодам

После установки доступны следующие NodePort-порты для подключения к каждой ноде:

- **Нода 0**: `<minikube-ip>:30000`
- **Нода 1**: `<minikube-ip>:30001`
- **Нода 2**: `<minikube-ip>:30002`

Получить IP minikube:
```bash
minikube ip
```

