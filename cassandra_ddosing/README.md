# Cassandra Disk Throttling Script

Скрипт для управления ограничениями скорости записи на диск для Cassandra-подов в minikube через cgroups v1 blkio throttling.

## Описание

`set_cassandra_throttling.py` позволяет динамически устанавливать и снимать ограничения на скорость записи на диск для процессов Cassandra, работающих в Kubernetes (minikube). Это полезно для тестирования поведения Cassandra при ограниченной пропускной способности дисковой подсистемы.

**Расположение**: Скрипт находится в директории `cassandra_ddosing/` вместе с этим README. Все команды ниже предполагают, что вы находитесь в этой директории.

## Требования

- Python 3.7+
- minikube с запущенным Cassandra-кластером
- kubectl, настроенный для работы с minikube
- Пакеты Python: `click`

## Установка зависимостей

```bash
uv sync
```

## Использование

### Базовые команды

#### Установить ограничение скорости записи

```bash
# Установить лимит 1 МБ/с для всех Cassandra-подов
python set_cassandra_throttling.py set-limit 1

# Установить лимит 5 МБ/с
python set_cassandra_throttling.py set-limit 5

# Установить лимит 0.5 МБ/с
python set_cassandra_throttling.py set-limit 0.5
```

#### Снять ограничение скорости записи

```bash
# Снять лимит для всех Cassandra-подов
python set_cassandra_throttling.py unset-limit
```

### Дополнительные опции

#### Работа с конкретным подом

```bash
# Установить лимит только для cassandra-0
python set_cassandra_throttling.py set-limit 1 --pod cassandra-0

# Снять лимит только для cassandra-1
python set_cassandra_throttling.py unset-limit --pod cassandra-1
```

#### Указание namespace

```bash
# Работа с подами в другом namespace
python set_cassandra_throttling.py set-limit 1 --namespace my-cassandra
```

#### Режим dry-run

```bash
# Показать команды без выполнения
python set_cassandra_throttling.py set-limit 1 --dry-run
python set_cassandra_throttling.py unset-limit --dry-run
```

### Примеры использования

```bash
# Сценарий 1: Тестирование деградации производительности
python set_cassandra_throttling.py set-limit 10
# ... запуск нагрузочных тестов ...
python set_cassandra_throttling.py set-limit 1
# ... наблюдение за поведением ...
python set_cassandra_throttling.py unset-limit

# Сценарий 2: Постепенное снижение лимита
python set_cassandra_throttling.py set-limit 10
sleep 60
python set_cassandra_throttling.py set-limit 5
sleep 60
python set_cassandra_throttling.py set-limit 1
sleep 60
python set_cassandra_throttling.py unset-limit

# Сценарий 3: Тестирование отдельных нод
python set_cassandra_throttling.py set-limit 1 --pod cassandra-0
python set_cassandra_throttling.py set-limit 5 --pod cassandra-1
# cassandra-2 остаётся без ограничений
```

## Как это работает

### Архитектура решения

```
┌─────────────────────────────────────────────────────────────┐
│ Хост-система (minikube)                                     │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Kubernetes Node                                       │  │
│  │                                                       │  │
│  │  ┌────────────────┐  ┌────────────────┐             │  │
│  │  │ Pod cassandra-0│  │ Pod cassandra-1│             │  │
│  │  │                │  │                │             │  │
│  │  │ ┌────────────┐ │  │ ┌────────────┐ │             │  │
│  │  │ │ Cassandra  │ │  │ │ Cassandra  │ │             │  │
│  │  │ │ Process    │ │  │ │ Process    │ │             │  │
│  │  │ │ PID: 12345 │ │  │ │ PID: 23456 │ │             │  │
│  │  │ └────────────┘ │  │ └────────────┘ │             │  │
│  │  └────────────────┘  └────────────────┘             │  │
│  │         │                    │                       │  │
│  │         │ cgroup             │ cgroup                │  │
│  │         ▼                    ▼                       │  │
│  │  ┌─────────────────────────────────────────────┐    │  │
│  │  │ /sys/fs/cgroup/blkio/kubepods/...           │    │  │
│  │  │                                              │    │  │
│  │  │ blkio.throttle.write_bps_device:            │    │  │
│  │  │   259:5 1048576  ← Наш лимит (1 МБ/с)      │    │  │
│  │  └─────────────────────────────────────────────┘    │  │
│  │                                                       │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                             │
│  Блочное устройство: /dev/nvme0n1 (259:5)                 │
└─────────────────────────────────────────────────────────────┘
         ▲
         │ minikube ssh + bash script
         │
┌────────┴─────────┐
│ Наш скрипт       │
│ Python + click   │
└──────────────────┘
```

### Пошаговый процесс

1. **Поиск подов**
   - Скрипт использует `kubectl get pods` с фильтром по label `app.kubernetes.io/name=cassandra`
   - Получает список имён подов в указанном namespace

2. **Подключение к minikube**
   - Использует `minikube ssh` для выполнения команд на хост-системе minikube
   - Передаёт bash-скрипт через heredoc для избежания проблем с экранированием

3. **Определение блочного устройства**
   ```bash
   # Получаем раздел, на котором смонтирован /var/lib/kubelet
   DEV_PART=$(df /var/lib/kubelet | awk 'NR==2{print $1}')
   # Результат: /dev/nvme0n1p2
   
   # Убираем номер раздела для получения базового устройства
   # NVMe: /dev/nvme0n1p2 → /dev/nvme0n1
   # SATA: /dev/sda1 → /dev/sda
   if echo "$DEV_PART" | grep -q 'nvme'; then
       DEV=$(echo "$DEV_PART" | sed 's/p[0-9]*$//')
   else
       DEV=$(echo "$DEV_PART" | sed 's/[0-9]*$//')
   fi
   ```

4. **Получение major:minor номеров устройства**
   ```bash
   # Через ls -l получаем major:minor
   MAJOR_MINOR=$(ls -l "$DEV" | awk '{print $5":"$6}' | sed 's/,//')
   # Результат: 259:5
   ```

5. **Поиск процессов Cassandra**
   ```bash
   # Находим все процессы с "cassandra" в командной строке
   pgrep -f cassandra
   # Результат: список PID (например, 103994, 104757, 106353)
   ```

6. **Определение cgroup для каждого процесса**
   ```bash
   # Читаем файл /proc/$pid/cgroup
   CG=$(awk -F: '/blkio/{print $3}' /proc/$pid/cgroup | head -1)
   # Результат: /kubepods/burstable/pod<UUID>/<container-id>
   ```

7. **Применение throttling**
   ```bash
   # Записываем лимит в файл cgroup
   echo "259:5 1048576" > /sys/fs/cgroup/blkio$CG/blkio.throttle.write_bps_device
   # Где:
   #   259:5 - major:minor устройства
   #   1048576 - лимит в байтах/сек (1 МБ/с)
   ```

### Технические детали

#### Cgroups v1 blkio throttling

Скрипт использует механизм cgroups v1 для ограничения I/O:

- **Файл управления**: `/sys/fs/cgroup/blkio/<cgroup-path>/blkio.throttle.write_bps_device`
- **Формат записи**: `<major>:<minor> <bytes_per_sec>`
- **Снятие ограничения**: `<major>:<minor> 0`

#### Почему используется базовое устройство?

Cgroups blkio throttling работает только с базовыми блочными устройствами, а не с разделами:
- ✅ Работает: `/dev/nvme0n1` (259:5)
- ❌ Не работает: `/dev/nvme0n1p2` (259:7)

#### Обработка разных типов дисков

Скрипт автоматически определяет тип устройства:

| Тип диска | Пример раздела | Базовое устройство | Regex |
|-----------|----------------|-------------------|-------|
| SATA/SCSI | `/dev/sda1` | `/dev/sda` | `sed 's/[0-9]*$//'` |
| NVMe | `/dev/nvme0n1p2` | `/dev/nvme0n1` | `sed 's/p[0-9]*$//'` |
| VirtIO | `/dev/vda1` | `/dev/vda` | `sed 's/[0-9]*$//'` |

### Ограничения и особенности

1. **Работает только с minikube**
   - Скрипт использует `minikube ssh` для доступа к хост-системе
   - Для других Kubernetes-кластеров потребуется модификация

2. **Требуется sudo на хосте**
   - Запись в cgroups требует root-прав
   - minikube по умолчанию разрешает sudo без пароля

3. **Применяется ко всем процессам Cassandra**
   - Скрипт находит все процессы с "cassandra" в имени
   - Применяет throttling к каждому найденному процессу

4. **Лимит на уровне устройства**
   - Ограничение применяется к конкретному блочному устройству
   - Все поды на одном устройстве используют общий лимит

5. **Только запись**
   - Скрипт ограничивает только операции записи (`write_bps_device`)
   - Чтение не ограничивается

## Проверка работы

### Просмотр текущих ограничений

```bash
# Подключиться к minikube
minikube ssh

# Найти процессы Cassandra
pgrep -f cassandra

# Для каждого PID посмотреть cgroup
cat /proc/<PID>/cgroup | grep blkio

# Посмотреть текущий лимит
cat /sys/fs/cgroup/blkio/<cgroup-path>/blkio.throttle.write_bps_device
```

### Тестирование эффекта

```bash
# Установить низкий лимит
python set_cassandra_throttling.py set-limit 1

# В другом терминале: запустить запись в Cassandra
# Например, через cqlsh или cassandra-stress

# Наблюдать за метриками I/O
minikube ssh -- "iostat -x 1"

# Снять лимит
python set_cassandra_throttling.py unset-limit
```

## Устранение неполадок

### Скрипт не находит поды

**Проблема**: `✗ Cassandra-поды не найдены`

**Решение**:
```bash
# Проверить, что поды запущены
kubectl get pods -n cassandra

# Проверить labels
kubectl get pods -n cassandra --show-labels

# Если label другой, использовать --namespace
python set_cassandra_throttling.py set-limit 1 --namespace <your-namespace>
```

### Ошибка определения устройства

**Проблема**: `ERROR: Cannot determine device partition`

**Решение**:
```bash
# Проверить, что /var/lib/kubelet существует в minikube
minikube ssh -- "df /var/lib/kubelet"

# Если используется другая точка монтирования, нужно модифицировать скрипт
```

### Ошибка записи в cgroup

**Проблема**: `No such file or directory` при записи в cgroup

**Решение**:
```bash
# Проверить версию cgroups
minikube ssh -- "mount | grep cgroup"

# Скрипт поддерживает только cgroups v1
# Если используется v2, потребуется модификация
```

## Разработка и отладка

### Режим dry-run

Используйте `--dry-run` для просмотра команд без выполнения:

```bash
python set_cassandra_throttling.py set-limit 1 --dry-run
```

Это покажет bash-скрипт, который будет выполнен.

### Логирование

Скрипт выводит подробную информацию о каждом шаге:
- Найденные поды
- Определённое устройство и его major:minor
- Каждый обработанный PID
- Результаты операций

### Модификация скрипта

Основные функции для модификации:

- `get_cassandra_pods()` - поиск подов
- `set_write_throttle()` - установка лимита
- `unset_write_throttle()` - снятие лимита

Bash-скрипт находится в переменной `bash_cmd` внутри этих функций.

## См. также

- [Linux cgroups documentation](https://www.kernel.org/doc/Documentation/cgroup-v1/blkio-controller.txt)
- [Kubernetes resource management](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- [Cassandra performance tuning](https://cassandra.apache.org/doc/latest/operating/hardware.html)

## Лицензия

Этот скрипт создан для внутреннего использования в проекте cassandra_ddosing.
