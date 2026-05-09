# cassandra_ddosing
Здесь мы попытаемся DDOSить Cassandra DB так, чтобы она начала затирать данные до их записи на диск

## Структура проекта

```
cassandra_ddosing/
├── README.md                          # Этот файл
└── cassandra/                         # Конфигурация Cassandra-кластера для minikube
    ├── cassandra_values.yaml          # Helm values для настройки Cassandra (ресурсы, persistence, JVM)
    ├── cassandra_nodeports.yaml       # Манифест NodePort-сервисов для доступа к каждой ноде извне minikube
    ├── setup_cassandra.sh             # Скрипт установки Cassandra-кластера (3 ноды) в minikube
    ├── delete_cassandra.sh            # Скрипт удаления Cassandra-кластера из minikube
    ├── setup_minikube.sh              # Скрипт для первоначальной настройки minikube (если нужно)
    └── delete_minikube.sh             # Скрипт для удаления minikube (если нужно)
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

## Как запускать

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

