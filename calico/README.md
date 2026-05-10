# Calico CNI для ограничения сетевого трафика Cassandra

Этот каталог содержит скрипты для установки и настройки Calico CNI в minikube для ограничения пропускной способности сети между нодами Cassandra.

## Структура

- `install_calico.sh` — установка Calico через Helm
- `delete_calico.sh` — удаление Calico
- `../cassandra_ddosing/manage_cassandra_limits.py` — Python-скрипт для управления дисковыми и сетевыми ограничениями

## Установка Calico

```bash
cd calico
./install_calico.sh
```

Скрипт:
1. Проверит, что minikube запущен
2. Добавит Helm репозиторий Calico
3. Установит Calico Operator через Helm (версия v3.27.0)
4. Дождётся готовности всех подов

## Использование

### Установка ограничения сети

Ограничить трафик между нодами Cassandra (порт 7000) до 10 Мбит/с:

```bash
# Ограничить трафик до 10 Мбит/с (Calico должен быть установлен)
uv run cassandra_ddosing/manage_cassandra_limits.py limit-net-speed 10

# Dry-run (показать манифест без применения)
uv run cassandra_ddosing/manage_cassandra_limits.py limit-net-speed 10 --dry-run
```

### Снятие ограничения

```bash
uv run cassandra_ddosing/manage_cassandra_limits.py unlimit-net-speed
```

### Просмотр текущей политики

```bash
kubectl get globalnetworkpolicy cassandra-internode-bandwidth-limit -o yaml
```

## Удаление Calico

```bash
cd calico
./delete_calico.sh
```

Скрипт:
1. Удалит все Calico NetworkPolicy
2. Удалит Helm release
3. Удалит namespace `tigera-operator`
4. Очистит CRD

## Как это работает

### Calico NetworkPolicy

Calico создаёт `GlobalNetworkPolicy`, которая:
- Применяется ко всем подам с label `app.kubernetes.io/name=cassandra`
- Ограничивает **только egress** трафик на порт 7000 (inter-node communication)
- Не затрагивает CQL-трафик (порт 9042) и другие порты

Пример политики:

```yaml
apiVersion: projectcalico.org/v3
kind: GlobalNetworkPolicy
metadata:
  name: cassandra-internode-bandwidth-limit
spec:
  selector: app.kubernetes.io/name == 'cassandra'
  egress:
    - action: Allow
      protocol: TCP
      destination:
        ports:
          - 7000
      metadata:
        annotations:
          projectcalico.org/bandwidth: "10M"
    - action: Allow
  ingress:
    - action: Allow
  order: 100
  types:
    - Egress
    - Ingress
```

### Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│ Minikube Cluster                                            │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Namespace: tigera-operator                           │  │
│  │                                                      │  │
│  │  ┌────────────────┐                                 │  │
│  │  │ Calico Operator│  (Helm release: calico)         │  │
│  │  └────────────────┘                                 │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Namespace: kube-system                               │  │
│  │                                                      │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐    │  │
│  │  │calico-node │  │calico-node │  │calico-node │    │  │
│  │  │  (DaemonSet)  │  (DaemonSet)  │  (DaemonSet)    │  │
│  │  └────────────┘  └────────────┘  └────────────┘    │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Namespace: cassandra                                 │  │
│  │                                                      │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐    │  │
│  │  │cassandra-0 │──│cassandra-1 │──│cassandra-2 │    │  │
│  │  │            │  │            │  │            │    │  │
│  │  │ Port 7000: │  │ Port 7000: │  │ Port 7000: │    │  │
│  │  │ 10 Mbit/s  │  │ 10 Mbit/s  │  │ 10 Mbit/s  │    │  │
│  │  └────────────┘  └────────────┘  └────────────┘    │  │
│  │         ▲               ▲               ▲           │  │
│  │         └───────────────┴───────────────┘           │  │
│  │         Calico NetworkPolicy применяется            │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Проверка работы

После установки ограничения можно проверить, что оно работает:

1. Запустить нагрузочный тест:
   ```bash
   uv run cassandra_ddosing/run_overload_with_pyspark.py --batches 5 --batch-size 50000
   ```

2. Проверить статистику Calico:
   ```bash
   # Просмотр политики
   kubectl get globalnetworkpolicy cassandra-internode-bandwidth-limit -o yaml
   
   # Логи calico-node
   kubectl logs -n kube-system -l k8s-app=calico-node --tail=50
   ```

## Требования

- minikube
- kubectl
- helm
- Python 3.7+ с uv
- click (устанавливается через uv)

## Примечания

- Calico устанавливается в namespace `tigera-operator`
- Calico поды (calico-node) работают в namespace `kube-system`
- NetworkPolicy применяется глобально ко всем подам Cassandra
- Ограничение применяется только к трафику на порту 7000 (inter-node)
- CQL-трафик (порт 9042) не ограничивается

## Troubleshooting

### Calico не устанавливается

```bash
# Проверить статус Helm release
helm list -n tigera-operator

# Проверить логи operator
kubectl logs -n tigera-operator -l k8s-app=tigera-operator

# Удалить и переустановить
./delete_calico.sh
./install_calico.sh
```

### NetworkPolicy не работает

```bash
# Проверить, что политика создана
kubectl get globalnetworkpolicy

# Проверить детали политики
kubectl get globalnetworkpolicy cassandra-internode-bandwidth-limit -o yaml

# Проверить логи calico-node
kubectl logs -n kube-system -l k8s-app=calico-node --tail=100 | grep -i bandwidth
```

### Поды Cassandra не запускаются после установки Calico

Calico может конфликтовать со стандартным CNI minikube. Попробуйте:

```bash
# Удалить Calico
./delete_calico.sh

# Перезапустить minikube
minikube stop
minikube start

# Переустановить Cassandra
cd ../cassandra
./delete_cassandra.sh
./setup_cassandra.sh

# Установить Calico
cd ../calico
./install_calico.sh
```
