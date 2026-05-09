#!/bin/bash

# Скрипт для установки Cassandra кластера в minikube
# 2 дата-центра (dc1 и dc2) по 3 ноды в каждом

set -e  # Прервать выполнение при ошибке

NAMESPACE="cassandra"
RELEASE="cassandra"
CHART_REPO="oci://ghcr.io/kubelauncher/charts/cassandra"

echo "=== Установка Cassandra кластера в minikube ==="

# Проверка, что minikube запущен
echo "Проверка статуса minikube..."
if ! minikube status > /dev/null 2>&1; then
    echo "✗ Minikube не запущен. Запустите minikube start"
    exit 1
fi
echo "✓ Minikube запущен"

# Создание namespace, если не существует
echo ""
echo "Создание namespace '$NAMESPACE'..."
set +e  # Временно отключаем прерывание при ошибке
kubectl get namespace "$NAMESPACE" > /dev/null 2>&1
namespace_exists=$?
set -e  # Включаем обратно

if [ $namespace_exists -eq 0 ]; then
    echo "✓ Namespace '$NAMESPACE' уже существует"
else
    kubectl create namespace "$NAMESPACE"
    echo "✓ Namespace '$NAMESPACE' создан"
fi

# Установка Cassandra (3 ноды)
echo ""
echo "=== Установка Cassandra (3 ноды) ==="
helm upgrade --install "$RELEASE" "$CHART_REPO" \
    --namespace "$NAMESPACE" \
    --values cassandra_values.yaml \
    --set replicaCount=3 \
    --wait \
    --timeout 15m

echo "✓ Cassandra установлен"

# Применение NodePort сервисов
echo ""
echo "=== Применение NodePort сервисов ==="
kubectl apply -f cassandra_nodeports.yaml
echo "✓ NodePort сервисы применены"

# Ожидание готовности всех подов
echo ""
echo "=== Ожидание готовности всех подов Cassandra ==="

echo "Ожидание готовности подов..."
for i in {1..60}; do
    ready_count=$(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$RELEASE" \
        -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null | \
        grep -o "True" | wc -l || echo "0")
    
    if [ "$ready_count" -eq "3" ]; then
        echo "✓ Все 3 пода готовы (попытка $i)"
        break
    fi
    
    echo "Готово $ready_count/3 подов (попытка $i/60)"
    sleep 5
done

# Проверка доступности Cassandra через CQL
echo ""
echo "=== Проверка доступности Cassandra ==="

echo "Проверка CQL-подключения к ${RELEASE}-0..."
for i in {1..30}; do
    if kubectl exec -n "$NAMESPACE" "${RELEASE}-0" -- cqlsh -u cassandra -p cassandra -e "DESCRIBE KEYSPACES" > /dev/null 2>&1; then
        echo "✓ CQL-подключение успешно (попытка $i)"
        break
    fi
    
    echo "Ожидание CQL... (попытка $i/30)"
    sleep 2
done

# Вывод информации о кластере
echo ""
echo "=== Информация о кластере ==="
echo ""
echo "Поды Cassandra:"
kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/instance=$RELEASE" -o wide

echo ""
echo "NodePort сервисы:"
kubectl get svc -n "$NAMESPACE" | grep NodePort

echo ""
echo "Статус кластера:"
kubectl exec -n "$NAMESPACE" "${RELEASE}-0" -- nodetool status || true

echo ""
echo "=== Установка завершена успешно! ==="
echo ""
echo "Доступ к Cassandra:"
echo "  - Логин: cassandra"
echo "  - Пароль: cassandra"
echo ""
echo "NodePort порты для подключения извне minikube:"
MINIKUBE_IP=$(minikube ip)
echo "  Нода 0: $MINIKUBE_IP:30000"
echo "  Нода 1: $MINIKUBE_IP:30001"
echo "  Нода 2: $MINIKUBE_IP:30002"
echo ""
echo "Пример подключения через cqlsh:"
echo "  cqlsh $MINIKUBE_IP 30000 -u cassandra -p cassandra"
