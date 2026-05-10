#!/bin/bash

# Скрипт для установки Calico CNI в minikube кластер через Helm

set -e  # Прервать выполнение при ошибке

CALICO_CHART_VERSION="v3.27.0"
CALICO_NAMESPACE="tigera-operator"
CALICO_RELEASE="calico"

echo "=== Установка Calico CNI через Helm ==="
echo ""

# Проверка, что minikube запущен
echo "Проверка статуса minikube..."
if ! minikube status > /dev/null 2>&1; then
    echo "✗ Minikube не запущен. Запустите minikube start"
    exit 1
fi
echo "✓ Minikube запущен"

# Проверка, не установлен ли уже Calico
echo ""
echo "Проверка наличия Calico..."
if helm list -n "$CALICO_NAMESPACE" 2>/dev/null | grep -q "$CALICO_RELEASE"; then
    echo "✓ Calico уже установлен (Helm release: $CALICO_RELEASE)"
    echo ""
    echo "Для переустановки сначала выполните: ./delete_calico.sh"
    exit 0
fi

# Проверка через kubectl (на случай установки не через Helm)
if kubectl get pods -n kube-system -l k8s-app=calico-node > /dev/null 2>&1; then
    CALICO_PODS=$(kubectl get pods -n kube-system -l k8s-app=calico-node -o name | wc -l)
    if [ "$CALICO_PODS" -gt 0 ]; then
        echo "✓ Calico уже установлен ($CALICO_PODS подов, но не через Helm)"
        echo ""
        echo "Для переустановки сначала выполните: ./delete_calico.sh"
        exit 0
    fi
fi

# Создание namespace для Calico operator
echo ""
echo "=== Создание namespace $CALICO_NAMESPACE ==="
kubectl create namespace "$CALICO_NAMESPACE" 2>/dev/null || echo "  Namespace уже существует"

# Добавление Helm репозитория Calico
echo ""
echo "=== Добавление Helm репозитория Calico ==="
helm repo add projectcalico https://docs.tigera.io/calico/charts
helm repo update

echo "✓ Helm репозиторий добавлен"

# Установка Calico через Helm
echo ""
echo "=== Установка Calico $CALICO_CHART_VERSION через Helm ==="
helm install "$CALICO_RELEASE" projectcalico/tigera-operator \
    --version "$CALICO_CHART_VERSION" \
    --namespace "$CALICO_NAMESPACE" \
    --create-namespace \
    --wait \
    --timeout 10m

echo ""
echo "✓ Calico установлен через Helm"

# Ожидание готовности Calico подов
echo ""
echo "=== Ожидание готовности Calico подов ==="

echo "Ожидание запуска подов в namespace calico-system..."
for i in {1..60}; do
    # Проверяем, что все поды calico-node в статусе Running
    RUNNING_COUNT=$(kubectl get pods -n calico-system -l k8s-app=calico-node \
        -o jsonpath='{.items[*].status.phase}' 2>/dev/null | \
        grep -o "Running" | wc -l || echo "0")
    
    TOTAL_COUNT=$(kubectl get pods -n calico-system -l k8s-app=calico-node \
        -o name 2>/dev/null | wc -l || echo "0")
    
    if [ "$TOTAL_COUNT" -gt 0 ] && [ "$RUNNING_COUNT" -eq "$TOTAL_COUNT" ]; then
        echo "✓ Все поды Calico запущены ($RUNNING_COUNT/$TOTAL_COUNT) (попытка $i)"
        break
    fi
    
    echo "Запущено $RUNNING_COUNT/$TOTAL_COUNT подов (попытка $i/60)"
    sleep 5
done

# Проверка готовности (Ready status)
echo ""
echo "Проверка готовности подов..."
for i in {1..60}; do
    READY_COUNT=$(kubectl get pods -n calico-system -l k8s-app=calico-node \
        -o jsonpath='{.items[*].status.conditions[?(@.type=="Ready")].status}' 2>/dev/null | \
        grep -o "True" | wc -l || echo "0")
    
    TOTAL_COUNT=$(kubectl get pods -n calico-system -l k8s-app=calico-node \
        -o name 2>/dev/null | wc -l || echo "0")
    
    if [ "$TOTAL_COUNT" -gt 0 ] && [ "$READY_COUNT" -eq "$TOTAL_COUNT" ]; then
        echo "✓ Все поды Calico готовы ($READY_COUNT/$TOTAL_COUNT) (попытка $i)"
        break
    fi
    
    echo "Готово $READY_COUNT/$TOTAL_COUNT подов (попытка $i/60)"
    sleep 5
done

# Вывод информации о Calico
echo ""
echo "=== Информация о Calico ==="
echo ""
echo "Поды Calico (namespace: calico-system):"
kubectl get pods -n calico-system -o wide

echo ""
echo "Поды Calico Operator (namespace: tigera-operator):"
kubectl get pods -n tigera-operator -o wide

echo ""
echo "Calico API Resources:"
kubectl api-resources --api-group=crd.projectcalico.org 2>/dev/null || echo "  (API resources ещё не готовы)"

echo ""
echo "=== Установка завершена успешно! ==="
echo ""
echo "Теперь можно использовать Calico NetworkPolicy для управления сетевыми ограничениями."
echo ""
echo "Примеры:"
echo "  # Установить ограничение 10 Мбит/с на порт 7000"
echo "  uv run cassandra_ddosing/manage_cassandra_network_calico.py limit-net-speed 10"
echo ""
echo "  # Показать текущую политику"
echo "  uv run cassandra_ddosing/manage_cassandra_network_calico.py show-policy"
echo ""
