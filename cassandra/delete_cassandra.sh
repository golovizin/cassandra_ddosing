#!/bin/bash

# Скрипт для удаления Cassandra кластера из minikube

set -e  # Прервать выполнение при ошибке

NAMESPACE="cassandra"
RELEASE="cassandra"

echo "=== Удаление Cassandra кластера из minikube ==="

# Проверка существования namespace
if ! kubectl get namespace "$NAMESPACE" > /dev/null 2>&1; then
    echo "✓ Namespace '$NAMESPACE' не существует, нечего удалять"
    exit 0
fi

# Удаление NodePort сервисов
echo ""
echo "=== Удаление NodePort сервисов ==="
if kubectl get -f cassandra_nodeports.yaml > /dev/null 2>&1; then
    kubectl delete -f cassandra_nodeports.yaml
    echo "✓ NodePort сервисы удалены"
else
    echo "✓ NodePort сервисы не найдены"
fi

# Удаление Cassandra
echo ""
echo "=== Удаление Cassandra ==="
if helm list -n "$NAMESPACE" | grep -q "$RELEASE"; then
    helm uninstall "$RELEASE" --namespace "$NAMESPACE"
    echo "✓ Cassandra удалён"
else
    echo "✓ Cassandra не найден"
fi

# Ожидание удаления подов
echo ""
echo "=== Ожидание удаления подов ==="
for i in {1..30}; do
    pod_count=$(kubectl get pods -n "$NAMESPACE" -l "app.kubernetes.io/name=cassandra" --no-headers 2>/dev/null | wc -l || echo "0")
    
    if [ "$pod_count" -eq "0" ]; then
        echo "✓ Все поды Cassandra удалены (попытка $i)"
        break
    fi
    
    echo "Ожидание удаления подов... Осталось: $pod_count (попытка $i/30)"
    sleep 2
done

# Удаление PVC (Persistent Volume Claims)
echo ""
echo "=== Удаление PVC ==="
pvc_count=$(kubectl get pvc -n "$NAMESPACE" --no-headers 2>/dev/null | wc -l || echo "0")
if [ "$pvc_count" -gt "0" ]; then
    kubectl delete pvc --all -n "$NAMESPACE"
    echo "✓ PVC удалены ($pvc_count шт.)"
else
    echo "✓ PVC не найдены"
fi

# Удаление namespace
echo ""
echo "=== Удаление namespace ==="
kubectl delete namespace "$NAMESPACE"
echo "✓ Namespace '$NAMESPACE' удалён"

# Ожидание полного удаления namespace
echo ""
echo "=== Ожидание полного удаления namespace ==="
for i in {1..30}; do
    if ! kubectl get namespace "$NAMESPACE" > /dev/null 2>&1; then
        echo "✓ Namespace полностью удалён (попытка $i)"
        break
    fi
    
    echo "Ожидание удаления namespace... (попытка $i/30)"
    sleep 2
done

echo ""
echo "=== Удаление завершено успешно! ==="
