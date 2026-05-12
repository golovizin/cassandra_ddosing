#!/bin/bash

# Скрипт для удаления Calico CNI из minikube кластера (установленного через Helm)

set -e  # Прервать выполнение при ошибке

CALICO_NAMESPACE="tigera-operator"
CALICO_RELEASE="calico"

echo "=== Удаление Calico CNI ==="
echo ""

# Проверка, что minikube запущен
echo "Проверка статуса minikube..."
if ! minikube status > /dev/null 2>&1; then
    echo "✗ Minikube не запущен"
    exit 1
fi
echo "✓ Minikube запущен"

# Удаление Calico NetworkPolicy (если есть)
echo ""
echo "=== Удаление Calico NetworkPolicy ==="
if kubectl get globalnetworkpolicy cassandra-internode-bandwidth-limit > /dev/null 2>&1; then
    echo "Удаление cassandra-internode-bandwidth-limit..."
    kubectl delete globalnetworkpolicy cassandra-internode-bandwidth-limit || true
    echo "✓ NetworkPolicy удалена"
else
    echo "✓ NetworkPolicy не найдена"
fi

# Проверка, установлен ли Calico через Helm
echo ""
echo "Проверка наличия Calico Helm release..."
if helm list -A 2>/dev/null | grep -q "$CALICO_RELEASE"; then
    echo "✓ Найден Helm release: $CALICO_RELEASE"
    
    # Принудительное удаление зависших uninstall jobs
    echo ""
    echo "=== Очистка зависших uninstall jobs ==="
    UNINSTALL_JOBS=$(kubectl get jobs -n "$CALICO_NAMESPACE" -o name 2>/dev/null | grep uninstall || echo "")
    if [ -n "$UNINSTALL_JOBS" ]; then
        echo "Удаление зависших jobs..."
        echo "$UNINSTALL_JOBS" | xargs kubectl delete -n "$CALICO_NAMESPACE" --ignore-not-found=true --force --grace-period=0 2>/dev/null || true
        echo "✓ Jobs удалены"
    else
        echo "✓ Зависших jobs не найдено"
    fi
    
    # Принудительное удаление Calico Installation CRD
    echo ""
    echo "=== Удаление Calico Installation ==="
    if kubectl get installation default > /dev/null 2>&1; then
        echo "Удаление installation default..."
        kubectl delete installation default --ignore-not-found=true --timeout=10s 2>/dev/null || true
        # Если не удалилось, удаляем с finalizers
        kubectl patch installation default -p '{"metadata":{"finalizers":[]}}' --type=merge 2>/dev/null || true
        kubectl delete installation default --ignore-not-found=true --force --grace-period=0 2>/dev/null || true
        echo "✓ Installation удалён"
    else
        echo "✓ Installation не найден"
    fi
    
    # Удаление через Helm (без ожидания, если зависло)
    echo ""
    echo "=== Удаление Calico через Helm ==="
    helm uninstall "$CALICO_RELEASE" -n "$CALICO_NAMESPACE" --wait=false 2>/dev/null || true
    
    echo "✓ Helm release удалён"
else
    echo "✓ Helm release не найден"
    
    # Проверяем наличие подов (возможно, установлено не через Helm)
    CALICO_PODS=$(kubectl get pods -n kube-system -l k8s-app=calico-node -o name 2>/dev/null | wc -l || echo "0")
    
    if [ "$CALICO_PODS" -eq 0 ]; then
        echo "✓ Поды Calico не найдены"
    else
        echo "⚠ Найдены поды Calico ($CALICO_PODS), но не через Helm"
        echo "  Попробуйте удалить вручную через kubectl"
    fi
fi

# Удаление всех Calico namespaces
echo ""
echo "=== Удаление Calico namespaces ==="
for NS in "calico-system" "calico-apiserver" "tigera-operator"; do
    if kubectl get namespace "$NS" > /dev/null 2>&1; then
        echo "Удаление namespace $NS..."
        
        # Принудительно удаляем все ресурсы внутри (без ожидания)
        kubectl delete all --all -n "$NS" --force --grace-period=0 --wait=false 2>/dev/null || true
        
        # Удаляем finalizers у namespace
        kubectl patch namespace "$NS" -p '{"metadata":{"finalizers":[]}}' --type=merge 2>/dev/null || true
        
        # Запускаем удаление namespace в фоне (без ожидания)
        kubectl delete namespace "$NS" --wait=false 2>/dev/null &
        
        echo "✓ Запущено удаление namespace $NS"
    else
        echo "✓ Namespace $NS не найден"
    fi
done

# Даём время на начало удаления
echo "Ожидание начала удаления..."
sleep 5

# Ожидание завершения удаления подов
echo ""
echo "=== Ожидание завершения удаления подов ==="

for i in {1..30}; do
    REMAINING=$(kubectl get pods -n kube-system -l k8s-app=calico-node -o name 2>/dev/null | wc -l || echo "0")
    
    if [ "$REMAINING" -eq 0 ]; then
        echo "✓ Все поды Calico удалены (попытка $i)"
        break
    fi
    
    echo "Осталось подов: $REMAINING (попытка $i/30)"
    sleep 2
done

# Удаление CRD (Custom Resource Definitions)
echo ""
echo "=== Удаление Calico CRD ==="
CALICO_CRDS=$(kubectl get crd | grep "projectcalico.org" | awk '{print $1}' || echo "")

if [ -n "$CALICO_CRDS" ]; then
    echo "Найдены CRD:"
    echo "$CALICO_CRDS"
    echo ""
    echo "Удаление CRD..."
    echo "$CALICO_CRDS" | xargs kubectl delete crd --ignore-not-found=true
    echo "✓ CRD удалены"
else
    echo "✓ CRD не найдены"
fi

# Проверка, что всё удалено
echo ""
echo "=== Проверка удаления ==="
REMAINING_PODS=$(kubectl get pods -n kube-system -l k8s-app=calico-node -o name 2>/dev/null | wc -l || echo "0")
REMAINING_CRDS=$(kubectl get crd | grep "projectcalico.org" | wc -l || echo "0")

if [ "$REMAINING_PODS" -eq 0 ] && [ "$REMAINING_CRDS" -eq 0 ]; then
    echo "✓ Calico полностью удалён"
else
    echo "⚠ Некоторые ресурсы остались:"
    if [ "$REMAINING_PODS" -gt 0 ]; then
        echo "  - Подов: $REMAINING_PODS"
    fi
    if [ "$REMAINING_CRDS" -gt 0 ]; then
        echo "  - CRD: $REMAINING_CRDS"
    fi
fi

echo ""
echo "=== Удаление завершено ==="
echo ""
echo "Для восстановления стандартного CNI может потребоваться перезапуск minikube:"
echo "  minikube stop"
echo "  minikube start"
echo ""
