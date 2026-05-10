#!/usr/bin/env python3
"""
Скрипт для управления ограничениями диска и сети для Cassandra-подов в minikube.

Использует:
- cgroups v2 blkio.throttle для ограничения write throughput на диске
- Calico NetworkPolicy для ограничения network throughput между нодами (порт 7000)
"""

import json
import subprocess
import sys
import textwrap
import time
from typing import List, Tuple

import click


def run_command(cmd: List[str], capture_output: bool = True) -> Tuple[int, str, str]:
    """
    Выполняет команду и возвращает код возврата, stdout и stderr.

    Args:
        cmd: Список аргументов команды
        capture_output: Захватывать ли вывод

    Returns:
        Кортеж (return_code, stdout, stderr)
    """
    try:
        result = subprocess.run(cmd, capture_output=capture_output, text=True, check=False)
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        return 1, "", str(e)


def get_cassandra_pods(namespace: str = "cassandra") -> List[str]:
    """
    Получает список имён Cassandra-подов в указанном namespace.

    Args:
        namespace: Kubernetes namespace

    Returns:
        Список имён подов
    """
    cmd = [
        "kubectl",
        "get",
        "pods",
        "-n",
        namespace,
        "-l",
        "app.kubernetes.io/name=cassandra",
        "-o",
        "jsonpath={.items[*].metadata.name}",
    ]

    returncode, stdout, stderr = run_command(cmd)

    if returncode != 0:
        print(f"✗ Ошибка при получении списка подов: {stderr}", file=sys.stderr)
        return []

    pods = stdout.strip().split()
    return pods


# ── Disk throttling ───────────────────────────────────────────────────────────


def set_disk_throttle(pod_name: str, namespace: str, limit_bytes_per_sec: int, dry_run: bool = False) -> bool:
    """
    Устанавливает ограничение скорости записи на диск для пода через minikube ssh.

    Args:
        pod_name: Имя пода
        namespace: Kubernetes namespace
        limit_bytes_per_sec: Лимит в байтах в секунду
        dry_run: Только показать команды, не выполнять

    Returns:
        True если успешно, False иначе
    """
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Настройка disk throttling для пода {pod_name}...")

    bash_cmd = f"""set -euo pipefail
# Определяем базовое блочное устройство через /var/lib/kubelet
DEV_PART=$(df /var/lib/kubelet 2>/dev/null | awk 'NR==2{{print $1}}')
if [ -z "$DEV_PART" ]; then
    echo "ERROR: Cannot determine device partition" >&2
    exit 1
fi

# Убираем номер раздела: /dev/sda1 -> /dev/sda, /dev/nvme0n1p2 -> /dev/nvme0n1
if echo "$DEV_PART" | grep -q 'nvme'; then
    DEV=$(echo "$DEV_PART" | sed 's/p[0-9]*$//')
else
    DEV=$(echo "$DEV_PART" | sed 's/[0-9]*$//')
fi

# Получаем major:minor через ls -l
MAJOR_MINOR=$(ls -l "$DEV" | awk '{{print $5":"$6}}' | sed 's/,//')
if [ -z "$MAJOR_MINOR" ]; then
    echo "ERROR: Cannot determine major:minor for $DEV" >&2
    exit 1
fi

echo "Device: $DEV ($MAJOR_MINOR)"

# Определяем cgroup-prefix PID 1
ROOT_CG=$(awk -F: '/blkio/{{print $3}}' /proc/1/cgroup 2>/dev/null | head -1)
ROOT_CG="${{ROOT_CG%/}}"

# Применяем throttling к процессам Cassandra
pgrep -f 'CassandraDaemon' | while read pid; do
    CG=$(awk -F: '/blkio/{{print $3}}' /proc/$pid/cgroup 2>/dev/null | head -1)
    [ -z "$CG" ] && continue
    echo "$CG" | grep -q 'kubepods' || continue
    if [ -n "$ROOT_CG" ] && [ "$ROOT_CG" != "/" ]; then
        CG="${{CG#$ROOT_CG}}"
    fi
    [ -z "$CG" ] && CG="/"
    CGROUP_PATH="/sys/fs/cgroup/blkio${{CG%/}}"
    if [ ! -d "$CGROUP_PATH" ]; then
        echo "✗ PID $pid: cgroup dir not found: $CGROUP_PATH"
        continue
    fi
    echo "$MAJOR_MINOR {limit_bytes_per_sec}" | tee "$CGROUP_PATH/blkio.throttle.write_bps_device" > /dev/null && \\
    echo "✓ PID $pid: disk throttle set to {limit_bytes_per_sec} bytes/s"
done || true
"""

    if dry_run:
        print(f"  [DRY RUN] Bash script:")
        print(textwrap.indent(bash_cmd, "    "))
        return True

    heredoc_cmd = f"sudo bash << 'EOFSCRIPT'\n{bash_cmd}\nEOFSCRIPT"
    returncode, stdout, stderr = run_command(["minikube", "ssh", "--", heredoc_cmd])

    if returncode == 0 and stdout.strip():
        print(f"✓ Disk throttling установлен")
        for line in stdout.strip().split("\n"):
            print(f"  {line}")
        return True

    print(f"✗ Не удалось установить disk throttling")
    print(f"  Return code: {returncode}")
    if stdout:
        print(f"  Stdout: {stdout}")
    if stderr:
        print(f"  Stderr: {stderr}")
    return False


def unset_disk_throttle(pod_name: str, namespace: str, dry_run: bool = False) -> bool:
    """
    Снимает ограничение скорости записи на диск для пода.

    Args:
        pod_name: Имя пода
        namespace: Kubernetes namespace
        dry_run: Только показать команды, не выполнять

    Returns:
        True если успешно, False иначе
    """
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Снятие disk throttling для пода {pod_name}...")

    bash_cmd = """set -euo pipefail
# Определяем базовое блочное устройство
DEV_PART=$(df /var/lib/kubelet 2>/dev/null | awk 'NR==2{print $1}')
if [ -z "$DEV_PART" ]; then
    echo "ERROR: Cannot determine device partition" >&2
    exit 1
fi

if echo "$DEV_PART" | grep -q 'nvme'; then
    DEV=$(echo "$DEV_PART" | sed 's/p[0-9]*$//')
else
    DEV=$(echo "$DEV_PART" | sed 's/[0-9]*$//')
fi

MAJOR_MINOR=$(ls -l "$DEV" | awk '{print $5":"$6}' | sed 's/,//')
if [ -z "$MAJOR_MINOR" ]; then
    echo "ERROR: Cannot determine major:minor for $DEV" >&2
    exit 1
fi

echo "Device: $DEV ($MAJOR_MINOR)"

ROOT_CG=$(awk -F: '/blkio/{print $3}' /proc/1/cgroup 2>/dev/null | head -1)
ROOT_CG="${ROOT_CG%/}"

pgrep -f 'CassandraDaemon' | while read pid; do
    CG=$(awk -F: '/blkio/{print $3}' /proc/$pid/cgroup 2>/dev/null | head -1)
    [ -z "$CG" ] && continue
    echo "$CG" | grep -q 'kubepods' || continue
    if [ -n "$ROOT_CG" ] && [ "$ROOT_CG" != "/" ]; then
        CG="${CG#$ROOT_CG}"
    fi
    [ -z "$CG" ] && CG="/"
    CGROUP_PATH="/sys/fs/cgroup/blkio${CG%/}"
    if [ ! -d "$CGROUP_PATH" ]; then
        echo "✗ PID $pid: cgroup dir not found: $CGROUP_PATH"
        continue
    fi
    echo "$MAJOR_MINOR 0" | tee "$CGROUP_PATH/blkio.throttle.write_bps_device" > /dev/null && \\
    echo "✓ PID $pid: disk throttling removed"
done || true
"""

    if dry_run:
        print(f"  [DRY RUN] Bash script:")
        print(textwrap.indent(bash_cmd, "    "))
        return True

    heredoc_cmd = f"sudo bash << 'EOFSCRIPT'\n{bash_cmd}\nEOFSCRIPT"
    returncode, stdout, stderr = run_command(["minikube", "ssh", "--", heredoc_cmd])

    if returncode == 0 and stdout.strip():
        print(f"✓ Disk throttling снят")
        for line in stdout.strip().split("\n"):
            print(f"  {line}")
        return True

    print(f"✗ Не удалось снять disk throttling")
    print(f"  Return code: {returncode}")
    if stdout:
        print(f"  Stdout: {stdout}")
    if stderr:
        print(f"  Stderr: {stderr}")
    return False


# ── Network throttling (Calico) ──────────────────────────────────────────────


def check_calico_installed() -> bool:
    """
    Проверяет, установлен ли Calico в кластере.

    Returns:
        True если Calico установлен, False иначе
    """
    # Calico Operator создаёт поды в namespace calico-system
    cmd = ["kubectl", "get", "pods", "-n", "calico-system", "-l", "k8s-app=calico-node", "-o", "name"]
    returncode, stdout, stderr = run_command(cmd)

    if returncode == 0 and stdout.strip():
        return True

    # Проверяем через API resources
    cmd = ["kubectl", "api-resources", "--api-group=crd.projectcalico.org"]
    returncode, stdout, stderr = run_command(cmd)

    return returncode == 0 and "networkpolicies" in stdout.lower()


def create_calico_bandwidth_policy(namespace: str, mbps: float, dry_run: bool = False) -> bool:
    """
    Создаёт Calico NetworkPolicy с ограничением bandwidth для порта 7000.

    Args:
        namespace: Kubernetes namespace
        mbps: Лимит в Мбит/с (поддерживает дробные значения)
        dry_run: Только показать манифест, не применять

    Returns:
        True если успешно, False иначе
    """
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Создание Calico NetworkPolicy...")

    # Для Calico bandwidth можно указывать в формате:
    # - "10M" для целых чисел
    # - "500K" для значений < 1 Мбит/с
    # Конвертируем в Кбит/с если < 1 Мбит/с для большей точности
    if mbps < 1.0:
        bandwidth_value = f"{int(mbps * 1000)}K"
    else:
        # Для значений >= 1 используем формат с дробной частью
        bandwidth_value = f"{mbps:.1f}M"

    # Calico GlobalNetworkPolicy с bandwidth limiting
    # Ограничиваем только egress трафик на порт 7000 (inter-node communication)
    policy_yaml = f"""apiVersion: projectcalico.org/v3
kind: GlobalNetworkPolicy
metadata:
  name: cassandra-internode-bandwidth-limit
spec:
  # Применяется к подам Cassandra
  selector: app.kubernetes.io/name == 'cassandra'
  
  # Ограничиваем исходящий трафик
  egress:
    # Разрешаем весь трафик, но с ограничением bandwidth для порта 7000
    - action: Allow
      protocol: TCP
      destination:
        ports:
          - 7000
      # Ограничение bandwidth
      metadata:
        annotations:
          projectcalico.org/bandwidth: "{bandwidth_value}"
    
    # Остальной трафик без ограничений
    - action: Allow
  
  # Входящий трафик без ограничений
  ingress:
    - action: Allow
  
  order: 100
  types:
    - Egress
    - Ingress
"""

    if dry_run:
        print(f"  [DRY RUN] Манифест Calico NetworkPolicy:")
        print("  " + "\n  ".join(policy_yaml.split("\n")))
        return True

    # Применяем политику через kubectl
    cmd = ["kubectl", "apply", "-f", "-"]
    try:
        result = subprocess.run(cmd, input=policy_yaml, capture_output=True, text=True, check=False)

        if result.returncode == 0:
            print(f"✓ Calico NetworkPolicy создана")
            print(f"  Ограничение: {mbps} Мбит/с ({bandwidth_value}) на порт 7000 (egress)")
            return True
        else:
            print(f"✗ Не удалось создать NetworkPolicy")
            print(f"  Return code: {result.returncode}")
            if result.stderr:
                print(f"  Stderr: {result.stderr}")
            return False
    except Exception as e:
        print(f"✗ Ошибка при создании NetworkPolicy: {e}")
        return False


def delete_calico_bandwidth_policy(dry_run: bool = False) -> bool:
    """
    Удаляет Calico NetworkPolicy с ограничением bandwidth.

    Args:
        dry_run: Только показать команду, не выполнять

    Returns:
        True если успешно, False иначе
    """
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Удаление Calico NetworkPolicy...")

    if dry_run:
        print(f"  [DRY RUN] Команда kubectl delete:")
        print(f"    kubectl delete globalnetworkpolicy cassandra-internode-bandwidth-limit")
        return True

    cmd = ["kubectl", "delete", "globalnetworkpolicy", "cassandra-internode-bandwidth-limit"]
    returncode, stdout, stderr = run_command(cmd)

    if returncode == 0:
        print(f"✓ Calico NetworkPolicy удалена")
        return True
    elif "not found" in stderr.lower():
        print(f"✓ NetworkPolicy не была установлена")
        return True
    else:
        print(f"✗ Не удалось удалить NetworkPolicy")
        print(f"  Return code: {returncode}")
        if stderr:
            print(f"  Stderr: {stderr}")
        return False


# ── CLI ───────────────────────────────────────────────────────────────────────


@click.group()
def cli():
    """
    Управление ограничениями диска и сети для Cassandra-подов в minikube.
    """
    pass


@cli.command(name="limit-disk-speed")
@click.argument("mbytes_per_sec", type=float)
@click.option("--namespace", default="cassandra", help="Kubernetes namespace (по умолчанию: cassandra)")
@click.option("--pod", help="Имя конкретного пода (если не указано, применяется ко всем)")
@click.option("--dry-run", is_flag=True, help="Показать команды без выполнения")
def limit_disk_speed(mbytes_per_sec: float, namespace: str, pod: str, dry_run: bool):
    """
    Устанавливает ограничение скорости записи на диск.

    MBYTES_PER_SEC - лимит в МБ/сек

    \b
    Примеры:
      python manage_cassandra_limits.py limit-disk-speed 1
      python manage_cassandra_limits.py limit-disk-speed 10 --pod cassandra-0
    """
    limit_bytes_per_sec = int(mbytes_per_sec * 1024 * 1024)

    click.echo(f"{'=' * 60}")
    click.echo(f"Установка ограничения скорости записи на диск")
    click.echo(f"{'=' * 60}")
    click.echo(f"Namespace: {namespace}")
    click.echo(f"Лимит: {mbytes_per_sec} МБ/сек ({limit_bytes_per_sec} байт/сек)")
    if dry_run:
        click.echo(f"Режим: DRY RUN")
    click.echo(f"{'=' * 60}")

    pods = [pod] if pod else get_cassandra_pods(namespace)
    if not pods:
        click.echo(f"✗ Cassandra-поды не найдены", err=True)
        sys.exit(1)

    click.echo(f"\nПоды: {', '.join(pods)}")

    success_count = sum(1 for p in pods if set_disk_throttle(p, namespace, limit_bytes_per_sec, dry_run))

    click.echo(f"\n{'=' * 60}")
    click.echo(f"Успешно: {success_count}/{len(pods)}")
    click.echo(f"{'=' * 60}")


@cli.command(name="unlimit-disk-speed")
@click.option("--namespace", default="cassandra", help="Kubernetes namespace")
@click.option("--pod", help="Имя конкретного пода")
@click.option("--dry-run", is_flag=True, help="Показать команды без выполнения")
def unlimit_disk_speed(namespace: str, pod: str, dry_run: bool):
    """
    Снимает ограничение скорости записи на диск.

    \b
    Примеры:
      python manage_cassandra_limits.py unlimit-disk-speed
      python manage_cassandra_limits.py unlimit-disk-speed --pod cassandra-0
    """
    click.echo(f"{'=' * 60}")
    click.echo(f"Снятие ограничения скорости записи на диск")
    click.echo(f"{'=' * 60}")

    pods = [pod] if pod else get_cassandra_pods(namespace)
    if not pods:
        click.echo(f"✗ Cassandra-поды не найдены", err=True)
        sys.exit(1)

    click.echo(f"Поды: {', '.join(pods)}")

    success_count = sum(1 for p in pods if unset_disk_throttle(p, namespace, dry_run))

    click.echo(f"\n{'=' * 60}")
    click.echo(f"Успешно: {success_count}/{len(pods)}")
    click.echo(f"{'=' * 60}")


@cli.command(name="limit-net-speed")
@click.argument("mbytes_per_sec", type=float)
@click.option("--namespace", default="cassandra", help="Kubernetes namespace")
@click.option("--dry-run", is_flag=True, help="Показать манифест без применения")
def limit_net_speed(mbytes_per_sec: float, namespace: str, dry_run: bool):
    """
    Устанавливает ограничение пропускной способности сети для inter-node трафика (порт 7000) через Calico.

    MBYTES_PER_SEC - лимит в МБайт/сек

    \b
    Примеры:
      # Ограничить до 0.5 МБайт/с (4 Мбит/с)
      uv run cassandra_ddosing/manage_cassandra_limits.py limit-net-speed 0.5

      # Ограничить до 10 МБайт/с (80 Мбит/с)
      uv run cassandra_ddosing/manage_cassandra_limits.py limit-net-speed 10

      # Показать манифест без применения
      uv run cassandra_ddosing/manage_cassandra_limits.py limit-net-speed 0.5 --dry-run

    \b
    ВАЖНО: Требуется установленный Calico CNI!
    Установка: cd calico && ./install_calico.sh
    """
    # Конвертируем МБайт/с в Мбит/с для Calico
    # 1 МБайт/с = 8 Мбит/с
    mbps = mbytes_per_sec * 8

    click.echo(f"{'=' * 60}")
    click.echo(f"Установка ограничения пропускной способности сети (Calico)")
    click.echo(f"{'=' * 60}")
    click.echo(f"Namespace: {namespace}")
    click.echo(f"Лимит: {mbytes_per_sec} МБайт/с ({mbps} Мбит/с, только порт 7000 - inter-node)")
    if dry_run:
        click.echo(f"Режим: DRY RUN")
    click.echo(f"{'=' * 60}")

    # Проверяем Calico
    if not dry_run and not check_calico_installed():
        click.echo("\n✗ Calico не установлен!", err=True)
        click.echo("  Установите Calico: cd calico && ./install_calico.sh")
        sys.exit(1)

    # Создаём политику
    if create_calico_bandwidth_policy(namespace, mbps, dry_run):
        click.echo(f"\n{'=' * 60}")
        click.echo(f"✓ Успешно")
        click.echo(f"{'=' * 60}")
    else:
        click.echo(f"\n{'=' * 60}")
        click.echo(f"✗ Ошибка", err=True)
        click.echo(f"{'=' * 60}")
        sys.exit(1)


@cli.command(name="unlimit-net-speed")
@click.option("--dry-run", is_flag=True, help="Показать команду без выполнения")
def unlimit_net_speed(dry_run: bool):
    """
    Снимает ограничение пропускной способности сети (удаляет Calico NetworkPolicy).

    \b
    Примеры:
      uv run cassandra_ddosing/manage_cassandra_limits.py unlimit-net-speed
    """
    click.echo(f"{'=' * 60}")
    click.echo(f"Снятие ограничения пропускной способности сети (Calico)")
    click.echo(f"{'=' * 60}")
    if dry_run:
        click.echo(f"Режим: DRY RUN")
    click.echo(f"{'=' * 60}")

    if delete_calico_bandwidth_policy(dry_run):
        click.echo(f"\n{'=' * 60}")
        click.echo(f"✓ Успешно")
        click.echo(f"{'=' * 60}")
    else:
        click.echo(f"\n{'=' * 60}")
        click.echo(f"✗ Ошибка", err=True)
        click.echo(f"{'=' * 60}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
