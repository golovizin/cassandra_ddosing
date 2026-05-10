#!/usr/bin/env python3
"""
Скрипт для установки ограничения скорости записи на диск для Cassandra-подов в minikube.

Использует cgroups v2 blkio.throttle для ограничения write throughput на уровне контейнера.
"""

import re
import subprocess
import sys
import textwrap
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


def get_data_mount_point(pod_name: str, namespace: str) -> str:
    """
    Определяет точку монтирования для data-директории Cassandra внутри пода.

    Args:
        pod_name: Имя пода
        namespace: Kubernetes namespace

    Returns:
        Точка монтирования (например, /data/cassandra или /var/lib/cassandra)
    """
    for mount_point in ["/data/cassandra", "/var/lib/cassandra", "/"]:
        cmd = [
            "kubectl",
            "exec",
            "-n",
            namespace,
            pod_name,
            "--",
            "sh",
            "-c",
            f"test -d {mount_point} && echo {mount_point}",
        ]

        returncode, stdout, stderr = run_command(cmd)

        if returncode == 0 and stdout.strip():
            return stdout.strip()

    print(f"✗ Не удалось найти точку монтирования данных для {pod_name}", file=sys.stderr)
    return ""


def get_block_device(pod_name: str, namespace: str) -> str:
    """
    Определяет блочное устройство для data-директории Cassandra внутри пода.

    Args:
        pod_name: Имя пода
        namespace: Kubernetes namespace

    Returns:
        Путь к блочному устройству (например, /dev/sda или /dev/nvme0n1)
    """
    # Находим устройство, на котором смонтирована /var/lib/cassandra
    mount_point = get_data_mount_point(pod_name, namespace)
    if not mount_point:
        print(f"✗ Не удалось определить точку монтирования данных для {pod_name}", file=sys.stderr)
        return ""

    cmd = [
        "kubectl",
        "exec",
        "-n",
        namespace,
        pod_name,
        "--",
        "sh",
        "-c",
        f"(df {mount_point} 2>/dev/null || df /) | tail -1 | awk '{{print $1}}'",
    ]

    returncode, stdout, stderr = run_command(cmd)

    if returncode != 0:
        print(f"✗ Ошибка при определении блочного устройства для {pod_name}: {stderr}", file=sys.stderr)
        return ""

    device = stdout.strip()

    if not device:
        print(f"✗ Пустой вывод при определении устройства для {pod_name}", file=sys.stderr)
        print(
            f"  Попробуйте выполнить вручную: kubectl exec -n {namespace} {pod_name} -- df {mount_point}",
            file=sys.stderr,
        )
        return ""

    if not device.startswith("/dev/"):
        print(f"✗ Устройство '{device}' не начинается с /dev/ для {pod_name}", file=sys.stderr)
        print(
            f"  Возможно, используется overlay или tmpfs. Попробуйте найти реальное блочное устройство.",
            file=sys.stderr,
        )
        return ""

    return device


def get_device_major_minor(pod_name: str, namespace: str, mount_point: str) -> Tuple[str, str]:
    """
    Получает major:minor номера базового блочного устройства.

    Для разделов (например, nvme0n1p2) возвращает major:minor базового устройства (nvme0n1),
    так как cgroup blkio throttling работает только с базовыми устройствами.

    Args:
        pod_name: Имя пода
        namespace: Kubernetes namespace
        mount_point: Точка монтирования (например, /data/cassandra)

    Returns:
        Кортеж (major, minor) базового устройства
    """
    cmd = [
        "kubectl",
        "exec",
        "-n",
        namespace,
        pod_name,
        "--",
        "sh",
        "-c",
        f"grep '{mount_point}' /proc/self/mountinfo | head -1 | awk '{{print $3, $NF}}'",
    ]

    returncode, stdout, stderr = run_command(cmd)

    if returncode != 0:
        print(
            f"✗ Ошибка при получении информации об устройстве для {mount_point} в {pod_name}: {stderr}", file=sys.stderr
        )
        return "", ""

    output = stdout.strip().split()
    if len(output) < 2:
        print(f"✗ Некорректный формат вывода mountinfo для {mount_point} в {pod_name}", file=sys.stderr)
        return "", ""

    major_minor = output[0]
    device_path = output[1]

    if ":" not in major_minor:
        print(f"✗ Некорректный формат major:minor: '{major_minor}' для {mount_point} в {pod_name}", file=sys.stderr)
        return "", ""

    cmd_base = ["minikube", "ssh", "--", f"stat -c '%t:%T' {device_path} 2>/dev/null || echo ''"]

    returncode, stdout, stderr = run_command(cmd_base)

    if returncode == 0 and stdout.strip() and ":" in stdout.strip():
        hex_major, hex_minor = stdout.strip().split(":")
        major = str(int(hex_major, 16))
        minor = str(int(hex_minor, 16))
        return major, minor

    major, minor = major_minor.split(":", 1)
    return major, minor


def get_pod_uid(pod_name: str, namespace: str) -> str:
    """
    Получает UID пода.

    Args:
        pod_name: Имя пода
        namespace: Kubernetes namespace

    Returns:
        Pod UID
    """
    cmd = ["kubectl", "get", "pod", pod_name, "-n", namespace, "-o", "jsonpath={.metadata.uid}"]

    returncode, stdout, stderr = run_command(cmd)

    if returncode != 0:
        print(f"✗ Ошибка при получении UID для {pod_name}: {stderr}", file=sys.stderr)
        return ""

    return stdout.strip()


def get_container_id(pod_name: str, namespace: str) -> str:
    """
    Получает container ID для основного контейнера Cassandra в поде.

    Args:
        pod_name: Имя пода
        namespace: Kubernetes namespace

    Returns:
        Container ID (полный, без префикса)
    """
    cmd = [
        "kubectl",
        "get",
        "pod",
        pod_name,
        "-n",
        namespace,
        "-o",
        "jsonpath={.status.containerStatuses[?(@.name=='cassandra')].containerID}",
    ]

    returncode, stdout, stderr = run_command(cmd)

    if returncode != 0:
        print(f"✗ Ошибка при получении container ID для {pod_name}: {stderr}", file=sys.stderr)
        return ""

    container_id = stdout.strip()

    if not container_id:
        print(f"✗ Пустой container ID для {pod_name}", file=sys.stderr)
        return ""

    if container_id.startswith("containerd://"):
        container_id = container_id.replace("containerd://", "")
    elif container_id.startswith("docker://"):
        container_id = container_id.replace("docker://", "")

    return container_id


def set_write_throttle(pod_name: str, namespace: str, limit_bytes_per_sec: int, dry_run: bool = False) -> bool:
    """
    Устанавливает ограничение скорости записи на диск для пода через minikube ssh.
    Использует подход через поиск процессов Cassandra и их cgroup.

    Args:
        pod_name: Имя пода
        namespace: Kubernetes namespace
        limit_bytes_per_sec: Лимит в байтах в секунду
        dry_run: Только показать команды, не выполнять

    Returns:
        True если успешно, False иначе
    """
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Настройка throttling для пода {pod_name}...")

    # Автоматически определяем базовое блочное устройство и применяем throttling
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

# Определяем cgroup-prefix PID 1 — граница нашего cgroup namespace внутри minikube
ROOT_CG=$(awk -F: '/blkio/{{print $3}}' /proc/1/cgroup 2>/dev/null | head -1)
ROOT_CG="${{ROOT_CG%/}}"

# Применяем throttling к процессам Cassandra (только в kubepods cgroup)
# Используем точное имя класса, чтобы не захватить посторонние SSH-сессии
pgrep -f 'CassandraDaemon' | while read pid; do
    CG=$(awk -F: '/blkio/{{print $3}}' /proc/$pid/cgroup 2>/dev/null | head -1)
    [ -z "$CG" ] && continue
    # Пропускаем процессы не в kubepods (например, сам SSH-сеанс)
    echo "$CG" | grep -q 'kubepods' || continue
    # Убираем корневой prefix, чтобы получить путь относительно нашего cgroup namespace
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
    echo "✓ PID $pid: $MAJOR_MINOR {limit_bytes_per_sec} bytes/s"
done || true
"""

    if dry_run:
        print(f"  [DRY RUN] Bash script:")
        print(textwrap.indent(bash_cmd, "    "))
        return True

    # Используем heredoc для передачи скрипта
    heredoc_cmd = f"sudo bash << 'EOFSCRIPT'\n{bash_cmd}\nEOFSCRIPT"
    returncode, stdout, stderr = run_command(["minikube", "ssh", "--", heredoc_cmd])

    if returncode == 0 and stdout.strip():
        print(f"✓ Throttling установлен")
        for line in stdout.strip().split("\n"):
            print(f"  {line}")
        return True

    print(f"✗ Не удалось установить throttling")
    print(f"  Return code: {returncode}")
    if stdout:
        print(f"  Stdout: {stdout}")
    if stderr:
        print(f"  Stderr: {stderr}")
    return False


def unset_write_throttle(pod_name: str, namespace: str, dry_run: bool = False) -> bool:
    """
    Снимает ограничение скорости записи на диск для пода через minikube ssh.

    Args:
        pod_name: Имя пода
        namespace: Kubernetes namespace
        dry_run: Только показать команды, не выполнять

    Returns:
        True если успешно, False иначе
    """
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Снятие throttling для пода {pod_name}...")

    bash_cmd = """set -euo pipefail
# Определяем базовое блочное устройство через /var/lib/kubelet
DEV_PART=$(df /var/lib/kubelet 2>/dev/null | awk 'NR==2{print $1}')
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
MAJOR_MINOR=$(ls -l "$DEV" | awk '{print $5":"$6}' | sed 's/,//')
if [ -z "$MAJOR_MINOR" ]; then
    echo "ERROR: Cannot determine major:minor for $DEV" >&2
    exit 1
fi

echo "Device: $DEV ($MAJOR_MINOR)"

# Определяем cgroup-prefix PID 1 — граница нашего cgroup namespace внутри minikube
ROOT_CG=$(awk -F: '/blkio/{print $3}' /proc/1/cgroup 2>/dev/null | head -1)
ROOT_CG="${ROOT_CG%/}"

# Снимаем throttling с процессов Cassandra (только в kubepods cgroup)
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
    echo "✓ PID $pid: throttling removed"
done || true
"""

    if dry_run:
        print(f"  [DRY RUN] Bash script:")
        print(textwrap.indent(bash_cmd, "    "))
        return True

    # Используем heredoc для передачи скрипта
    heredoc_cmd = f"sudo bash << 'EOFSCRIPT'\n{bash_cmd}\nEOFSCRIPT"
    returncode, stdout, stderr = run_command(["minikube", "ssh", "--", heredoc_cmd])

    if returncode == 0 and stdout.strip():
        print(f"✓ Throttling снят")
        for line in stdout.strip().split("\n"):
            print(f"  {line}")
        return True

    print(f"✗ Не удалось снять throttling")
    print(f"  Return code: {returncode}")
    if stdout:
        print(f"  Stdout: {stdout}")
    if stderr:
        print(f"  Stderr: {stderr}")
    return False


@click.group()
def cli():
    """
    Управление ограничениями скорости записи на диск для Cassandra-подов в minikube.

    Использует cgroups для ограничения write throughput на уровне контейнера.
    """
    pass


@cli.command(name="set-limit")
@click.argument("limit_mbps", type=float, default=1, required=False)
@click.option("--namespace", default="cassandra", help="Kubernetes namespace (по умолчанию: cassandra)")
@click.option("--pod", help="Имя конкретного пода (если не указано, применяется ко всем Cassandra-подам)")
@click.option("--dry-run", is_flag=True, help="Показать команды без выполнения")
def set_limit(limit_mbps: float, namespace: str, pod: str, dry_run: bool):
    """
    Устанавливает ограничение скорости записи на диск.

    LIMIT_MBPS - лимит в МБ/сек (по умолчанию: 1)

    \b
    Примеры:
      # Установить лимит по умолчанию (1 МБ/сек)
      set_cassandra_throttling.py set-limit

      # Установить лимит 1 МБ/сек
      set_cassandra_throttling.py set-limit 1

      # Установить лимит только для конкретного пода
      set_cassandra_throttling.py set-limit 1 --pod cassandra-0

      # Показать команды без выполнения
      set_cassandra_throttling.py set-limit --dry-run
    """
    limit_bytes_per_sec = int(limit_mbps * 1024 * 1024)

    click.echo(f"{'='*60}")
    click.echo(f"Установка ограничения скорости записи на диск")
    click.echo(f"{'='*60}")
    click.echo(f"Namespace: {namespace}")
    click.echo(f"Лимит: {limit_mbps} МБ/сек ({limit_bytes_per_sec} байт/сек)")
    if dry_run:
        click.echo(f"Режим: DRY RUN (команды не будут выполнены)")
    click.echo(f"{'='*60}")

    if pod:
        pods = [pod]
        click.echo(f"\nИспользуется указанный под: {pod}")
    else:
        click.echo(f"\nПоиск Cassandra-подов в namespace '{namespace}'...")
        pods = get_cassandra_pods(namespace)

        if not pods:
            click.echo(f"✗ Cassandra-поды не найдены в namespace '{namespace}'", err=True)
            sys.exit(1)

        click.echo(f"✓ Найдено подов: {len(pods)}")
        for p in pods:
            click.echo(f"  - {p}")

    success_count = 0
    fail_count = 0

    for p in pods:
        if set_write_throttle(p, namespace, limit_bytes_per_sec, dry_run):
            success_count += 1
        else:
            fail_count += 1

    click.echo(f"\n{'='*60}")
    click.echo(f"Результаты:")
    click.echo(f"  Успешно: {success_count}")
    click.echo(f"  Ошибок: {fail_count}")
    click.echo(f"{'='*60}")

    if fail_count > 0:
        sys.exit(1)


@cli.command(name="unset-limit")
@click.option("--namespace", default="cassandra", help="Kubernetes namespace (по умолчанию: cassandra)")
@click.option("--pod", help="Имя конкретного пода (если не указано, применяется ко всем Cassandra-подам)")
@click.option("--dry-run", is_flag=True, help="Показать команды без выполнения")
def unset_limit(namespace: str, pod: str, dry_run: bool):
    """
    Снимает ограничение скорости записи на диск.

    \b
    Примеры:
      # Снять лимит для всех Cassandra-подов
      set_cassandra_throttling.py unset-limit

      # Снять лимит только для конкретного пода
      set_cassandra_throttling.py unset-limit --pod cassandra-0

      # Показать команды без выполнения
      set_cassandra_throttling.py unset-limit --dry-run
    """
    click.echo(f"{'='*60}")
    click.echo(f"Снятие ограничения скорости записи на диск")
    click.echo(f"{'='*60}")
    click.echo(f"Namespace: {namespace}")
    if dry_run:
        click.echo(f"Режим: DRY RUN (команды не будут выполнены)")
    click.echo(f"{'='*60}")

    if pod:
        pods = [pod]
        click.echo(f"\nИспользуется указанный под: {pod}")
    else:
        click.echo(f"\nПоиск Cassandra-подов в namespace '{namespace}'...")
        pods = get_cassandra_pods(namespace)

        if not pods:
            click.echo(f"✗ Cassandra-поды не найдены в namespace '{namespace}'", err=True)
            sys.exit(1)

        click.echo(f"✓ Найдено подов: {len(pods)}")
        for p in pods:
            click.echo(f"  - {p}")

    success_count = 0
    fail_count = 0

    for p in pods:
        if unset_write_throttle(p, namespace, dry_run):
            success_count += 1
        else:
            fail_count += 1

    click.echo(f"\n{'='*60}")
    click.echo(f"Результаты:")
    click.echo(f"  Успешно: {success_count}")
    click.echo(f"  Ошибок: {fail_count}")
    click.echo(f"{'='*60}")

    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    cli()
