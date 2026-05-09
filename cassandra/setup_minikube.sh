minikube delete

# Если вы сидите на Ubuntu и хотите запускать minikube c driver=docker:
# To avoid this error: https://kind.sigs.k8s.io/docs/user/known-issues/#pod-errors-due-to-too-many-open-files
sudo sysctl fs.inotify.max_user_watches=524288
sudo sysctl fs.inotify.max_user_instances=512

# Поехали...
minikube start --driver=docker --cpus=6 --memory=32g --disk-size=60g --docker-opt default-ulimit=nofile=65536:65536

minikube addons enable metrics-server
# To avoid PVC errors
minikube addons enable default-storageclass
minikube addons enable storage-provisioner

echo "Проверяем статус minikube:"
minikube status