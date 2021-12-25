# mzn-dispatcher

Dispatches solver runs as Kubernetes jobs

## Running during development

You have two options: Using [skaffold](https://skaffold.dev) or deploying
the service manually.

### Skaffold

```bash
skaffold dev --port-forward=services
```

### Manual

If using minikube:

```bash
eval $(minikube docker-env)
```

Then:

```bash
docker build -t mzn-dispatcher:$VERSION .
docker build -t mzn-job-sidecar:$VERSION sidecar
kubectl apply -f mzn-dispatcher.yaml
kubectl port-forward service/mzn-dispatcher-service 8080:8080
```

Make sure that `$VERSION` matches the version in `src/config.py`.
