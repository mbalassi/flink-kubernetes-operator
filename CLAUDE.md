# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

### Basic Build
```bash
mvn clean install
```

### Fast Build (skip tests, parallel)
```bash
mvn clean install -DskipTests -T 1C
```

### Run Tests
```bash
mvn test
```

### Run Integration Tests
```bash
mvn verify -Dit.skip=false
```

### Run Single Test
```bash
mvn test -Dtest=<TestClassName>
```

### Code Quality Checks
```bash
mvn checkstyle:check        # Run checkstyle
mvn spotless:check          # Check code formatting
mvn spotless:apply          # Apply code formatting
```

### Build Docker Image
```bash
docker build . -t <repo>/flink-kubernetes-operator:latest
```

## Project Architecture

This is the Apache Flink Kubernetes Operator, a multi-module Maven project that manages Flink applications on Kubernetes.

### Key Modules
- **flink-kubernetes-operator**: Main operator implementation with controllers and reconcilers
- **flink-kubernetes-operator-api**: Custom Resource Definitions (CRDs) and API objects
- **flink-autoscaler**: Autoscaling functionality for Flink jobs
- **flink-kubernetes-webhook**: Admission webhooks
- **e2e-tests**: End-to-end test scripts

### Core Components

#### Controllers (flink-kubernetes-operator/src/main/java/org/apache/flink/kubernetes/operator/controller/)
- `FlinkDeploymentController`: Manages FlinkDeployment resources
- `FlinkSessionJobController`: Manages FlinkSessionJob resources
- `FlinkStateSnapshotController`: Manages state snapshots
- `FlinkBlueGreenDeploymentController`: Blue/green deployment support

#### Reconcilers (flink-kubernetes-operator/src/main/java/org/apache/flink/kubernetes/operator/reconciler/)
- Implement the core reconciliation logic for different resource types
- `ApplicationReconciler`: For standalone Flink applications
- `SessionReconciler`: For session-based deployments
- `StateSnapshotReconciler`: For checkpoint/savepoint management

#### API Objects (flink-kubernetes-operator-api/src/main/java/org/apache/flink/kubernetes/operator/api/)
- `FlinkDeployment`: Main CRD for Flink application deployments
- `FlinkSessionJob`: CRD for session job deployments
- `FlinkStateSnapshot`: CRD for checkpoint/savepoint management
- `FlinkBlueGreenDeployment`: CRD for blue/green deployments

### Key Packages
- `config/`: Configuration management and operator configuration
- `metrics/`: Metrics collection and reporting
- `observer/`: Resource observation and status monitoring
- `service/`: Core services for job lifecycle management
- `utils/`: Utility classes for Kubernetes operations
- `validation/`: Resource validation logic

## Development Environment

### Prerequisites
- Java 17 (required)
- Maven 3
- Docker Desktop
- minikube
- helm

### Local Setup
```bash
# Build and load image to minikube
eval $(minikube docker-env)
DOCKER_BUILDKIT=1 docker build . -t flink-kubernetes-operator:latest

# Install operator locally
helm install flink-kubernetes-operator helm/flink-kubernetes-operator \
  --set image.repository=flink-kubernetes-operator \
  --set image.tag=latest
```

### Code Style
- Uses Google Java Format (AOSP style)
- Spotless plugin enforces formatting
- Checkstyle enforces code quality rules
- Import order: `org.apache.flink`, `org.apache.flink.shaded`, empty line, `javax`, `java`, `scala`, static imports

### Testing
- Unit tests: `mvn test`
- Integration tests: `mvn verify -Dit.skip=false`
- E2E tests: Shell scripts in `e2e-tests/` directory
- Test naming: Integration tests end with `ITCase`