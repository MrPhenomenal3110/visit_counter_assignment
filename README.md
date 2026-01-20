# Visit Counter Assignment

This is a starter codebase for implementing a distributed visit counter service using FastAPI, Redis, and Docker.

## Architecture Overview

The system is designed with the following components:

1. **FastAPI Application**: Handles HTTP requests and provides REST API endpoints
2. **Redis Cluster**: Multiple Redis instances for distributed storage
3. **Consistent Hashing**: For distributing keys across Redis nodes
4. **Batch Processing**: For optimizing write operations

## Setup Instructions

1. Make sure you have Docker and Docker Compose installed
2. Clone this repository
3. Run the application:
   ```bash
   docker compose up --build
   ```
4. The API will be available at `http://localhost:8000`

## Implementation Tasks

The codebase contains TODOs in various files that need to be implemented:

1. **Consistent Hashing** (`app/core/consistent_hash.py`):

   - Implement the consistent hashing ring
   - Handle node addition and removal
   - Implement key distribution

2. **Redis Manager** (`app/core/redis_manager.py`):

   - Implement connection pooling
   - Handle Redis operations with retries
   - Implement batch operations

3. **Visit Counter Service** (`app/services/visit_counter.py`):
   - Implement visit counting logic
   - Implement batch processing
   - Handle concurrent updates

## API Endpoints

- `POST /visit/{page_id}`: Record a visit
- `GET /visits/{page_id}`: Get visit count

## Testing

You can test the API using curl or any HTTP client:

```bash
# Record a visit
curl -X POST http://localhost:8000/api/v1/counter/visit/123

# Get visit count
curl http://localhost:8000/api/v1/counter/visits/123
```

## CI/CD Pipeline

This project includes a production-grade CI/CD pipeline implemented using GitHub Actions. The pipeline follows DevSecOps principles with security checks integrated at every stage.

### CI Pipeline (`.github/workflows/ci.yml`)

**Trigger**: Automatically runs on push to `master`/`main` branches, or manually via `workflow_dispatch`

#### Pipeline Stages and Reasoning:

1. **Checkout**: Retrieves source code from the repository
   - **Why**: Required to access application code and dependencies

2. **Setup Python**: Configures Python 3.11 runtime with pip caching
   - **Why**: Ensures consistent build environment and speeds up dependency installation

3. **Linting (Ruff)**: Enforces coding standards and identifies code quality issues
   - **Why**: Prevents technical debt accumulation and maintains code consistency
   - **Risk Mitigated**: Catches style violations and potential bugs early

4. **SAST (CodeQL)**: Static Application Security Testing for Python
   - **Why**: Detects code-level vulnerabilities (OWASP Top 10) before deployment
   - **Risk Mitigated**: Identifies security flaws like SQL injection, XSS, insecure deserialization

5. **SCA (pip-audit)**: Software Composition Analysis for Python dependencies
   - **Why**: Identifies vulnerable dependencies in the supply chain
   - **Risk Mitigated**: Prevents shipping applications with known CVEs in dependencies

6. **Unit Tests (pytest)**: Validates business logic correctness
   - **Why**: Ensures code changes don't introduce regressions
   - **Risk Mitigated**: Catches functional bugs before they reach production

7. **Docker Build**: Creates containerized application image
   - **Why**: Packages application for consistent deployment across environments
   - **Uses**: Docker Buildx with GitHub Actions cache for faster builds

8. **Container Image Scan (Trivy)**: Scans Docker image for OS and library vulnerabilities
   - **Why**: Prevents vulnerable container images from being deployed
   - **Risk Mitigated**: Identifies CVEs in base image and installed packages
   - **Output**: Results uploaded to GitHub Security tab

9. **Runtime Test**: Validates container behavior with health check
   - **Why**: Ensures the containerized application starts and responds correctly
   - **Risk Mitigated**: Catches runtime configuration issues before deployment

10. **Registry Push**: Publishes trusted image to DockerHub
    - **Why**: Makes the validated image available for deployment
    - **Tags**: Both commit SHA and `latest` tag for traceability

### CD Pipeline (`.github/workflows/cd.yml`)

**Trigger**:
- Automatic after successful CI run (via `workflow_run`)
- Manual via `workflow_dispatch` with optional `image_tag`

#### Pipeline Stages and Reasoning:

1. **Checkout**: Retrieves source code (for manifests)
   - **Why**: Ensures workflow has access to `k8s/*.yaml`

2. **AWS Authentication**: Configures AWS credentials for EKS access
   - **Why**: Required to authenticate with AWS services
   - **Uses**: `aws-actions/configure-aws-credentials` action

3. **Configure kubeconfig**: Sets up kubectl to connect to EKS cluster
   - **Why**: Enables kubectl commands to interact with the cluster
   - **Method**: `aws eks update-kubeconfig`

4. **Determine Image Tag**: Uses CI commit SHA or manual input
   - **Why**: Guarantees we deploy a specific, traceable image

5. **Deploy to Kubernetes**: Applies manifests and waits for rollout
   - **Why**: Reconciles desired state and performs rolling updates
   - **Method**: `kubectl apply -f k8s/deployment.yaml -f k8s/service.yaml`

6. **DAST - Basic Health Check**: Performs basic dynamic testing
   - **Why**: Validates the deployed service is accessible and responding
   - **Note**: Placeholder for full DAST; production should use dedicated tools

### Required GitHub Configuration

#### Repository Variables (Settings → Secrets and variables → Actions → Variables)
- `DOCKERHUB_USERNAME`: Your DockerHub username
- `IMAGE_NAME`: Name of the Docker image (e.g., `visit-counter`)

#### Repository Secrets (Settings → Secrets and variables → Actions → Secrets)
- `DOCKERHUB_TOKEN`: DockerHub access token for pushing images
- `AWS_ACCESS_KEY_ID`: AWS access key for EKS cluster access
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key
- `AWS_REGION`: AWS region where EKS cluster is located (e.g., `us-east-1`)
- `EKS_CLUSTER_NAME`: Name of your EKS cluster

### Prerequisites for CD Pipeline

Before running the CD pipeline, ensure:
1. EKS cluster is running and accessible
2. Kubernetes manifests exist in `k8s/`:
   - `k8s/deployment.yaml`
   - `k8s/service.yaml`
3. If the DockerHub repo is private, create the image pull secret named `dockerhub-creds`
4. (Optional) Set `K8S_NAMESPACE` repo variable to override the default namespace

Example Deployment manifest (placeholders are replaced in CD):
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: visit-counter
spec:
  replicas: 1
  selector:
    matchLabels:
      app: visit-counter
  template:
    metadata:
      labels:
        app: visit-counter
    spec:
      imagePullSecrets:
      - name: dockerhub-creds
      containers:
      - name: app
        image: DOCKERHUB_USERNAME/IMAGE_NAME:IMAGE_TAG
        ports:
        - containerPort: 8000
```

### Running CI/CD Locally

#### Run CI Checks Locally:

```bash
# Install dependencies
pip install -r requirements.txt
pip install ruff pip-audit pytest httpx

# Run linting
ruff check app/

# Run security scan
pip-audit

# Run tests
pytest tests/ -v

# Build Docker image
docker build -t visit-counter:local .

# Scan image with Trivy (requires Trivy installed)
trivy image visit-counter:local

# Test container runtime
docker run -d --name test-container -p 8000:8000 visit-counter:local
curl http://localhost:8000/
docker stop test-container && docker rm test-container
```

#### Test CD Steps Locally:

```bash
# Configure AWS credentials
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_REGION=your-region

# Configure kubeconfig
aws eks update-kubeconfig --name your-cluster-name --region your-region

# Verify connection
kubectl get nodes

# Deploy update (replace placeholders)
export DOCKERHUB_USERNAME=your-username
export IMAGE_NAME=visit-counter-app
export IMAGE_TAG=latest
sed -i "s|DOCKERHUB_USERNAME|$DOCKERHUB_USERNAME|g" k8s/deployment.yaml
sed -i "s|IMAGE_NAME|$IMAGE_NAME|g" k8s/deployment.yaml
sed -i "s|IMAGE_TAG|$IMAGE_TAG|g" k8s/deployment.yaml
kubectl apply -f k8s/deployment.yaml -n default
kubectl apply -f k8s/service.yaml -n default
kubectl rollout status deployment/visit-counter -n default
```

### Security and Quality Gates

The CI pipeline implements multiple security gates:
- **Code Quality**: Ruff linting fails on style violations
- **Code Security**: CodeQL analysis surfaces vulnerabilities in GitHub Security tab
- **Dependency Security**: pip-audit fails on high/critical CVEs
- **Container Security**: Trivy scan blocks vulnerable images
- **Runtime Validation**: Container health check ensures deployability

**Fail-Fast Philosophy**: Each stage must pass before proceeding to the next, preventing vulnerable or broken code from progressing through the pipeline.

## File Structure

```
.
├── .github/
│   └── workflows/
│       ├── ci.yml          # Continuous Integration pipeline
│       └── cd.yml          # Continuous Deployment pipeline
├── app/
│   ├── api/
│   │   └── v1/
│   │       └── endpoints/
│   │           └── counter.py
│   │       └── api.py
│   ├── core/
│   │   ├── config.py
│   │   ├── consistent_hash.py
│   │   └── redis_manager.py
│   ├── services/
│   │   └── visit_counter.py
│   ├── schemas/
│   │   └── counter.py
│   └── main.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```
