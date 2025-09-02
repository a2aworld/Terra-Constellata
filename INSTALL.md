# Terra Constellata Installation Guide

## Python Environment Setup

1. Verified Python version: 3.13.5 (compatible with 3.11+)

2. Created virtual environment in d:/VS/terra-constellata/venv

3. Activated virtual environment

4. Installed core packages:
   - PyTorch
   - LangChain
   - NetworkX
   - spaCy
   - NLTK
   - Scikit-learn
   - GeoPandas
   - Shapely
   - Pandas
   - FastAPI
   - Uvicorn

5. Installed ArangoDB Python driver: python-arango

## PostgreSQL with PostGIS Installation

To install PostgreSQL with PostGIS extension:

1. Download and install PostgreSQL from the official website: https://www.postgresql.org/download/

2. During the installation process, select the PostGIS extension when prompted, or install it separately using the Stack Builder tool included with PostgreSQL.

3. After installation, create a new database or use an existing one.

4. Enable PostGIS in your database by running the following SQL command in pgAdmin or psql:

   ```sql
   CREATE EXTENSION postgis;
   ```

5. Verify the installation by running:

   ```sql
   SELECT PostGIS_Version();
   ```

This should return the version of PostGIS installed.

For more detailed instructions, refer to the PostGIS documentation: https://postgis.net/documentation/

## Docker Container Setup

For cloud-native backend deployment with local containers:

### Prerequisites
- Docker installed and running
- Docker Compose installed
- At least 4GB RAM available
- 10GB free disk space

### Quick Start

1. **Clone and navigate to the project directory:**
   ```bash
   cd terra-constellata
   ```

2. **Start the backend services:**
   ```bash
   ./start.sh
   ```

3. **Verify services are running:**
   - PostgreSQL (PostGIS): http://localhost:5432
   - ArangoDB: http://localhost:8529 (admin interface)
   - A2A Protocol Server: http://localhost:8080/health

### Service Details

- **PostgreSQL with PostGIS**: Spatial database for geospatial data
- **ArangoDB**: Graph database for Cognitive Knowledge Graph (CKG)
- **A2A Protocol Server**: JSON-RPC server for agent-to-agent communication

### Management Scripts

- `./start.sh` - Start all services
- `./stop.sh` - Stop all services
- `./logs.sh [service]` - View logs (service: all, postgres, arangodb, a2a-server)

### Environment Configuration

Edit `.env` file to customize:
- Database credentials
- Port mappings
- Service configurations

### Data Persistence

Data is persisted in Docker volumes:
- `postgres_data` - PostgreSQL data
- `arangodb_data` - ArangoDB data
- `arangodb_apps` - ArangoDB apps

### Scaling Alternatives

#### Local Development
- Use Docker Compose for single-node development
- Access services via localhost ports
- Hot-reload available for A2A server code changes

#### Kubernetes Deployment
For production scaling:

1. **Minikube (Local Kubernetes):**
   ```bash
   minikube start
   kubectl apply -f k8s/
   ```

2. **Docker Swarm:**
   ```bash
   docker stack deploy -c docker-compose.yml terra-stack
   ```

3. **Cloud Kubernetes:**
   - AWS EKS
   - Google GKE
   - Azure AKS

#### Alternative Orchestrators
- **Podman**: Drop-in Docker replacement
- **Nomad**: HashiCorp's workload orchestrator
- **Docker Compose with Swarm**: For multi-node setups

### Health Checks

All services include health checks:
- PostgreSQL: Database connectivity
- ArangoDB: API endpoint availability
- A2A Server: Health endpoint response

### Troubleshooting

- **Port conflicts**: Edit ports in `.env` file
- **Memory issues**: Increase Docker memory allocation
- **Permission errors**: Ensure Docker has proper permissions
- **Data reset**: Use `docker-compose down -v` to remove volumes

## Verification

Run `pip list` in the activated virtual environment to verify all packages are installed.