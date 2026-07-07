# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DeepVision Studio is a deep learning teaching visualization platform with multiple teaching modes (A-F), real model execution via Python services, and collaborative features. The project consists of three interconnected services.

## Architecture

```
Browser (Angular SPA)
  └── Spring Boot Backend (port 8080)
        ├── Auth, JWT, H2 database
        ├── Training orchestration + WebSocket
        ├── LLM proxy + SSE streaming
        └── Proxy → Python forward service (port 5000)
                   └── NumPy forward computation

         Python PyTorch Training Worker
              └── Spawned per training task
                  └── Real PyTorch training + checkpoint
```

## Commands

### Frontend (Angular 20)
```bash
cd frontend
npm install
npm start                    # Dev server with proxy (http://localhost:4200)
npm run build                # Production build
ng test                      # Run tests
```

### Spring Backend
```bash
cd backend/spring
mvn spring-boot:run          # Dev mode
mvn test                     # Run tests
```

### Python Forward Service
```bash
cd backend/python-forward
pip install -r requirements.txt
python app.py                # Runs on port 5000
```

### Docker Deployment
```bash
docker compose up --build    # Full stack
# Services: frontend:4200, spring:8080 (internal), python-forward:5000 (internal)
```

### Resource Sync (Mode D Transformer)
```powershell
cd scripts
./sync-transformer-explainer.ps1   # Sync ONNX/tokenizer assets
```

## Key Architectural Decisions

### Two Inference Patterns
1. **Server-side inference (Modes A, B)**: Angular → Spring → Python (NumPy/PyTorch)
2. **Browser-side inference (Modes C, D)**: Angular loads TF.js or ONNX-runtime-web directly, no backend involvement

### Training Flow (Mode B)
```
Angular → POST /api/training/start
  → Spring creates training-jobs/<jobId>/
  → Spring spawns training_worker.py as subprocess
  → Python outputs JSON events to stdout
  → Spring parses and forwards via WebSocket /api/training/stream
  → Python saves checkpoint.pt on completion
  → Spring stores metadata in H2, file path for weights
```

### Data Storage Strategy
- H2 database: User accounts, dataset metadata, checkpoint metadata, forward records
- File storage: Training datasets, uploaded files, checkpoint.pt, preview images
- WebSocket: Training metrics stream (in-memory), chat messages (in-memory, auto-cleanup)

### Authentication
- JWT tokens, stateless auth
- Frontend stores token in localStorage
- Spring validates on protected endpoints
- WebSocket connections validate JWT from query parameter

## Frontend Structure

| Directory | Purpose |
|-----------|---------|
| `core/auth/` | Login, register, JWT handling |
| `shared/` | Reusable components: network visualization, LLM floating assistant, teaching glossary, simulation engine, training services |
| `modes/` | Mode A-F pages, each with sub-components, services, and explainer modules |
| `shell/` | Home page, teaching documentation |
| `public/` | Static assets: sample images, mode-c/mode-d-assets |

### Module Import Paths
```typescript
import { XComponent } from '@modes/mode-a/...';    // Modes
import { XService } from '@shared/simulation/...'; // Shared
import { AuthService } from '@core/auth/...';      // Core
import { XComponent } from '@shell/home/...';      // Shell
```

## Backend Structure (Spring Boot)

| Package | Responsibility |
|---------|----------------|
| `auth/` | User registration, login, JWT generation, BCrypt passwords |
| `forward/` | Proxy to Python forward service, forward record persistence |
| `training/` | Dataset management, training job orchestration, checkpoint, WebSocket streams |
| `llm/` | Proxy to LLM API with SSE streaming support |
| `museum/` | AI Museum presence WebSocket (multiplayer avatar tracking) |
| `common/` | Security config, CORS, OpenAPI docs, exception handling |

## Common Development Patterns

### Adding a New Mode
1. Add route in `frontend/src/app/app.routes.ts` with `loadComponent`
2. Create mode page component in `frontend/src/app/modes/mode-X/`
3. Add TypeScript interfaces in shared models
4. For server-side inference: add Spring controller + service + DTOs
5. For browser-side inference: add StateService + InferenceService

### Training WebSocket Events
```typescript
// Event types from Python worker stdout:
{ type: 'metric', epoch, batch, loss, accuracy, ... }
{ type: 'backprop', phase, gradNorms, layerStats, ... }
{ type: 'test_result', testLoss, testAccuracy, samples, ... }
{ type: 'control', status: 'running'|'paused'|'stopped'|'completed' }
{ type: 'error', message }
```

### Backend Config Environment Variables
| Variable | Purpose |
|----------|---------|
| `DEEPVISION_JWT_SECRET` | JWT signing key |
| `DEEPVISION_FORWARD_BASE_URL` | Python forward service URL |
| `DEEPVISION_DATASET_ROOT` | Training dataset storage |
| `DEEPVISION_TRAINING_JOBS_ROOT` | Training job working directory |
| `DEEPVISION_TRAINING_PYTHON` | Python executable path |
| `ARK_API_KEY` | LLM API key |
