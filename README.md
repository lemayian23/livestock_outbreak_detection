# Livestock Outbreak Detection

> End-to-end platform that detects livestock disease outbreaks before they spread.

A full-stack application that ingests farm health records, validates them against a schema, runs ensemble anomaly detection, and alerts stakeholders via dashboard, email, or API.

---

## Live demo

- **Frontend:** [app.yourdomain.com](https://app.yourdomain.com) *(fill in once deployed)*
- **API docs:** [api.yourdomain.com/docs](https://api.yourdomain.com/docs) *(fill in once deployed)*

---

## Screenshots

| Landing | Detect | Run detail |
|---------|--------|-----------|
| ![Landing](docs/screenshots/landing.png) | ![Detect](docs/screenshots/detect.png) | ![Run detail](docs/screenshots/run-detail.png) |
---

## Architecture

```
┌────────────────────────┐
│  Browser (Next.js)     │  app.yourdomain.com  → Vercel
└───────────┬────────────┘
            │ HTTPS + JWT
            ▼
┌────────────────────────┐
│  FastAPI (Python)      │  api.yourdomain.com  → Render
│  ├─ /auth              │
│  ├─ /v1/detect         │
│  ├─ /v1/validate       │
│  ├─ /runs              │
│  └─ /api-keys          │
└───────────┬────────────┘
            │
            ├─────────────────────────────┐
            ▼                             ▼
┌────────────────────────┐      ┌────────────────────────┐
│  Postgres (Neon)       │      │  R2 / local storage    │
│  users, runs, anomalies│      │  HTML/CSV reports      │
└────────────────────────┘      └────────────────────────┘
```

**Backend** (`src/`)
- `api/` — FastAPI app, auth, models, routers, Alembic-ready
- `anomaly_detection/` — Isolation Forest, statistical, seasonal, ensemble
- `data_validation/` — schema enforcement + business rules
- `data_quality/` — completeness, accuracy, consistency, timeliness
- `config_manager/` — multi-env config, encrypted secrets
- `custom_logging/` — structured JSON logs with rotation
- `monitoring/` — health checks, system metrics
- `reporting/` — HTML report generation

**Frontend** (`frontend/`)
- Next.js 15 (App Router) + TypeScript
- Tailwind CSS + shadcn/ui
- Auth via Bearer JWT + httpOnly refresh cookie
- Recharts for visualizations

---

## Tech stack

| Layer | Technology |
|---|---|
| Frontend | Next.js 15, React 19, Tailwind, shadcn/ui, Recharts |
| Backend | FastAPI, SQLAlchemy 2.0, Alembic, Pydantic v2 |
| Database | PostgreSQL (Neon) |
| Auth | JWT (HS256), bcrypt, httpOnly refresh cookies |
| Deployment | Render (API), Vercel (frontend), Neon (DB) |
| CI | GitHub Actions |
| Monitoring | Structured logs, health endpoints |

---

## Quickstart

### Backend

```bash
git clone https://github.com/lemayian23/livestock_outbreak_detection.git
cd livestock_outbreak_detection

python -m venv venv
venv\Scripts\activate      # Windows
# source venv/bin/activate # macOS/Linux

pip install -r requirements.txt

cp .env.example .env
# Edit .env with your DATABASE_URL and AUTH_SECRET_KEY

alembic upgrade head
python api_main.py
```

API runs at `http://localhost:8000`. Swagger UI at `/docs`.

### Frontend

```bash
cd frontend
npm install
cp .env.local.example .env.local
npm run dev
```

App runs at `http://localhost:3000`.

---

## Environment variables

### Backend (`.env`)
```
DATABASE_URL=postgresql+psycopg://...
AUTH_SECRET_KEY=<random-64-chars>
ACCESS_TOKEN_EXPIRE_MINUTES=15
REFRESH_TOKEN_EXPIRE_DAYS=7
API_KEY=<random-64-chars>
APP_ENV=development
```

### Frontend (`frontend/.env.local`)
```
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
```

---

## API surface

| Endpoint | Method | Description |
|---|---|---|
| `/health` | GET | Liveness check |
| `/auth/signup` | POST | Create account |
| `/auth/login` | POST | Log in |
| `/auth/refresh` | POST | Rotate access token |
| `/auth/me` | GET | Current user |
| `/v1/detect` | POST | Run detection on records |
| `/v1/detect/csv` | POST | Run detection on uploaded CSV |
| `/v1/validate` | POST | Schema validation only |
| `/v1/features` | GET | Feature toggles |
| `/runs` | GET | List user's runs |
| `/runs/{id}` | GET | Run detail |
| `/runs/{id}` | DELETE | Delete run |
| `/api-keys` | GET/POST | Manage API keys |
| `/api-keys/{id}` | DELETE | Revoke key |

Full OpenAPI spec at `/openapi.json`.

---

## Testing

```bash
pytest tests/ -v          # 93 tests
```

Frontend build check:
```bash
cd frontend
npm run build
```

---

## License

MIT — see `LICENSE`.