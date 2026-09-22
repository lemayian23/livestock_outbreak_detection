# Deployment Guide

This document captures the exact production setup for the Livestock Outbreak
Detection platform. Follow it to redeploy from scratch or to bring up a new
environment.

**Live production URLs**

- Frontend: https://app.lemayian.com
- Backend API: https://api.lemayian.com
- API health: https://api.lemayian.com/health
- API docs (Swagger): https://api.lemayian.com/docs

**Deployment topology**

| Component | Platform | Notes |
|---|---|---|
| Frontend (Next.js 15) | NovaHost cPanel Starter Cloud | Node.js 20 via Passenger |
| Backend (FastAPI) | Render | Free tier, Python 3.11 |
| Database (Postgres) | Neon | Free tier, pooled connection |
| DNS | NovaHost Zone Editor | CNAME for `api`, A for `app` |
| CI | GitHub Actions | Backend tests + frontend build |

---

## 1. Prerequisites

- GitHub account with push access to the repo
- Render account (free)
- Neon account (free)
- NovaHost cPanel access for `lemayian.com`
- Node.js 20 installed locally (for frontend builds)
- Python 3.11+ installed locally

---

## 2. Database — Neon

### 2.1 Create the project

1. Sign up at https://neon.tech
2. Click **Create project**
   - Name: `livestock-detection`
   - Region: **AWS eu-central-1 (Frankfurt)** (closest to Kenya)
   - Postgres version: 16 (default)
3. On the project page, open the **Connection string** panel
4. Set the dropdown to **Pooled connection**
5. Copy the string. It looks like:

```
postgresql://neondb_owner:PASSWORD@ep-xxx-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require
```

**Why pooled:** serverless/Render workloads open and close connections
frequently. The pooler handles churn gracefully.

### 2.2 Verify from your machine (optional)

```bash
set DATABASE_URL=postgresql://neondb_owner:PASSWORD@ep-xxx-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require
python -c "from src.api.database import engine; from sqlalchemy import text; c = engine.connect(); print('OK', c.execute(text('select 1')).scalar()); c.close()"
set DATABASE_URL=
```

Expected: `OK 1`

---

## 3. Backend — Render

### 3.1 Create the service

1. Render Dashboard → **New +** → **Web Service**
2. Connect your GitHub repo: `lemayian23/livestock_outbreak_detection`
3. Fill the form:

| Field | Value |
|---|---|
| Name | `livestock-outbreak-detection-api` |
| Region | Frankfurt (or nearest) |
| Branch | `main` |
| Root Directory | *(empty)* |
| Runtime | Python 3 |
| Build Command | `pip install -r requirements.txt && pip install -e . && alembic upgrade head` |
| Start Command | `uvicorn src.api.main:app --host 0.0.0.0 --port $PORT` |
| Instance Type | Free |

The build command runs Alembic migrations before the API boots, so every
deploy applies any new migrations to Neon.

### 3.2 Environment variables

Render → **Environment** → **Add Environment Variable**:

| Key | Value |
|---|---|
| `APP_ENV` | `production` |
| `DATABASE_URL` | the Neon connection string from §2 |
| `AUTH_SECRET_KEY` | `python -c "import secrets; print(secrets.token_urlsafe(48))"` |
| `API_KEY` | same command, different output |
| `LOG_LEVEL` | `INFO` |
| `LOG_JSON_FORMAT` | `true` |

### 3.3 Health check

Render → **Settings** → **Health Check Path**:

```
/health
```

### 3.4 Trigger the first deploy

Click **Manual Deploy → Deploy latest commit**. Watch the logs for:

```
INFO  [alembic.runtime.migration] Running upgrade  -> <hash>, initial schema
==> Running 'uvicorn src.api.main:app --host 0.0.0.0 --port $PORT'
INFO:     Application startup complete.
```

Confirm:

```bash
curl https://YOUR-SERVICE.onrender.com/health
```

Expected:

```json
{"status":"ok","env":"production", ...}
```

### 3.5 Custom domain — `api.lemayian.com`

**In Render:**

1. Service → **Settings** → **Custom Domains** → **Add Custom Domain**
2. Enter `api.lemayian.com`
3. Render shows the CNAME target (e.g. `livestock-outbreak-detection-api.onrender.com`)

**In NovaHost cPanel:**

1. cPanel → **Zone Editor** → **Manage** next to `lemayian.com`
2. **Add Record**:
   - Type: `CNAME`
   - Name: `api`
   - Record: `livestock-outbreak-detection-api.onrender.com`
   - TTL: `3600`
3. Save

Wait 5–30 minutes. Render auto-issues an SSL certificate once DNS resolves.
Verify:

```bash
curl https://api.lemayian.com/health
```

---

## 4. Frontend — NovaHost cPanel (Node.js 20)

The frontend is deployed as a Next.js **standalone build** running under
cPanel's Node.js Selector (Passenger).

### 4.1 Create the subdomain

cPanel → **Domains** → **Create a New Domain**:

- Type: **Subdomain**
- Domain: `app.lemayian.com`
- **Uncheck** "Share document root with lemayian.com"

cPanel creates `/home/lemayian/app.lemayian.com/`.

### 4.2 Create the Node.js app

cPanel → **Setup Node.js App** → **Create Application**:

| Field | Value |
|---|---|
| Node.js version | `20.x` |
| Application mode | Production |
| NODE_ENV value | `production` |
| Application root | `app.lemayian.com` |
| Application URL | `app.lemayian.com` |
| Application startup file | `server.js` |

Environment variables to add:

| Name | Value |
|---|---|
| `NEXT_PUBLIC_API_BASE_URL` | `https://api.lemayian.com` |
| `NODE_ENV` | `production` |
| `HOSTNAME` | `0.0.0.0` |
| `PORT` | `8080` |

**`PORT=8080` is critical.** Without it, Next.js binds to the default 3000,
Passenger can't reach the app, and the logs show
`[Error: Server is not running.] { code: 'ERR_SERVER_NOT_RUNNING' }`.

Click **Create**.

### 4.3 Build the standalone bundle locally

From the repo root:

```bash
cd frontend

# Force the production API URL into the client bundle
set NEXT_PUBLIC_API_BASE_URL=https://api.lemayian.com

# Clean previous build
rmdir /s /q .next

# Build
npm run build
```

Verify the URL is baked in (should print many matches):

```bash
findstr /s /i "api.lemayian.com" .next\static\chunks\app\*.js
```

Verify no dev URLs remain (should print nothing):

```bash
findstr /s /i "localhost:8000" .next\static\chunks\app\*.js
```

### 4.4 Assemble the deploy folder

```bash
rmdir /s /q deploy
mkdir deploy
xcopy .next\standalone\frontend\* deploy\ /E /I /Y
xcopy .next\static\* deploy\.next\static\ /E /I /Y
```

Verify `server.js` sits at the top:

```bash
dir deploy\server.js
```

### 4.5 Zip it

```bash
cd deploy
tar -a -c -f ..\deploy.zip *
cd ..
```

Or right-click the `deploy` folder → Send to → Compressed (zipped) folder.

### 4.6 Upload to cPanel

cPanel → **File Manager** → `/home/lemayian/app.lemayian.com/`.

If this is a **fresh deploy**:
1. Upload `frontend/deploy.zip`
2. Right-click → **Extract** → Overwrite existing files
3. Delete `deploy.zip`

If this is a **redeploy** (files already exist):
1. Stop the app in **Setup Node.js App** → **Stop App**
2. Delete `node_modules/`, `.next/`, `server.js`, `package.json`
   — do **not** delete `.well-known/`, `.htaccess`, `cgi-bin/`,
     `php.ini`, `.user.ini`, `tmp/`
3. Upload and extract `deploy.zip`
4. Delete `deploy.zip`

### 4.7 NPM Install

**Important:** the uploaded zip contains a `node_modules/` folder, which
cPanel's CloudLinux Node.js Selector refuses to work with. Delete it before
installing:

1. File Manager → `/home/lemayian/app.lemayian.com/` → delete `node_modules/`
2. cPanel → **Setup Node.js App** → **Run NPM Install**
3. Wait for completion

cPanel will recreate `node_modules` as a symlink into the virtual environment.

### 4.8 Restart and test

1. **Setup Node.js App** → **Restart**
2. Wait 15 seconds. Status should be **Started**
3. Open https://app.lemayian.com in an **incognito** window

### 4.9 SSL for the frontend

cPanel → **SSL/TLS Status** → tick `app.lemayian.com` → **Run AutoSSL**

Takes 1–3 minutes. After it completes, `https://app.lemayian.com` serves with
a valid Let's Encrypt certificate.

---

## 5. CORS

The backend must allow the frontend origin.

Edit `config/settings.yaml`:

```yaml
api:
  cors_origins:
    - "http://localhost:3000"
    - "http://localhost:3001"
    - "http://127.0.0.1:3000"
    - "https://app.lemayian.com"
```

Commit and push. Render redeploys automatically.

Verify CORS is live:

```bash
curl -i -X OPTIONS https://api.lemayian.com/auth/signup ^
  -H "Origin: https://app.lemayian.com" ^
  -H "Access-Control-Request-Method: POST" ^
  -H "Access-Control-Request-Headers: content-type"
```

Look for:

```
access-control-allow-origin: https://app.lemayian.com
```

---

## 6. CI/CD

`.github/workflows/ci.yml` runs on every push and PR:

- **backend** — installs Python 3.11 deps, runs `pytest`
- **frontend** — installs Node 20 deps, runs `tsc --noEmit` and `next build`

Both jobs must pass before merging. CI does **not** deploy — Render deploys
on its own when `main` is pushed, and cPanel deploys are manual.

---

## 7. Common issues and fixes

### `[Error: Server is not running.] { code: 'ERR_SERVER_NOT_RUNNING' }`

The Node app crashed within a second of starting. Fixes, in order:

1. Set `PORT=8080` in cPanel env vars (§4.2)
2. Verify `server.js` is at the top of the app root
3. Open cPanel **Terminal** and run:

   ```
   source /home/lemayian/nodevenv/app.lemayian.com/20/bin/activate && cd /home/lemayian/app.lemayian.com && node server.js
   ```

   The output will show the real error. Press Ctrl+C to stop.

### `Cloudlinux NodeJS Selector demands to store node modules ...`

You uploaded a real `node_modules/` folder. Delete it, then run **NPM Install**
(§4.7).

### Frontend fetches `http://localhost:8000` in production

The bundle was built without `NEXT_PUBLIC_API_BASE_URL` set. Rebuild locally
per §4.3 with the `set` command, then redeploy.

### `/runs/{id}` returns 404 after detection

The API returned the session ID instead of the DB UUID. Ensure
`src/api/routes.py` uses `run_id=db_run.id if db_run else run_id` in the
`detect` handler.

### Neon connection fails with SSL error

Make sure the connection string ends with `?sslmode=require`.

### Render free tier cold starts

After 15 minutes idle, the first request takes 30–60 seconds. This is a
limitation of the free tier. Upgrade to Starter ($7/mo) to remove it.

---

## 8. Redeploying after code changes

### Backend change

```bash
git add .
git commit -m "..."
git push origin main
```

Render auto-deploys. Watch the logs for a successful deploy.

### Frontend change

1. Rebuild per §4.3 through §4.5
2. Upload the new `deploy.zip` per §4.6
3. Delete `node_modules/`, then **Run NPM Install** (§4.7)
4. **Restart** the app (§4.8)

### Database schema change

1. Make the model change in `src/api/models.py`
2. Locally:

   ```bash
   set DATABASE_URL=sqlite:///./local_dev.db
   alembic revision --autogenerate -m "describe change"
   alembic upgrade head
   ```

3. Commit and push the new migration file
4. Render's build command runs `alembic upgrade head` on the next deploy

---

## 9. Rollback

**Backend:** Render → **Deploys** → find the last good deploy → **Redeploy**.

**Frontend:** re-upload the previous `deploy.zip` and restart. Keep the last
known-good zip in `frontend/deploy-history/` if you want a fast rollback.

**Database:** Neon keeps point-in-time backups on paid plans. On the free
tier, restore from the last Alembic migration and re-apply data if needed.

---

## 10. Checklist for a fresh deploy

- [ ] Neon project created, pooled connection string copied
- [ ] Render Web Service created, env vars set, migrations run on first build
- [ ] `api.lemayian.com` CNAME added, SSL issued
- [ ] Frontend built with `NEXT_PUBLIC_API_BASE_URL=https://api.lemayian.com`
- [ ] `deploy.zip` uploaded and extracted to `/home/lemayian/app.lemayian.com/`
- [ ] `node_modules/` deleted, NPM Install run in cPanel
- [ ] cPanel env vars include `PORT=8080`, `HOSTNAME=0.0.0.0`, `NODE_ENV=production`, `NEXT_PUBLIC_API_BASE_URL=https://api.lemayian.com`
- [ ] App restarted, status = Started
- [ ] `app.lemayian.com` loaded in incognito
- [ ] SSL issued for `app.lemayian.com`
- [ ] CORS on backend includes `https://app.lemayian.com`
- [ ] Signup works end-to-end
- [ ] `https://api.lemayian.com/health` returns 200