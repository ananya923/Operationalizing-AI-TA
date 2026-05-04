# Week 2 — Deployment & CI/CD

## Assignment

Deploy the pre-trained API to GKE and set up a reliable CI/CD pipeline with GitHub Actions.

**Deliverables:**
- Live API endpoint (public IP)
- Architecture diagram (GitHub → GCR → GKE)
- GitHub repo with working CI/CD
- 1-2 page design report

## What You're Working With

- Pre-trained LightGBM model (lgbm_demand_model.txt, 11 MB)
- Dataset (demand_enriched.parquet, 74 MB)
- FastAPI backend (app/backend/)
- Metadata (zone lookup, zone coordinates, fare averages)

The API must:
- Serve on port 8000
- Load model + data at startup (~10s)
- Handle requests in <100ms
- Respond to 10+ concurrent requests
- Gracefully degrade if data unavailable

## Part 1: GCP Setup

1. Create GCP project
2. Enable required APIs:
   ```bash
   gcloud services enable container.googleapis.com artifactregistry.googleapis.com
   ```
3. Create GCS bucket and upload files:
   ```bash
   gsutil mb gs://[bucket-name]
   gsutil cp data/processed/demand_enriched.parquet gs://[bucket-name]/
   gsutil cp data/processed/lgbm_demand_model.txt gs://[bucket-name]/
   ```
4. Create GKE cluster (2-node, auto-scaling):
   ```bash
   gcloud container clusters create operationalizing-ai \
     --zone us-central1-a --num-nodes 2 --machine-type n1-standard-2 \
     --enable-autoscaling --min-nodes 2 --max-nodes 5
   ```
5. Create Artifact Registry:
   ```bash
   gcloud artifacts repositories create docker-repo \
     --repository-format=docker --location=us-central1
   ```

## Part 2: Container & Kubernetes

Templates are in `starter/`. You'll need:
- Dockerfile (containerize the FastAPI app)
- k8s/ manifests (configmap, deployment, service)

Build and test locally before deploying to GKE. Verify `/health` endpoint returns 200 and handles concurrent requests.

Deploy to GKE using kubectl apply. Verify the LoadBalancer service gets a public IP and is accessible.

## Part 3: GitHub Actions CI/CD

Workflow templates are in `starter/.github/workflows/`. Design:
- CI pipeline: when does it run? what validation blocks deployment?
- CD pipeline: how does GitHub authenticate to GCP? how is the image tagged and pushed? how is GKE updated?

Set up GitHub secrets for GCP authentication (project ID, cluster info, etc.). Use Workload Identity for secure, credential-file-free auth.

Verify workflows run on push/merge and deployment completes successfully.

## Part 4: Testing Deployment

```bash
# Health check
curl https://[EXTERNAL-IP]/health

# Heatmap
curl "https://[EXTERNAL-IP]/api/heatmap?hour=17&dow=4&date=2026-01-15&holiday=regular"

# Forecast
curl "https://[EXTERNAL-IP]/api/forecast?zone_id=42&hour=17&dow=4&steps=16&date=2026-01-15"

# Recommendations
curl "https://[EXTERNAL-IP]/api/recommendations?zone_id=42&hour=17&dow=4&n=3&date=2026-01-15"
```

## Deliverables

Submit on Canvas:

1. **Live API endpoint** (public IP or DNS)
2. **Architecture diagram** (GitHub → GCR → GKE) as PDF/image
3. **GitHub repo link** (make it private, include in submission)
4. **Design report** (1-2 pages):
   - Architecture overview
   - Why GKE + GitHub Actions? (vs other platforms)
   - Operational assumptions (data freshness, scaling limits, failure modes)
   - Tradeoffs and limitations

## Grading

| Criterion | Weight |
|-----------|--------|
| API live & responding | 30% |
| CI/CD functional (Actions run, deploys work) | 25% |
| Kubernetes setup correct | 20% |
| Design reasoning documented | 15% |
| Code quality (organized, efficient Dockerfile) | 10% |

## Notes

- Test Docker image locally before pushing to GCR
- Model load takes ~10s; set readiness probe `initialDelaySeconds: 20`
- GKE costs ~$0.10-0.15/hour; delete cluster when done with `gcloud container clusters delete`
- Don't commit real credentials; use GitHub secrets
- Rolling updates: deploy one pod at a time to minimize downtime
- If pod crashes, check logs: `kubectl logs [pod-name]`
- If image won't pull, check GCR permissions and image name in deployment spec

## Due

End of Week 2 (see syllabus)
