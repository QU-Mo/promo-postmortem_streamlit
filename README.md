# Promo Postmortem Streamlit

## Overview
This project contains a Streamlit application for post-promotion analysis. The main UI lives in `src/app.py` and depends on the helper modules under `src/`.

## Local setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the application locally:
   ```bash
   streamlit run src/app.py
   ```

## Deployment architecture
This repository now includes the same deployment chain you described for other Streamlit apps:

1. **Docker image**: `Dockerfile` packages the app into a Python 3.11 container and starts Streamlit on port `8501`.
2. **GitHub Actions build pipeline**: `.github/workflows/schneiderflow-build.yml` builds the image on `main`, pushes it to Google Artifact Registry, automatically deploys the development overlay, and supports a manual production promotion through `workflow_dispatch`.
3. **Kubernetes manifests with Kustomize**: `kustomize/base` defines the reusable `Deployment`, `Service`, and `Ingress`, while `kustomize/environments/development` and `kustomize/environments/production` each contain their own `kustomization.yaml`, `deployment.yaml`, and `ingress.yaml` overlays.
4. **Fixed browser links**: each overlay injects a stable hostname through the Ingress resource, so users access the app through a normal URL instead of running `streamlit run` locally.

## Required GitHub variables and secrets
Configure the following repository settings before enabling the workflow:

### Where to configure them in GitHub
Open your repository on GitHub, then go to:

`Settings` → `Secrets and variables` → `Actions`

Inside that page there are two separate tabs:

- **Variables**: add the non-sensitive values under **Repository variables**
- **Secrets**: add the sensitive values under **Repository secrets**

If you want to scope different values by environment, also create the `development` and `production` environments under:

`Settings` → `Environments`

Then add environment-specific variables/secrets there instead of only at the repository level. This matches the workflow configuration, because the deployment jobs explicitly run with `environment: development` and `environment: production`.

### Recommended split
- **Repository-level variables/secrets**: values shared by every workflow run, especially the image build settings used before an environment is selected
- **Environment-level variables/secrets**: values that differ between `development` and `production`, especially ingress hostnames, TLS secret names, and cluster-specific deployment settings

In practice, a good default is:

#### Keep at repository level
- `GCP_PROJECT_ID`
- `GAR_LOCATION`
- `GAR_REPOSITORY`
- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_SERVICE_ACCOUNT`

#### Put inside each GitHub Environment
- `GKE_CLUSTER_NAME`
- `GKE_CLUSTER_LOCATION`
- `DEV_HOST`
- `DEV_TLS_SECRET`
- `PROD_HOST`
- `PROD_TLS_SECRET`

If development and production use different GCP projects or service accounts, you can also move the relevant `GCP_*` settings into the corresponding environment.

### Repository variables
- `GCP_PROJECT_ID`
- `GAR_LOCATION`
- `GAR_REPOSITORY`
- `GKE_CLUSTER_NAME`
- `GKE_CLUSTER_LOCATION`
- `DEV_HOST`
- `DEV_TLS_SECRET`
- `PROD_HOST`
- `PROD_TLS_SECRET`

### Repository secrets
- `GCP_WORKLOAD_IDENTITY_PROVIDER`
- `GCP_SERVICE_ACCOUNT`

### What each value is used for
- `GCP_PROJECT_ID`: Google Cloud project that owns Artifact Registry and the Kubernetes cluster
- `GAR_LOCATION`: Artifact Registry region, such as `europe-west1` or `us-central1`
- `GAR_REPOSITORY`: Artifact Registry Docker repository name
- `GKE_CLUSTER_NAME`: target GKE cluster name
- `GKE_CLUSTER_LOCATION`: target GKE cluster region or zone
- `DEV_HOST`: public hostname for the development ingress, for example `promo-postmortem.dev.example.com`
- `DEV_TLS_SECRET`: Kubernetes TLS secret name used by the development ingress
- `PROD_HOST`: public hostname for the production ingress
- `PROD_TLS_SECRET`: Kubernetes TLS secret name used by the production ingress
- `GCP_WORKLOAD_IDENTITY_PROVIDER`: Workload Identity Federation provider used by GitHub Actions
- `GCP_SERVICE_ACCOUNT`: service account email used by the workflows to push images and deploy manifests

### Copy-ready GitHub configuration checklist

#### Repository variables
Add these in `Settings` → `Secrets and variables` → `Actions` → `Variables`:

- `GCP_PROJECT_ID=<your-shared-gcp-project-id>`
- `GAR_LOCATION=<artifact-registry-region>`
- `GAR_REPOSITORY=<artifact-registry-repository-name>`

Where to find them:

- `GCP_PROJECT_ID`: in Google Cloud Console, open the project selector at the top of the page; the **Project ID** is shown next to the project name
- `GAR_LOCATION`: open **Artifact Registry** in Google Cloud Console; the repository list shows the region/location for each repository, such as `europe-west1` or `us-central1`
- `GAR_REPOSITORY`: open **Artifact Registry** in Google Cloud Console and copy the Docker repository name you want GitHub Actions to push into

#### Repository secrets
Add these in `Settings` → `Secrets and variables` → `Actions` → `Secrets`:

- `GCP_WORKLOAD_IDENTITY_PROVIDER=<projects/.../locations/global/workloadIdentityPools/.../providers/...>`
- `GCP_SERVICE_ACCOUNT=<github-actions-deployer@your-project.iam.gserviceaccount.com>`

#### Development environment
Create `development` in `Settings` → `Environments`, then add:

**Variables**
- `GKE_CLUSTER_NAME=<development-gke-cluster-name>`
- `GKE_CLUSTER_LOCATION=<development-gke-region-or-zone>`
- `DEV_HOST=<promo-postmortem-dev.your-domain.com>`
- `DEV_TLS_SECRET=<development-ingress-tls-secret-name>`

**Optional secrets**
- Add environment-specific `GCP_WORKLOAD_IDENTITY_PROVIDER` or `GCP_SERVICE_ACCOUNT` here if development uses separate credentials

#### Production environment
Create `production` in `Settings` → `Environments`, then add:

**Variables**
- `GKE_CLUSTER_NAME=<production-gke-cluster-name>`
- `GKE_CLUSTER_LOCATION=<production-gke-region-or-zone>`
- `PROD_HOST=<promo-postmortem.your-domain.com>`
- `PROD_TLS_SECRET=<production-ingress-tls-secret-name>`

**Optional secrets**
- Add environment-specific `GCP_WORKLOAD_IDENTITY_PROVIDER` or `GCP_SERVICE_ACCOUNT` here if production uses separate credentials

## Manifest validation
The `schneiderflow-manifests.yml` workflow renders both overlays on pull requests so deployment changes are validated before merge.
