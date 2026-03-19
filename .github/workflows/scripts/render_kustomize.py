from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: render_kustomize.py <development|production>")

    environment = sys.argv[1]
    if environment not in {"development", "production"}:
        raise SystemExit(f"Unsupported environment: {environment}")

    root = Path(__file__).resolve().parents[3]
    env_dir = root / "kustomize" / "environments" / environment

    rendered = subprocess.run(
        ["kubectl", "kustomize", str(env_dir)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    replacements = {
        "${IMAGE_URI}": os.environ.get("IMAGE_URI", "example.invalid/promo-postmortem-streamlit"),
        "${IMAGE_TAG}": os.environ.get("IMAGE_TAG", "latest"),
        "${DEV_HOST}": os.environ.get("DEV_HOST", "promo-postmortem.dev.example.com"),
        "${DEV_TLS_SECRET}": os.environ.get("DEV_TLS_SECRET", "promo-postmortem-dev-tls"),
        "${PROD_HOST}": os.environ.get("PROD_HOST", "promo-postmortem.example.com"),
        "${PROD_TLS_SECRET}": os.environ.get("PROD_TLS_SECRET", "promo-postmortem-prd-tls"),
    }

    for placeholder, value in replacements.items():
        rendered = rendered.replace(placeholder, value)

    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
