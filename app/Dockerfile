# Base: official Ray image. It matches the cluster's Ray version exactly (head, workers and
# driver must agree), ships the tools KubeRay's health probes exec (wget), and runs as the
# non-root "ray" user (uid 1000), which the restricted Pod Security Standard requires.
# Multi-arch (amd64/arm64) so Karpenter can pick Graviton capacity. Pin by digest in prod.
ARG RAY_VERSION=2.59.0
FROM rayproject/ray:${RAY_VERSION}-py312-cpu

WORKDIR /home/ray/app
COPY --chown=1000:100 requirements.txt .
# ray / pandas / pyarrow come from the base image and are not reinstalled (version skew risk).
RUN pip install --no-cache-dir -r requirements.txt

# Security floor for packages inherited from the base image: Trivy (CI gate) found fixable
# CRITICAL CVEs in PyJWT, anyio and certifi. Raise them here rather than ignore them; drop the
# floors once the Ray base image ships fixed versions. The conda package cache is not used at
# runtime and holds another stale certifi copy.
COPY --chown=1000:100 security-floor.txt .
RUN pip install --no-cache-dir -r security-floor.txt && conda clean -afy >/dev/null

COPY --chown=1000:100 corpreg/ corpreg/
COPY --chown=1000:100 ray_pipeline.py .

ENV PYTHONPATH=/home/ray/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
USER 1000

# No ENTRYPOINT override: KubeRay supplies `ray start ...` for head/worker pods, and the
# RayJob's entrypoint runs `python ray_pipeline.py` on the head as the driver.
