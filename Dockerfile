FROM quay.io/astronomer/astro-runtime:5.0.6-base AS stage1

LABEL maintainer="Astronomer <humans@astronomer.io>"
ARG BUILD_NUMBER=-1
LABEL io.astronomer.docker=true
LABEL io.astronomer.docker.build.number=$BUILD_NUMBER
LABEL io.astronomer.docker.airflow.onbuild=true
# Install Python and OS-Level Packages
COPY packages.txt .
RUN apt-get update && cat packages.txt | xargs apt-get install -y

FROM stage1 AS stage2
# Install Python Packages
ARG PIP_EXTRA_INDEX_URL
ENV PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL}
COPY requirements.txt .
RUN pip install --no-cache-dir -q -r requirements.txt
RUN pip install $PIP_EXTRA_INDEX_URL

FROM stage1 AS stage3
# Copy requirements directory
COPY --from=stage2 /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=stage2 /usr/local/bin /home/astro/.local/bin
ENV PATH="/home/astro/.local/bin:$PATH"

COPY . .