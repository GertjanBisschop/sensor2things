# CI/CD Pipeline

The GitLab CI/CD pipeline (`.gitlab-ci.yml`) automates security scanning, code quality enforcement, static analysis, unit testing, and container builds for the project.

---

## GitLab Pipeline

### Stages and Jobs

1. **Security Scanning (Templates)**
   - **SAST & Secret Detection**: Integrated GitLab security templates (`Security/SAST.gitlab-ci.yml` and `Security/Secret-Detection.gitlab-ci.yml`) automatically scan the codebase for security vulnerabilities and leaked credentials.

2. **`linting` Stage**
   - **`linting`**: Executes `uv run ruff check` to ensure Python code style, formatting, and linting rules are upheld.

3. **`static-analysis` Stage**
   - **`static-analysis`**: Executes `uv run pyright` to perform strict static type analysis across the project codebase.

4. **`test` Stage**
   - **`test`**: Runs unit tests via `uv run pytest --junitxml=report.xml` and publishes JUnit XML test reports (`artifacts:reports:junit`) for visualization in the GitLab merge request widget and pipeline reports.

5. **`build` Stage**
   - **`docker-build`**: Builds the multi-stage production container image via Docker-in-Docker (`docker:dind`) from `docker/Dockerfile` and pushes `$CI_REGISTRY_IMAGE:latest` to the GitLab Container Registry when changes are merged to the `master` branch.
   - **`docker-build-branch`**: Builds and pushes `$CI_REGISTRY_IMAGE:$CI_COMMIT_REF_SLUG` for branches other than `master`.

---

## Pipeline Optimizations

- **Conditional Change Rules**: To conserve CI runner resources, the `linting`, `static-analysis`, `test`, and Docker build jobs execute only when relevant source or configuration files change (`src/**/*`, `pyproject.toml`, `uv.lock`, `pytest.ini`).
- **Dependency Caching**: The pipeline uses `ghcr.io/astral-sh/uv:python3.14-trixie` as its default base image and caches the `.uv-cache` directory keyed on `uv.lock` for fast dependency resolution.
