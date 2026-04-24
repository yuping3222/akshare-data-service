"""System tests for Docker integration.

Verifies that the service works correctly when deployed in Docker:
- Dockerfile and docker-compose.yml structure validation (no daemon needed)
- Package import and functionality in containerized environment (requires Docker)

Tests that only parse/validate Dockerfile or docker-compose.yml as text files
run unconditionally. Tests that actually build or run containers are skipped
when Docker is not available.
"""

import re
import subprocess
from pathlib import Path

import pytest
import yaml


def docker_available() -> bool:
    """Check if Docker is installed and running."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


requires_docker = pytest.mark.skipif(
    not docker_available(),
    reason="Docker not available",
)


@pytest.fixture
def dockerfile_path(project_root: Path) -> Path:
    """Path to the project Dockerfile."""
    return project_root / "Dockerfile"


@pytest.fixture
def compose_path(project_root: Path) -> Path:
    """Path to the project docker-compose.yml."""
    return project_root / "docker-compose.yml"


@pytest.fixture
def dockerfile_content(dockerfile_path: Path) -> str:
    """Dockerfile text content."""
    return dockerfile_path.read_text()


@pytest.fixture
def compose_content(compose_path: Path) -> str:
    """docker-compose.yml text content."""
    return compose_path.read_text()


@pytest.fixture
def compose_yaml(compose_path: Path) -> dict:
    """Parsed docker-compose.yml as a dict."""
    return yaml.safe_load(compose_path.read_text())


# ---------------------------------------------------------------------------
# File-existence tests — no Docker daemon needed, just check files on disk
# ---------------------------------------------------------------------------


@pytest.mark.system
class TestDockerfileStructure:
    """Static validation of Dockerfile contents (no daemon needed)."""

    def test_dockerfile_exists(self, dockerfile_path: Path) -> None:
        """Dockerfile exists at project root."""
        assert dockerfile_path.exists(), "Dockerfile should exist at project root"

    def test_dockerfile_uses_python311(self, dockerfile_content: str) -> None:
        """Dockerfile uses Python 3.12 base image (upgraded from 3.11 in P2-2)."""
        assert re.search(r"python:3\.1[12]", dockerfile_content), (
            "Dockerfile should use a python:3.11 or 3.12 base image"
        )

    def test_dockerfile_installs_package(self, dockerfile_content: str) -> None:
        """Dockerfile installs the package via pip."""
        assert "pip install" in dockerfile_content, (
            "Dockerfile should install the package via pip"
        )

    def test_dockerfile_sets_cache_dir(self, dockerfile_content: str) -> None:
        """Dockerfile configures AKSHARE_DATA_CACHE_DIR."""
        assert "AKSHARE_DATA_CACHE_DIR" in dockerfile_content, (
            "Dockerfile should set AKSHARE_DATA_CACHE_DIR env var"
        )

    def test_dockerfile_copies_source(self, dockerfile_content: str) -> None:
        """Dockerfile copies source and config directories."""
        assert re.search(r"COPY\s+src/", dockerfile_content), (
            "Dockerfile should COPY src/ directory"
        )
        assert re.search(r"COPY\s+config/", dockerfile_content), (
            "Dockerfile should COPY config/ directory"
        )


@pytest.mark.system
class TestDockerComposeStructure:
    """Static validation of docker-compose.yml contents (no daemon needed)."""

    def test_docker_compose_exists(self, compose_path: Path) -> None:
        """docker-compose.yml exists at project root."""
        assert compose_path.exists(), "docker-compose.yml should exist"

    def test_docker_compose_has_healthcheck(self, compose_yaml: dict) -> None:
        """docker-compose.yml includes a healthcheck section."""
        service = compose_yaml["services"]["akshare-data"]
        assert "healthcheck" in service, (
            "docker-compose should define a healthcheck for akshare-data service"
        )

    def test_docker_compose_healthcheck_imports_service(
        self, compose_content: str
    ) -> None:
        """Healthcheck imports akshare_data and initializes service."""
        assert "import akshare_data" in compose_content, (
            "Healthcheck should import akshare_data"
        )
        assert "get_service" in compose_content, "Healthcheck should call get_service()"

    def test_docker_compose_memory_limits(self, compose_yaml: dict) -> None:
        """docker-compose.yml defines memory limits."""
        service = compose_yaml["services"]["akshare-data"]
        limits = service["deploy"]["resources"]["limits"]
        assert "memory" in limits, "docker-compose should define memory limits"

    def test_docker_compose_volume_persistence(self, compose_yaml: dict) -> None:
        """docker-compose.yml defines named volumes for cache persistence."""
        service = compose_yaml["services"]["akshare-data"]
        assert "volumes" in service, "docker-compose should define volumes"
        volumes = [v.split(":")[0] if ":" in v else v for v in service["volumes"]]
        assert any("cache" in str(v) for v in volumes), (
            "docker-compose should mount a cache volume"
        )
        # Also verify top-level volumes declaration
        assert "volumes" in compose_yaml, (
            "docker-compose should declare top-level volumes"
        )


# ---------------------------------------------------------------------------
# Build / run tests — require a live Docker daemon
# ---------------------------------------------------------------------------


@pytest.mark.system
class TestDockerImportTest:
    """Tests that verify the package works in a Docker build."""

    @requires_docker
    def test_build_docker_image(self, project_root: Path) -> None:
        """Docker image builds successfully."""
        result = subprocess.run(
            [
                "docker",
                "build",
                "-t",
                "akshare-data-service:test",
                "-f",
                str(project_root / "Dockerfile"),
                str(project_root),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, (
            f"Docker build failed:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )

    @requires_docker
    def test_import_akshare_data_in_container(self, project_root: Path) -> None:
        """akshare_data can be imported in Docker container."""
        subprocess.run(
            [
                "docker",
                "build",
                "-t",
                "akshare-data-service:test",
                "-f",
                str(project_root / "Dockerfile"),
                str(project_root),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )

        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "akshare-data-service:test",
                "python",
                "-c",
                "import akshare_data; print('Import OK')",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"Import failed in container:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        assert "Import OK" in result.stdout

    @requires_docker
    def test_data_service_init_in_container(self, project_root: Path) -> None:
        """DataService can be initialized in Docker container."""
        subprocess.run(
            [
                "docker",
                "build",
                "-t",
                "akshare-data-service:test",
                "-f",
                str(project_root / "Dockerfile"),
                str(project_root),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )

        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "akshare-data-service:test",
                "python",
                "-c",
                "from akshare_data import DataService; s = DataService(); print('Service OK:', type(s))",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"DataService init failed in container:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        assert "Service OK" in result.stdout

    @requires_docker
    def test_namespace_api_available_in_container(self, project_root: Path) -> None:
        """Namespace API (cn, macro) is available in container."""
        subprocess.run(
            [
                "docker",
                "build",
                "-t",
                "akshare-data-service:test",
                "-f",
                str(project_root / "Dockerfile"),
                str(project_root),
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )

        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "akshare-data-service:test",
                "python",
                "-c",
                (
                    "from akshare_data import DataService; "
                    "s = DataService(); "
                    "assert hasattr(s, 'cn'), 'Missing cn namespace'; "
                    "assert hasattr(s, 'macro'), 'Missing macro namespace'; "
                    "print('Namespaces OK')"
                ),
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"Namespace check failed in container:\nstdout: {result.stdout}\nstderr: {result.stderr}"
        )
        assert "Namespaces OK" in result.stdout


@pytest.mark.system
class TestLocalImportSanity:
    """Sanity checks for local (non-Docker) import and initialization."""

    def test_import_akshare_data(self) -> None:
        """akshare_data package imports without error."""
        import akshare_data

        assert akshare_data is not None

    def test_import_data_service(self) -> None:
        """DataService class is importable."""
        from akshare_data import DataService

        assert DataService is not None

    def test_get_service_module_level(self) -> None:
        """get_service() module-level function works."""
        import akshare_data

        svc = akshare_data.get_service()
        assert svc is not None

    def test_get_daily_module_level(self) -> None:
        """get_daily() module-level function is available."""
        import akshare_data

        assert hasattr(akshare_data, "get_daily")
