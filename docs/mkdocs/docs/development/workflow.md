# Workflow

### Local development environment for InfDB for developers
```bash
# on linux and macos by installation script
curl -LsSf https://astral.sh/uv/install.sh | sh
# or by pip
pip install uv
```

### Create environment (only once)
```bash
# linux and macos
uv sync
```

### Activate environment
```bash
# linux and macos
source .venv/bin/activate
# windows
venv\Scripts\activate
```
### Clean repo
```bash
git fetch origin
git reset --hard
git clean -fdx
```

### Stop and remove all docker containers and volumes
```bash
# 1. Stop all containers
docker stop $(docker ps -a -q)

# 2. Remove all containers (breaks the link to the volumes)
docker rm $(docker ps -a -q)

# 3. Delete all volumes
docker volume rm $(docker volume ls -q)
```

### Clean docker
```bash
docker system prune -a --volume
```

### Tree with permission
```bash
tree -pug
# -p permissions
# -u user
# -g group
```

## Repository Structure

- **src/**: Main application package
  - **infdb_package/**: Business logic services
  - **main.py**: Application entry point
- **docs/**: Documentation
  - **architecture/**: System architecture documentation
  - **contributing/**: Contribution guidelines and code of conduct
  - **development/**: Developer guides and workflows
  - **guidelines/**: Project guidelines and standards
  - **operations/**: Operational guides and CI/CD documentation
  - **source/**: Source files for documentation
  - **img/**: Images used in documentation
- **dockers/**: Docker configuration files
- **tools/**: External tools and scripts that interact with infDB
  - Individual tool directories with their own configurations
  - **Readme.md**: Detailed documentation for all tools
- **configs/**: Configuration files for infDB initialization
- **tests/**: Test suite
  - **unit/**: Unit tests for individual components
  - **integration/**: Tests for component interactions
  - **e2e/**: End-to-end tests for the application


## Development Workflow

1. **Open an issue** to discuss new features, bugs, or changes.
2. **Create a new branch** for each feature or bug fix based on an issue.
3. **Implement the changes** following the coding guidelines.
4. **Write tests** for new functionality or bug fixes.
5. **Run tests** to ensure the code works as expected.
6. **Create a merge request** to integrate your changes.
7. **Address review comments** and update your code as needed.
8. **Merge the changes** after approval.


## CI/CD Workflow

The CI/CD workflow is set up using GitLab CI/CD. The workflow runs tests, checks code style, and builds the documentation on every push to the repository. You can view workflow results directly in the repository's CI/CD section. For detailed information about the CI/CD workflow, see the [CI/CD Guide](docs/operations/CI_CD_Guide.md).

## Development Resources

The following resources are available to help developers understand and contribute to the project:

### Coding Guidelines

The [Coding Guidelines](docs/guidelines/CODING_GUIDELINES.md) document outlines the coding standards and best practices for the project. Start here when trying to understand the project as a developer.

### Architecture Documentation

The [Architecture Documentation](docs/architecture/index.rst) provides an overview of the system architecture, including the database schema, components, and integration points.

### Developer Guides

- [Development Setup Guide](docs/development/setup.md): Comprehensive instructions for setting up a development environment
- [Contribution Workflow](docs/development/workflow.md): Step-by-step process for contributing to the project
- [API Development Guide](docs/development/api_guide.md): Information for developers who want to use or extend the API
- [Database Schema Documentation](docs/development/database_schema.md): Detailed information about the database schema

### Contribution Guidelines

- [Contributing Guide](docs/contributing/CONTRIBUTING.md): Guidelines for contributing to the project
- [Code of Conduct](docs/contributing/CODE_OF_CONDUCT.md): Community standards and expectations
- [Release Procedure](docs/contributing/RELEASE_PROCEDURE.md): Process for creating new releases

### Operations Documentation

- [CI/CD Guide](docs/operations/CI_CD_Guide.md): Detailed information about the CI/CD workflow

## Contribution and Code Quality

Everyone is invited to develop this repository with good intentions. Please follow the workflow described in the [CONTRIBUTING.md](docs/contributing/CONTRIBUTING.md).

### Coding Standards

This repository follows consistent coding styles. Refer to [CONTRIBUTING.md](docs/contributing/CONTRIBUTING.md) and the [Coding Guidelines](docs/guidelines/CODING_GUIDELINES.md) for detailed standards.

### Pre-commit Hooks

Pre-commit hooks are configured to check code quality before commits, helping enforce standards.

### Changelog

The changelog is maintained in the [CHANGELOG.md](CHANGELOG.md) file. It lists all changes made to the repository. Follow instructions there to document any updates.

