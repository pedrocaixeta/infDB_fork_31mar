# Guidelines

This document outlines coding guidelines and best practices for the InfDB project, focusing on creating maintainable, efficient, and high-quality code for energy infrastructure digital twins. These guidelines are designed to help both new and existing developers contribute effectively to the project.

## Purpose

The purpose of these coding guidelines is to establish a consistent, maintainable, and high-quality codebase for the InfDB project. By following these guidelines, we aim to:

- **Ensure Code Consistency**: Establish uniform coding practices across the project to make the codebase more readable and maintainable.
- **Improve Collaboration**: Enable developers to work together more effectively by following shared conventions and practices.
- **Reduce Technical Debt**: Prevent the accumulation of technical debt by enforcing best practices from the start.
- **Enhance Code Quality**: Produce robust, efficient, and secure code that meets the specific needs of energy infrastructure digital twins.
- **Facilitate Onboarding**: Help new developers quickly understand the project structure and coding expectations.
- **Support Domain-Specific Requirements**: Address the unique challenges of energy system modeling, time-series data handling, and geospatial analysis.

These guidelines are not meant to be restrictive but rather to provide a framework that promotes code quality while allowing for innovation and creativity in solving complex energy domain problems.

<!-- ## Table of Contents

1. [Introduction](#1-introduction)
2. [Technology Stack](#2-technology-stack)
3. [Project Structure](#3-project-structure)
4. [Coding Standards](#4-coding-standards)
5. [Development Workflow](#5-development-workflow)
6. [Testing Strategy](#6-testing-strategy)
7. [Documentation Requirements](#7-documentation-requirements)
8. [Performance Considerations](#8-performance-considerations)
9. [Security Guidelines](#9-security-guidelines)
10. [Deployment Process](#10-deployment-process)
11. [Database-Specific Guidelines](#11-database-specific-guidelines)
12. [Contribution and Code Review Process](#12-contribution-and-code-review-process)
13. [Version Control and Branching Strategy](#13-version-control-and-branching-strategy)
14. [Error Handling and Logging Standards](#14-error-handling-and-logging-standards)
15. [Dependency Management](#15-dependency-management)
16. [API Design and Integration](#16-api-design-and-integration)
17. [CI/CD Practices](#17-cicd-practices)
18. [Monitoring and Observability](#18-monitoring-and-observability)
19. [Domain-Specific Terminology](#19-domain-specific-terminology) -->

<!-- The project follows a modular architecture with separate top-level directories. -->

## Coding Standards

### General

- Use **Python 3.12+** for development
- Follow **PEP 8** for Python code style 
- Use **4 spaces** for indentation in Python files
- Use **meaningful variable and function names** that describe their purpose
- Keep functions and methods **small and focused** on a single responsibility
- Write **docstrings** for all functions, classes, and modules
- Always include docstrings in Google style

### Python Specific

- Use **type hints** for all function parameters and return values
- Use **SQLModel** for data validation and database interactions
- Follow the **dependency injection** pattern using FastAPI's dependency system
- Use **async/await** for I/O-bound operations
- Implement proper **error handling** with custom exception classes
- Separate **business logic** from **API handlers**

### Automated Formatting
- Use `ruff format` for automatic code format testing:
  ```bash
  pip install uv
  ruff check .
  ```
- Use `ruff check` for linting:
  ```bash
  pip install uv
  ruff format --check .
  ```

### Folders & Files

- Use `snake_case` when creating them if they are too long

### Variables

#### Rules
- **Naming:** Use `snake_case` for variables; use **`UPPER_SNAKE_CASE`** for constants.
- **Placement:** Define constants at the top of the module.
- **Clarity:** Prefer descriptive names (e.g., `min_rebuild_gap_seconds` over `gap`).
- **Types:** Add type annotations when they improve clarity or tooling.
- **Booleans:** Name so they read naturally (e.g., `schema_changed`, `enough_time_elapsed`).
- **Environment values:** Centralize reads of environment variables; never hardcode secrets.

##### Example
```python
import os
import pathlib

OUTPUT_PATH = pathlib.Path("out.yml")
FALLBACK_EPSG = 25832

min_rebuild_gap_seconds: float = 3.0
schema_changed: bool = False
port: int = int(os.getenv("PORT", "5432") or "5432")
```



### Functions

#### Rules
- **Single responsibility:** Keep functions small and single-purpose.
- **Typing:** Annotate parameters and return types (e.g., `Optional[str]`, `int`).
- **Docstrings:** Provide a one-line summary, then `Args` / `Returns` / `Raises` (Google style).
- **Validation (optional):** Validate inputs early and fail fast on misuse.

##### Example
```python
from __future__ import annotations

import os
import sys
from typing import Optional


def env(name: str, default: Optional[str] = None, *, required: bool = False) -> Optional[str]:
    """Read an environment variable with default/required semantics.

    Args:
        name: Variable name.
        default: Fallback value if the variable is unset.
        required: If True, exit the program when the variable is missing or empty.

    Returns:
        The environment variable's value, or `default` if not set.

    Raises:
        SystemExit: If `required` is True and the variable is missing or empty.
    """
    val = os.getenv(name, default)
    if required and (val is None or val == ""):
        print(f"[ERR] missing required env: {name}", file=sys.stderr)
        sys.exit(2)
    return val
```



### Type Annotations
- Use type hints for all function parameters and return values:
  ```python
  def get_raster_center(building_id: int, resolution: int) -> dict:
      # Function implementation
  ```
- Use `Optional[Type]` for parameters that can be None.
- Use `Union[Type1, Type2]` for parameters that can be multiple types.
- Use `List[Type]`, `Dict[KeyType, ValueType]`, etc. for container types.

### Database Specific

- Follow **database normalization principles** (up to 3NF in most cases)
- Use **appropriate data types** for columns
- Implement **proper indexing strategies** based on query patterns
- Use **foreign key constraints** to maintain referential integrity
- Implement **check constraints** to enforce business rules
- Use **transactions** for operations that must be atomic
- Write **efficient queries** that minimize resource usage
- Document **database schema changes** thoroughly

<!-- ## 5. Development Workflow

1. **Set up the environment** following the installation instructions in [Usage -> Get Software](usage/get-software.md)
2. **Open an issue** to discuss new features, bugs, or changes
3. **Create a new branch** for each feature or bug fix based on an issue
4. **Implement the changes** following the coding guidelines
5. **Write tests** for new functionality or bug fixes
6. **Run tests** to ensure the code works as expected
7. **Create a merge request** to integrate your changes
8. **Address review comments** and update your code as needed
9. **Merge the changes** after approval -->

<!--  -->

## Documentation Requirements

- Maintain **up-to-date API documentation** using OpenAPI/Swagger
- Write **clear README files** for each major component
- Document **database schema** and relationships
- Create **entity-relationship diagrams** for the database
- Document **deployment procedures**
- Include **code examples** for common operations
- Update documentation with each significant change

### Docstrings

Use Google-style docstrings for all modules, classes, and functions:

  ```python
  def get_raster_center(building_id: int, resolution: int) -> dict:
      """Retrieves the raster center for a specific building.

      Args:
          building_id: The ID of the building.
          resolution: The resolution in meters.

      Returns:
          A dictionary containing the raster center coordinates.

      Raises:
          HTTPException: If no data is found.
      """
  ```

### Module-Level Docstring
```python
"""Brief module description.

This module demonstrates the Google-style docstring format. It provides
several examples of documenting different types of objects and methods.

Attributes:
    module_level_variable1 (int): Module level variables can be documented here.
    module_level_variable2 (str): An example of an inline attribute docstring.
"""
```

### Class Docstring
```python
class ExampleClass:
    """A summary line for the class.

    Attributes:
        attr1 (str): Description of `attr1`.
        attr2 (int, optional): Description of `attr2`, which is optional.
    """
```

### Code Comments
- Write comments for complex logic or non-obvious implementations.
- Avoid redundant comments that merely repeat what the code does.
- Use TODO comments for planned improvements, with issue numbers when applicable:
  ```python
  # TODO: Optimize query performance (#123)
  ```

### Project Documentation
- Maintain comprehensive API documentation using [Sphinx](https://github.com/sphinx-doc/sphinx).
- Update the `CHANGELOG.md` file for all changes, i.e. one entry per merge request.
- Include usage examples in documentation for complex features.

## Performance Considerations

- Optimize **database queries** to minimize load
- Implement **pagination** for large data sets
- Use **async/await** for I/O-bound operations
- Monitor and optimize **database indexes**
- Implement **connection pooling** for database connections
- Consider **partitioning strategies** for large tables
- Optimize **geospatial queries** using appropriate PostGIS functions
- Use **TimescaleDB hypertables** for efficient time-series data storage and querying

### Database Optimization
- Use appropriate indexes for frequently queried fields
- Optimize SQL queries for performance
- Use database connection pooling
- Implement caching for frequently accessed data
- Monitor query performance and set up alerts for slow queries
- Use EXPLAIN ANALYZE to identify performance bottlenecks

### API Optimization
- Use async/await for I/O-bound operations
- Implement pagination for endpoints that return large datasets
- Use appropriate HTTP status codes and headers
- Implement rate limiting for public APIs
- Use connection pooling for external API calls

### Resource Management
- Close database connections and file handles properly
- Use context managers for resource cleanup
- Monitor memory usage and optimize memory-intensive operations
- Implement proper connection pooling for database access

## Security Guidelines

- **Never store sensitive information** in code repositories
- Use **environment variables** for configuration
- Implement **proper authentication and authorization**
- Validate **all user inputs** using SQLModel
- Protect against **common web vulnerabilities** (XSS, CSRF, SQL injection)
- Use **HTTPS** for all communications
- Implement **rate limiting** to prevent abuse
- Regularly **update dependencies** to address security vulnerabilities
- Apply **principle of least privilege** for database access
- Implement **data encryption** for sensitive information

### Input Validation
- Validate all user inputs using Pydantic models (within SQLModel).
- Implement strict type checking for API parameters.
- Use parameterized queries to prevent SQL injection.

### Authentication and Authorization
- Implement API key authentication as specified in requirement #21.
- Validate API keys for all protected endpoints.
- Implement role-based access control for different user types.

### Data Protection
- Encrypt sensitive data in transit and at rest.
- Implement proper error handling to avoid leaking sensitive information.
- Follow the principle of least privilege for database access.

## Deployment Process

### Development Environment

- Use **Docker Compose** for local development
- Run the application with **hot reloading** enabled
- Use **development-specific configuration**
- Use **test data** for development

### Production Environment

- Deploy using **CI/CD pipeline**
- Run **comprehensive test suite** before deployment
- Use **production-specific configuration**
- Set up **monitoring and alerting**
- Configure **automatic backups**
- Implement **rollback procedures**

## Database-Specific Guidelines

### PostgreSQL

- Use **appropriate data types** for columns
- Implement **proper indexing strategies** based on query patterns
- Use **foreign key constraints** to maintain referential integrity
- Implement **check constraints** to enforce business rules
- Use **transactions** for operations that must be atomic
- Write **efficient queries** that minimize resource usage

### Data Import/Export

- Implement standardized procedures for data import and export
- Use transaction-safe import processes to prevent partial imports
- Validate imported data against schema constraints before committing
- Provide clear error reporting for failed imports
- Support both bulk and incremental data imports
- Implement data export in standard formats (CSV, JSON, GeoJSON)
- Document data formats and field mappings for external integrations
- Include metadata with exports (timestamp, version, source)

### TimescaleDB

- Use **hypertables** for time-series data
- Define appropriate **chunk intervals** based on data volume and query patterns
- Implement **retention policies** for historical data
- Use **continuous aggregates** for efficient aggregation queries
- Optimize **time-range queries** by including time constraints

### PostGIS

- Use **appropriate spatial reference systems** (SRID) for geospatial data
- Implement **spatial indexes** for efficient geospatial queries
- Use **spatial functions** for geospatial operations
- Optimize **spatial joins** to minimize computational overhead
- Consider **simplifying geometries** for performance when appropriate

### 3DCityDB

- Follow the **3DCityDB schema** for urban modeling
- Use **appropriate LOD (Level of Detail)** for different use cases
- Implement **proper integration** with other database components
- Optimize **3D queries** for performance

## Contribution and Code Review Process

### Pull Request Templates

Standardize PR descriptions by using the provided template.

### Code Review Standards 
  - At least one approval required before merging
  - Code author cannot approve their own PR
  - Automated tests must pass
  - Code style checks must pass

### Merge Criteria
  - All discussions must be resolved
  - CI pipeline must pass
  - Documentation and CHANGELOG.md must be updated

### Review Checklist
- Code follows style guidelines.
- Tests are included and pass.
- Documentation is updated.
- No security vulnerabilities are introduced.
- Performance implications are considered.
- Error handling is appropriate.

### Contribution Workflow Example

1. **Issue Creation**:
    ```
    Title: Add support for importing weather data from CSV files

    Description:
    Currently, the system only supports importing weather data via the API.
    We need to add support for importing data from CSV files to facilitate
    bulk data loading from existing datasets.

    Acceptance Criteria:
    - Support CSV files with standard format (columns: timestamp, raster_id, sensor_name, value)
    - Validate data before import
    - Handle errors gracefully with clear error messages
    - Add documentation for the new import feature
    ```

2. **Branch Creation**:
    ```bash
    git checkout develop
    git pull
    git checkout -b feature/123-csv-weather-import
    ```

3. **Implementation and Testing**:
    - Implement the feature following the coding guidelines
    - Write unit and integration tests
    - Update documentation

4. **Merge Request**:
   ```
   Title: Add CSV import support for weather data (#123)

   Description:
   This MR adds support for importing weather data from CSV files.

   Changes:
   - Add new endpoint `/weather/import/csv`
   - Implement CSV validation and parsing
   - Add error handling for malformed CSV files
   - Update documentation with usage examples

   Resolves #123
   ```

5. **Code Review Process**:
    - Reviewer provides feedback
    - Developer addresses feedback
    - Reviewer approves changes

6. **Merge and Deployment**:
    - Merge to develop branch
    - Deploy to staging environment
    - Verify functionality
    - Close the issue

## Version Control and Branching Strategy

### Branch Strategy

Follow the GitFlow branching model:

  - `main`: Production-ready code
  - `develop`: Integration branch for features
  - Feature branches: `feature/<issue-number>-<description>`
  - Hotfix branches: `hotfix/<issue-number>-<description>`
  - Release branches: `release/v<version>`

### Commit Messages
- Write clear, descriptive commit messages.
- Use the imperative mood ("Add feature" not "Added feature").
- Reference issue numbers in commit messages.
- Keep commits focused on a single change.

Example
  ```
  [Component] Short description (50 chars max)

  Detailed explanation if necessary. Wrap at 72 characters.
  Include motivation for change and contrast with previous behavior.

  Refs #123
  ```

### Pull Requests
- Use the pull request template.
- Create descriptive pull request titles and descriptions.
- Link pull requests to issues.
- Request reviews from appropriate team members.
- Address all review comments before merging.

### Version Tagging:
  - Follow [Semantic Versioning](https://semver.org/)
  - Tag all releases in Git
  - Include release notes with each tag

## Error Handling and Logging Standards

### Error Hierarchy:
  - Define a custom exception hierarchy for different error types
  - Use specific exception types for different error scenarios

### Custom Exception Examples
```python
# Base exception for all application-specific errors
class InfDBError(Exception):
    """Base exception for all InfDB-specific errors."""
    def __init__(self, message, details=None):
        super().__init__(message)
        self.details = details

# Domain-specific exceptions
class DatabaseConnectionError(InfDBError):
    """Raised when a database connection cannot be established."""
    pass

class GeospatialError(InfDBError):
    """Base exception for geospatial-related errors."""
    pass

class InvalidCoordinateError(GeospatialError):
    """Raised when invalid coordinates are provided."""
    pass

# Usage example
try:
    # Some operation that might fail
    result = repository.get_raster_center(building_id, resolution)
    if not result:
        raise InvalidCoordinateError(
            f"No raster found for building {building_id} at resolution {resolution}",
            details={"building_id": building_id, "resolution": resolution}
        )
except InvalidCoordinateError as e:
    # Handle the specific error
    logger.error(f"Coordinate error: {str(e)}", extra={"details": e.details})
    raise HTTPException(status_code=404, detail=str(e))
except DatabaseConnectionError as e:
    # Handle database connection errors
    logger.critical(f"Database connection failed: {str(e)}")
    raise HTTPException(status_code=503, detail="Service temporarily unavailable")
except InfDBError as e:
    # Handle any other application-specific errors
    logger.error(f"Application error: {str(e)}")
    raise HTTPException(status_code=500, detail=str(e))
except Exception as e:
    # Handle unexpected errors
    logger.exception(f"Unexpected error: {str(e)}")
    raise HTTPException(status_code=500, detail="An unexpected error occurred")
```

### Error Messages
- Provide clear, actionable error messages.
- Include relevant context in error messages.
- Use structured error responses for API endpoints:
  ```python
  {
      "error": "Invalid weather parameter",
      "details": "Parameter 'temperature' is not available for the specified date range."
  }
  ```

### Logging
- Use the Python `logging` module for all logging
- Configure appropriate log levels
- Include relevant context in log messages (user ID, request ID, etc.)
- Structure logs for easy parsing and analysis
- Avoid logging sensitive information

### Logging Levels:
  - DEBUG: Detailed information for debugging
  - INFO: Confirmation that things are working as expected
  - WARNING: Indication that something unexpected happened
  - ERROR: Due to a more serious problem, the software couldn't perform some function
  - CRITICAL: A serious error indicating the program may be unable to continue running

### Log Format:
  ```
  {timestamp} [{level}] {module}: {message} {context}
  ```

## Dependency Management

- **Dependency Documentation**:
    - Document all dependencies in `pyproject.toml` with pinned versions
    - Include comments explaining why each dependency is needed

- **Dependency Updates**:
    - Schedule regular dependency updates (monthly)
    - Test thoroughly after updates
    - Document any breaking changes

- **Dependency Approval Process**:
    - New dependencies must be approved by the team
    - Consider security, maintenance status, and license compatibility

<!-- ## 16. API Design and Integration

- **API Design Principles**:
  - Follow RESTful principles
  - Use consistent naming conventions
  - Implement proper status codes
  - Include comprehensive error responses

- **API Documentation**:
  - Document all endpoints using OpenAPI/Swagger
  - Include request/response examples
  - Document authentication requirements

- **API Versioning**:
  - Version all APIs when necessary
  - Document deprecation policies
  - Maintain backward compatibility when possible
  - Use semantic versioning for API versions (v1, v2, etc.)
  - Implement versioning in the URL path (e.g., `/api/v1/resource`)
  - Provide clear migration guides when introducing breaking changes
  - Set appropriate deprecation timelines (minimum 6 months for major endpoints)
  - Use HTTP headers to indicate deprecation status and sunset dates -->

## CI/CD Practices

### Continuous Integration
- Run tests automatically on all branches.
- Enforce code quality checks in CI pipeline.
- Generate test coverage reports.

### Continuous Deployment
- Automate deployment to staging and production environments.
- Implement blue-green deployments for zero-downtime updates.
- Include smoke tests after deployment.

### Pipeline Configuration
- Configure GitLab CI pipeline in `.gitlab-ci.yml`:
  ```yaml
  stages:
    - install
    - lint
    - test
    - build
    - deploy

  install_dependencies:
    stage: install
    script:
      - python -m venv venv
      - source venv/bin/activate
      - pip install -r requirements.txt
  ```