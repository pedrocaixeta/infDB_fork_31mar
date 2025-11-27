# Project Documentation

This project uses [Sphinx](https://www.sphinx-doc.org/) for documentation, which is built and published automatically on [Read the Docs](https://readthedocs.org/).

## 🛠️ Setup

To build the documentation locally:

1. Navigate to the `docs` folder:

    ```bash
    cd docs
    ```

2. Install the required dependencies:

    ```bash
    uv 
    ```

## Building Locally

To build the HTML version of the documentation manually:

```bash
make html
```

## To automatically rebuild and serve the documentation with live reloading (suggested to use)
Assuming that you are already in /docs directory:

```bash
sphinx-autobuild source/ source/_build/html --port 9000
```

This will start a local server at http://localhost:9000. I do not suggest 8000 since it is a common port for many services.

## 📖 Documentation Standards

When contributing to the documentation:

1. Use **Markdown** for all internal documentation files, **except** where specific formats like reStructuredText (`.rst`) are required (e.g., in the `source/` folder for Read the Docs).
2. Always include a **clear title and description** at the top of each document.
3. Use **relative links** when referencing other files.
4. Place images in the `img/` directory and reference them using relative paths.
5. Keep documentation in sync with code and implementation changes.
6. Follow the [Google developer documentation style guide](https://developers.google.com/style) for writing standards.

## 📂 Folder Structure

- ``docs/source/`` is used by `Sphinx <https://www.sphinx-doc.org/>`_ to refer to the **source files for documentation**. These are the reStructuredText (``.rst``) or Markdown (``.md``) files that Sphinx processes to generate output formats like HTML or PDF.

For example:
- The official `Sphinx RTD tutorial <https://sphinx-rtd-tutorial.readthedocs.io/en/latest/folders.html>`_ follows this pattern.
- The accompanying repository `simpleble <https://github.com/sglvladi/simpleble>`_ stores its documentation in a ``docs/source/`` directory as well.

Following this convention:
- Keep static assets like images (``img/``) and stylesheets (``css/``) at the same level as ``source/``, enabling shared use across multiple documentation sections.


A quick overview of the documentation directory structure:

- `docs/` – Root directory for all documentation-related files.
  - `architecture/`, `contributing/`, `development/`, `guidelines/`, `operations/` – Top-level documentation or notes, outside of Sphinx source structure.
  - `css/` – Custom CSS files for theming the generated HTML output.
  - `data_formats/` – (Optional) Reference or sample data files used in the documentation.
  - `img/` – Shared images used across different documentation sections.

- `docs/source/` – Main Sphinx source folder containing reStructuredText (`.rst`) files and the configuration.
  - `_build/` – Directory where the generated output (HTML, PDF, etc.) is placed after running Sphinx.
  - `api/` – Developer documentation for API-related modules.
  - `architecture/` – System-level architecture documentation.
  - `changelog/` – Version history and release notes.
  - `usage/` – Usage guides or setup instructions for users.
  - `conf.py` – Sphinx configuration file (themes, extensions, paths, etc.).
  - `index.rst` – Root entry point that defines the structure of the generated docs.

- Other files:
  - `.gitignore` – Specifies ignored files for version control.
  - `Makefile` / `make.bat` – Scripts to build the docs (e.g., `make html`).
  - `readme.md` – Markdown overview with documentation setup instructions.
  - `requirements.txt` – Python dependencies for building the documentation.
