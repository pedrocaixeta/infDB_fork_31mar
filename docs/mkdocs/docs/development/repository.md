# Repository Structure

The infDB repository is organized as follows:

```
├── .github/            # GitHub workflows and actions
├── configs/            # Global configuration files
├── docs/               # Documentation (MkDocs source)
├── services/           # Service definitions (Docker contexts)
│   ├── infdb-http/     # HTTP file server
│   └── ...
├── src/                # Project logic
│   └── infdb_package/  # Python package source
├── tools/              # Standalone tools
│   ├── infdb-loader/   # Data loader tool
│   └── ...
├── compose.yml         # Main Docker Compose file
└── infdb-startup.sh    # Initialization script
```