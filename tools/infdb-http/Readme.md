# InfDB HTTP File Server

A lightweight, password-protected HTTP file server for sharing InfDB project files, datasets, and documentation.

## Features

- 🔒 **Password Protection** - HTTP Basic Authentication
- 📂 **Directory Browsing** - Automatic file indexing
- ⚡ **High Performance** - nginx-based
- 🐳 **Docker Ready** - Easy deployment
- 🎨 **Modern UI** - Clean landing page

## Quick Start

### 1. Create Password File

Generate a password for the file server (username: `infdb`):

```bash
# Install htpasswd (if not already installed)
# macOS: brew install httpd
# Linux: sudo apt-get install apache2-utils

# Create password file (username: infdb)
htpasswd -c services/infdb-http/htpasswd infdb
```

You'll be prompted to enter a password twice.

**Default Credentials (for development):**
- Username: `infdb`
- Password: `infdb2025`

### 2. Create Files Directory

```bash
# Create the files directory
mkdir -p services/infdb-http/files

# Add some example files
echo "Welcome to InfDB!" > services/infdb-http/files/README.txt
```

### 3. Start the Server

```bash
# From project root
docker compose -f services/infdb-http/compose.yml up -d

# View logs
docker compose -f services/infdb-http/compose.yml logs -f

# Stop server
docker compose -f services/infdb-http/compose.yml down
```

### 4. Access the Server

Open your browser and navigate to:
- **Landing Page:** http://localhost:8080
- **Files Browser:** http://localhost:8080/files/
- **Health Check:** http://localhost:8080/health

When prompted, enter your username and password.

## Configuration

### Change Port

Edit `compose.yml` to change the exposed port:

```yaml
ports:
  - "9090:80"  # Change 8080 to your preferred port
```

### Add Multiple Users

```bash
# Add additional users (omit -c flag to avoid overwriting)
htpasswd services/infdb-http/htpasswd username2
htpasswd services/infdb-http/htpasswd username3
```

### Custom nginx Configuration

Edit `nginx.conf` to customize:
- File size limits (`client_max_body_size`)
- Directory listing format
- Additional security headers
- URL rewrite rules

## Directory Structure

```
services/infdb-http/
├── compose.yml          # Docker Compose configuration
├── nginx.conf           # nginx server configuration
├── index.html           # Landing page
├── htpasswd             # Password file (create this)
├── files/               # Your files go here (create this)
│   ├── datasets/
│   ├── documentation/
│   └── exports/
└── Readme.md            # This file
```

## File Organization

Organize your files in the `files/` directory:

```bash
files/
├── datasets/
│   ├── buildings.geojson
│   └── heat_demand.csv
├── documentation/
│   ├── api_guide.pdf
│   └── data_dictionary.xlsx
└── exports/
    ├── 2025-01-export.zip
    └── analysis_results.json
```

## Security Considerations

### Production Deployment

For production use:

1. **Use Strong Passwords:**
   ```bash
   htpasswd -c services/infdb-http/htpasswd admin
   # Enter a strong, unique password
   ```

2. **Enable HTTPS:**
   - Add SSL certificates
   - Configure nginx for HTTPS
   - Redirect HTTP to HTTPS

3. **Restrict Network Access:**
   ```yaml
   # In compose.yml, bind to specific interface
   ports:
     - "127.0.0.1:8080:80"  # Only localhost
   ```

4. **Use Environment Variables:**
   - Store sensitive config in `.env` file
   - Never commit `htpasswd` to version control

5. **Regular Updates:**
   ```bash
   docker compose pull  # Update nginx image
   ```

### File Permissions

Ensure proper file permissions:

```bash
# Make htpasswd readable only by owner
chmod 600 services/infdb-http/htpasswd

# Set files directory permissions
chmod 755 services/infdb-http/files
```

## Usage Examples

### Upload Files

```bash
# Copy files to the server
cp your-file.pdf services/infdb-http/files/

# Create subdirectories
mkdir -p services/infdb-http/files/reports/2025
cp report.pdf services/infdb-http/files/reports/2025/
```

### Download Files via curl

```bash
# Download with authentication
curl -u infdb:infdb2025 http://localhost:8080/files/README.txt

# Download and save
curl -u infdb:infdb2025 -O http://localhost:8080/files/dataset.zip
```

### wget with Authentication

```bash
wget --user=infdb --password=infdb2025 http://localhost:8080/files/data.csv
```

## Troubleshooting

### Permission Denied

```bash
# Check file ownership and permissions
ls -la services/infdb-http/htpasswd
chmod 644 services/infdb-http/htpasswd
```

### Port Already in Use

```bash
# Check what's using port 8080
lsof -i :8080

# Change port in compose.yml or stop conflicting service
```

### Authentication Not Working

```bash
# Verify htpasswd file exists and has content
cat services/infdb-http/htpasswd

# Recreate password file
htpasswd -c services/infdb-http/htpasswd infdb
```

### Files Not Showing

```bash
# Verify files directory exists
ls -la services/infdb-http/files/

# Check nginx container logs
docker compose -f services/infdb-http/compose.yml logs
```

## Advanced Configuration

### Custom Headers

Add to `nginx.conf` in the `server` block:

```nginx
add_header X-Content-Type-Options nosniff;
add_header X-Frame-Options DENY;
add_header X-XSS-Protection "1; mode=block";
```

### Rate Limiting

```nginx
http {
    limit_req_zone $binary_remote_addr zone=one:10m rate=10r/s;
    
    server {
        location /files/ {
            limit_req zone=one burst=20;
            # ... rest of config
        }
    }
}
```

### IP Whitelisting

```nginx
location /files/ {
    allow 192.168.1.0/24;
    allow 10.0.0.0/8;
    deny all;
    # ... rest of config
}
```

## Integration with InfDB

### Add to Startup Script

Edit `startup.sh` to include the file server:

```bash
echo "=== Start InfDB File Server ==="
docker compose -f services/infdb-http/compose.yml up -d
```

### Share with Network

```yaml
# In compose.yml, use existing network
networks:
  infdb-network:
    external: true
```

## Monitoring

### Check Server Status

```bash
# Health check
curl http://localhost:8080/health

# View access logs
docker compose -f services/infdb-http/compose.yml logs --tail=100

# Follow logs in real-time
docker compose -f services/infdb-http/compose.yml logs -f
```

### Statistics

```bash
# Container stats
docker stats infdb-http-server

# Disk usage
du -sh services/infdb-http/files/
```

## Backup

```bash
# Backup files directory
tar -czf infdb-files-backup-$(date +%Y%m%d).tar.gz services/infdb-http/files/

# Backup password file
cp services/infdb-http/htpasswd services/infdb-http/htpasswd.backup
```

## Related Documentation

- [nginx Documentation](https://nginx.org/en/docs/)
- [Docker Compose](https://docs.docker.com/compose/)
- [HTTP Basic Authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication)

## Support

For issues or questions:
1. Check nginx logs: `docker compose logs`
2. Verify configuration files
3. Review InfDB project documentation
4. Open an issue in the repository
