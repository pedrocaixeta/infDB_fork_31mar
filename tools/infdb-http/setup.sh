#!/bin/bash

# InfDB HTTP File Server Setup Script

set -e

echo "=== InfDB HTTP File Server Setup ==="
echo ""

# Change to script directory
cd "$(dirname "$0")"

# Check if htpasswd exists
if [ ! -f "htpasswd" ]; then
    echo "⚠️  No htpasswd file found!"
    echo ""
    read -p "Create default credentials (username: infdb)? [Y/n] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z $REPLY ]]; then
        read -sp "Enter password: " password
        echo
        read -sp "Confirm password: " password_confirm
        echo
        
        if [ "$password" != "$password_confirm" ]; then
            echo "❌ Passwords don't match!"
            exit 1
        fi
        
        echo "infdb:$(openssl passwd -apr1 "$password")" > htpasswd
        chmod 600 htpasswd
        echo "✅ Created htpasswd file"
    else
        echo "Skipping htpasswd creation"
    fi
else
    echo "✅ htpasswd file exists"
fi

# Check if files directory exists
if [ ! -d "files" ]; then
    echo "📁 Creating files directory..."
    mkdir -p files
    echo "Welcome to InfDB File Server!" > files/README.txt
    echo "✅ Created files directory"
else
    echo "✅ files directory exists"
fi

echo ""
echo "=== Setup Complete! ==="
echo ""
echo "Start the server with:"
echo "  docker compose -f services/infdb-http/compose.yml up -d"
echo ""
echo "Access at:"
echo "  http://localhost:8080"
echo ""
echo "Default credentials (development only):"
echo "  Username: infdb"
echo "  Password: (the one you set)"
echo ""
