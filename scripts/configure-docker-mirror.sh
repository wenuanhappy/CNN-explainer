#!/bin/bash
# Configure Docker mirror registries for faster image pulls

DOCKER_CONFIG="/var/snap/docker/3505/config/daemon.json"

# Backup existing config
if [ -f "$DOCKER_CONFIG" ]; then
    cp "$DOCKER_CONFIG" "${DOCKER_CONFIG}.backup.$(date +%s)"
fi

# Write new config with mirrors
cat > "$DOCKER_CONFIG" << 'EOF'
{
  "registry-mirrors": [
    "https://docker.mirrors.ustc.edu.cn",
    "https://hub-mirror.c.163.com",
    "https://mirror.baidubce.com"
  ]
}
EOF

echo "Docker daemon config updated. Now restart Docker:"
echo "  snap restart docker"
echo ""
echo "Or if using systemctl:"
echo "  sudo systemctl restart docker"
