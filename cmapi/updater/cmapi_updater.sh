#!/bin/bash
set -e

VERSION="${CMAPI_VERSION}"
if [[ -z "$VERSION" ]]; then
    echo "[Updater] Error: CMAPI_VERSION is not set"
    exit 1
fi

# Wait for a few seconds to allow endpoint to respond
sleep 5s
echo "[CMAPI Updater] Stopping CMAPI service..."
systemctl stop mariadb-columnstore-cmapi

echo "[CMAPI Updater] Removing existing package..."
if command -v apt >/dev/null; then
    apt remove -y mariadb-columnstore-cmapi
    apt install -y mariadb-columnstore-cmapi=${VERSION}
elif command -v yum >/dev/null; then
    yum remove -y MariaDB-columnstore-cmapi
    yum install -y MariaDB-columnstore-cmapi-${VERSION}
else
    echo "Unsupported package manager"
    exit 1
fi

echo "[CMAPI Updater] Restarting CMAPI service..."
systemctl start mariadb-columnstore-cmapi

echo "[CMAPI Updater] Done."
exit 0