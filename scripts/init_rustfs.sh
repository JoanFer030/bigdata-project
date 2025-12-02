#!/bin/bash

# Script para inicializar RustFS con el bucket necesario para DuckLake
# Este script debe ejecutarse DESPUÉS de que docker-compose esté corriendo

set -e

echo "=============================================="
echo "  Inicialización de RustFS para DuckLake"
echo "=============================================="

# Variables de entorno (deben coincidir con .env)
RUSTFS_CONTAINER="rustfs"
RUSTFS_USER="${RUSTFS_USER:-admin}"
RUSTFS_PASSWORD="${RUSTFS_PASSWORD:-muceim-duckduck.2025!}"
RUSTFS_BUCKET="${RUSTFS_BUCKET:-mitma}"
RUSTFS_ENDPOINT="http://localhost:9000"

echo ""
echo "📋 Configuración:"
echo "   Container: $RUSTFS_CONTAINER"
echo "   Endpoint:  $RUSTFS_ENDPOINT"
echo "   User:      $RUSTFS_USER"
echo "   Bucket:    $RUSTFS_BUCKET"
echo ""

# Verificar que el contenedor está corriendo
echo "🔍 Verificando contenedor RustFS..."
if ! docker ps | grep -q "$RUSTFS_CONTAINER"; then
    echo "❌ Error: Contenedor '$RUSTFS_CONTAINER' no está corriendo"
    echo "   Ejecuta primero: docker-compose up -d"
    exit 1
fi
echo "   ✅ Contenedor corriendo"

# Esperar a que RustFS esté listo
echo ""
echo "⏳ Esperando a que RustFS esté listo..."
for i in {1..30}; do
    if docker exec $RUSTFS_CONTAINER mc --version > /dev/null 2>&1; then
        echo "   ✅ RustFS listo"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "   ❌ Timeout esperando RustFS"
        exit 1
    fi
    echo "   Intento $i/30..."
    sleep 2
done

# Configurar alias de MinIO Client
echo ""
echo "🔧 Configurando MinIO Client..."
docker exec $RUSTFS_CONTAINER mc alias set local http://localhost:9000 "$RUSTFS_USER" "$RUSTFS_PASSWORD" > /dev/null 2>&1
echo "   ✅ Alias configurado"

# Verificar si el bucket ya existe
echo ""
echo "🔍 Verificando bucket '$RUSTFS_BUCKET'..."
if docker exec $RUSTFS_CONTAINER mc ls local/ 2>&1 | grep -q "$RUSTFS_BUCKET"; then
    echo "   ℹ️  Bucket '$RUSTFS_BUCKET' ya existe"
else
    echo "   📦 Creando bucket '$RUSTFS_BUCKET'..."
    docker exec $RUSTFS_CONTAINER mc mb "local/$RUSTFS_BUCKET"
    echo "   ✅ Bucket creado"
fi

# Listar buckets
echo ""
echo "📦 Buckets disponibles:"
docker exec $RUSTFS_CONTAINER mc ls local/

# Configurar política de acceso (opcional, para debugging)
echo ""
echo "🔐 Configurando política de acceso..."
docker exec $RUSTFS_CONTAINER mc anonymous set download "local/$RUSTFS_BUCKET" > /dev/null 2>&1 || true
echo "   ✅ Política configurada"

# Información adicional
echo ""
echo "=============================================="
echo "  ✅ Inicialización completada"
echo "=============================================="
echo ""
echo "📌 Información importante:"
echo ""
echo "   🌐 Web UI:  http://localhost:9001"
echo "   🔗 API:     http://localhost:9000"
echo "   👤 User:    $RUSTFS_USER"
echo "   🔑 Pass:    $RUSTFS_PASSWORD"
echo ""
echo "   📦 Bucket:  $RUSTFS_BUCKET"
echo "   📁 Path:    s3://$RUSTFS_BUCKET/"
echo ""
echo "💡 Comandos útiles:"
echo ""
echo "   # Listar contenido del bucket"
echo "   docker exec $RUSTFS_CONTAINER mc ls local/$RUSTFS_BUCKET/"
echo ""
echo "   # Ver detalles del bucket"
echo "   docker exec $RUSTFS_CONTAINER mc stat local/$RUSTFS_BUCKET/"
echo ""
echo "   # Borrar todo el contenido (¡CUIDADO!)"
echo "   docker exec $RUSTFS_CONTAINER mc rm --recursive --force local/$RUSTFS_BUCKET/"
echo ""
echo "=============================================="
