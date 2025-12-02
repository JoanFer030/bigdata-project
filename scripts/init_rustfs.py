#!/usr/bin/env python3
"""
Script para inicializar el bucket en RustFS usando boto3
"""

import boto3
import os
import sys
from botocore.client import Config
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv('.env')

# Configuración
# IMPORTANTE: Desde el host usamos localhost, no el nombre del contenedor
RUSTFS_ENDPOINT = 'localhost'  # Cambiado de RUSTFS_HOST para conexión desde host
RUSTFS_PORT = os.getenv('RUSTFS_PORT', '9000')
RUSTFS_USER = os.getenv('RUSTFS_USER', 'admin')
RUSTFS_PASSWORD = os.getenv('RUSTFS_PASSWORD', 'muceim-duckduck.2025!')
RUSTFS_BUCKET = os.getenv('RUSTFS_BUCKET', 'mitma')

ENDPOINT_URL = f"http://{RUSTFS_ENDPOINT}:{RUSTFS_PORT}"

print("=" * 60)
print("  Inicialización de RustFS con boto3")
print("=" * 60)
print()
print(f"📋 Configuración:")
print(f"   Endpoint: {ENDPOINT_URL}")
print(f"   User:     {RUSTFS_USER}")
print(f"   Bucket:   {RUSTFS_BUCKET}")
print()

try:
    # Crear cliente S3
    print("🔧 Conectando a RustFS...")
    s3_client = boto3.client(
        's3',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=RUSTFS_USER,
        aws_secret_access_key=RUSTFS_PASSWORD,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    # Listar buckets existentes
    print("🔍 Listando buckets existentes...")
    response = s3_client.list_buckets()
    existing_buckets = [bucket['Name'] for bucket in response['Buckets']]
    print(f"   Buckets: {existing_buckets if existing_buckets else 'Ninguno'}")
    print()
    
    # Verificar si el bucket ya existe
    if RUSTFS_BUCKET in existing_buckets:
        print(f"✅ El bucket '{RUSTFS_BUCKET}' ya existe")
    else:
        print(f"📦 Creando bucket '{RUSTFS_BUCKET}'...")
        s3_client.create_bucket(Bucket=RUSTFS_BUCKET)
        print(f"✅ Bucket '{RUSTFS_BUCKET}' creado exitosamente")
    
    print()
    print("=" * 60)
    print("  ✅ Inicialización completada")
    print("=" * 60)
    print()
    print("📌 Información importante:")
    print()
    print(f"   🌐 Web UI:  http://{RUSTFS_ENDPOINT}:9001")
    print(f"   🔗 API:     {ENDPOINT_URL}")
    print(f"   👤 User:    {RUSTFS_USER}")
    print(f"   🔑 Pass:    {RUSTFS_PASSWORD}")
    print()
    print(f"   📦 Bucket:  {RUSTFS_BUCKET}")
    print(f"   📁 Path:    s3://{RUSTFS_BUCKET}/")
    print()
    print("=" * 60)
    
    sys.exit(0)
    
except Exception as e:
    print()
    print(f"❌ Error: {str(e)}")
    print()
    print("💡 Posibles causas:")
    print("   1. RustFS no está corriendo (docker ps | grep rustfs)")
    print("   2. Credenciales incorrectas en .env")
    print("   3. Puerto incorrecto")
    print()
    print("🔧 Intenta:")
    print("   docker-compose up -d rustfs")
    print("   docker logs rustfs")
    print()
    sys.exit(1)
