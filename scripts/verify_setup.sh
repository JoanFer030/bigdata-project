#!/bin/bash

# Script de verificación post-instalación para DuckLake
# Verifica que todas las configuraciones estén correctas

set -e

echo "=================================================================="
echo "  🔍 VERIFICACIÓN POST-INSTALACIÓN - DuckLake con Airflow"
echo "=================================================================="
echo ""

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Función para imprimir estado
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
    fi
}

# Función para imprimir warning
print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# Función para imprimir info
print_info() {
    echo "ℹ️  $1"
}

ERRORS=0

# 1. Verificar archivos de configuración
echo "1️⃣  Verificando archivos de configuración..."
echo ""

if [ -f ".env" ]; then
    print_status 0 ".env encontrado"
else
    print_status 1 ".env NO encontrado"
    ERRORS=$((ERRORS + 1))
fi

if [ -f "docker-compose.yml" ]; then
    print_status 0 "docker-compose.yml encontrado"
else
    print_status 1 "docker-compose.yml NO encontrado"
    ERRORS=$((ERRORS + 1))
fi

if [ -f "airflow/Dockerfile" ]; then
    print_status 0 "airflow/Dockerfile encontrado"
    
    # Verificar que contenga las extensiones de DuckDB
    if grep -q "INSTALL ducklake" "airflow/Dockerfile"; then
        print_status 0 "  └─ Extensiones DuckDB configuradas en Dockerfile"
    else
        print_status 1 "  └─ Extensiones DuckDB NO configuradas en Dockerfile"
        print_warning "     Añade: RUN python -c \"import duckdb; con = duckdb.connect(); con.execute('INSTALL ducklake; INSTALL postgres; INSTALL httpfs;'); con.close()\""
        ERRORS=$((ERRORS + 1))
    fi
else
    print_status 1 "airflow/Dockerfile NO encontrado"
    ERRORS=$((ERRORS + 1))
fi

if [ -f "airflow/requirements.txt" ]; then
    print_status 0 "airflow/requirements.txt encontrado"
    
    # Verificar providers
    if grep -q "apache-airflow-providers-postgres" "airflow/requirements.txt"; then
        print_status 0 "  └─ Provider PostgreSQL configurado"
    else
        print_status 1 "  └─ Provider PostgreSQL NO configurado"
        ERRORS=$((ERRORS + 1))
    fi
    
    if grep -q "apache-airflow-providers-amazon" "airflow/requirements.txt"; then
        print_status 0 "  └─ Provider AWS/S3 configurado"
    else
        print_status 1 "  └─ Provider AWS/S3 NO configurado"
        ERRORS=$((ERRORS + 1))
    fi
else
    print_status 1 "airflow/requirements.txt NO encontrado"
    ERRORS=$((ERRORS + 1))
fi

if [ -f "airflow/airflow_settings.yaml" ]; then
    print_status 0 "airflow/airflow_settings.yaml encontrado"
    
    # Verificar conexiones
    if grep -q "postgres_datos_externos" "airflow/airflow_settings.yaml"; then
        print_status 0 "  └─ Conexión PostgreSQL configurada"
    else
        print_status 1 "  └─ Conexión PostgreSQL NO configurada"
        ERRORS=$((ERRORS + 1))
    fi
    
    if grep -q "rustfs_s3_conn" "airflow/airflow_settings.yaml"; then
        print_status 0 "  └─ Conexión RustFS configurada"
    else
        print_status 1 "  └─ Conexión RustFS NO configurada"
        ERRORS=$((ERRORS + 1))
    fi
else
    print_status 1 "airflow/airflow_settings.yaml NO encontrado"
    ERRORS=$((ERRORS + 1))
fi

if [ -f "airflow/dags/utils.py" ]; then
    print_status 0 "airflow/dags/utils.py encontrado"
    
    # Verificar función
    if grep -q "connect_datalake_from_airflow" "airflow/dags/utils.py"; then
        print_status 0 "  └─ Función connect_datalake_from_airflow() definida"
    else
        print_status 1 "  └─ Función connect_datalake_from_airflow() NO definida"
        ERRORS=$((ERRORS + 1))
    fi
else
    print_status 1 "airflow/dags/utils.py NO encontrado"
    ERRORS=$((ERRORS + 1))
fi

echo ""

# 2. Verificar contenedores Docker
echo "2️⃣  Verificando contenedores Docker..."
echo ""

if docker ps | grep -q "postgresql"; then
    print_status 0 "PostgreSQL está corriendo"
    
    # Verificar conectividad
    if docker exec postgresql pg_isready -U admin > /dev/null 2>&1; then
        print_status 0 "  └─ PostgreSQL acepta conexiones"
    else
        print_status 1 "  └─ PostgreSQL NO acepta conexiones"
        ERRORS=$((ERRORS + 1))
    fi
else
    print_status 1 "PostgreSQL NO está corriendo"
    print_info "  └─ Ejecuta: docker-compose up -d"
    ERRORS=$((ERRORS + 1))
fi

if docker ps | grep -q "rustfs"; then
    print_status 0 "RustFS está corriendo"
    
    # Verificar conectividad
    if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        print_status 0 "  └─ RustFS acepta conexiones"
    else
        print_warning "  └─ RustFS puede estar inicializándose..."
    fi
else
    print_status 1 "RustFS NO está corriendo"
    print_info "  └─ Ejecuta: docker-compose up -d"
    ERRORS=$((ERRORS + 1))
fi

# Buscar contenedor de Airflow scheduler
AIRFLOW_SCHEDULER=$(docker ps --filter "name=scheduler" --format "{{.Names}}" 2>/dev/null | head -1)

if [ -n "$AIRFLOW_SCHEDULER" ]; then
    print_status 0 "Airflow Scheduler está corriendo ($AIRFLOW_SCHEDULER)"
else
    print_status 1 "Airflow Scheduler NO está corriendo"
    print_info "  └─ Ejecuta: cd airflow && astro dev start"
    ERRORS=$((ERRORS + 1))
fi

echo ""

# 3. Verificar red Docker
echo "3️⃣  Verificando red Docker..."
echo ""

if docker network inspect airflow_9558a3_airflow > /dev/null 2>&1; then
    print_status 0 "Red 'airflow_9558a3_airflow' existe"
    
    # Verificar que PostgreSQL está en la red
    if docker network inspect airflow_9558a3_airflow 2>/dev/null | grep -q "postgresql"; then
        print_status 0 "  └─ PostgreSQL en la red"
    else
        print_status 1 "  └─ PostgreSQL NO está en la red"
        ERRORS=$((ERRORS + 1))
    fi
    
    # Verificar que RustFS está en la red
    if docker network inspect airflow_9558a3_airflow 2>/dev/null | grep -q "rustfs"; then
        print_status 0 "  └─ RustFS en la red"
    else
        print_status 1 "  └─ RustFS NO está en la red"
        ERRORS=$((ERRORS + 1))
    fi
else
    print_status 1 "Red 'airflow_9558a3_airflow' NO existe"
    print_info "  └─ La red se crea automáticamente al iniciar Airflow"
    ERRORS=$((ERRORS + 1))
fi

echo ""

# 4. Verificar bucket en RustFS
echo "4️⃣  Verificando bucket en RustFS..."
echo ""

if docker ps | grep -q "rustfs"; then
    # Usar Python para verificar el bucket (en vez de mc que no está instalado)
    BUCKET_CHECK=$(python3 -c "
import boto3
from botocore.client import Config
try:
    s3 = boto3.client('s3', 
        endpoint_url='http://localhost:9000',
        aws_access_key_id='admin',
        aws_secret_access_key='muceim-duckduck.2025!',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1')
    buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if 'mitma' in buckets:
        print('OK')
    else:
        print('NOT_FOUND')
except Exception as e:
    print(f'ERROR:{e}')
" 2>&1)
    
    if [ "$BUCKET_CHECK" = "OK" ]; then
        print_status 0 "Bucket 'mitma' existe en RustFS"
    else
        print_status 1 "Bucket 'mitma' NO existe en RustFS"
        print_info "  └─ Detalle: $BUCKET_CHECK"
        print_info "  └─ Ejecuta: python3 scripts/init_rustfs.py"
        ERRORS=$((ERRORS + 1))
    fi
fi

echo ""

# 5. Verificar base de datos PostgreSQL
echo "5️⃣  Verificando base de datos PostgreSQL..."
echo ""

if docker ps | grep -q "postgresql"; then
    if docker exec postgresql psql -U admin -d mitma -c "SELECT 1" > /dev/null 2>&1; then
        print_status 0 "Base de datos 'mitma' existe y es accesible"
    else
        print_status 1 "Base de datos 'mitma' NO es accesible"
        print_info "  └─ Verifica las credenciales en .env"
        ERRORS=$((ERRORS + 1))
    fi
fi

echo ""

# 6. Resumen
echo "=================================================================="
echo "  📊 RESUMEN DE VERIFICACIÓN"
echo "=================================================================="
echo ""

if [ $ERRORS -eq 0 ]; then
    echo -e "${GREEN}✅ Todas las verificaciones pasaron correctamente${NC}"
    echo ""
    echo "🚀 Próximos pasos:"
    echo ""
    echo "   1. Reconstruir Airflow (si no lo has hecho):"
    echo "      cd airflow && astro dev stop && astro dev start --build"
    echo ""
    echo "   2. Acceder a Airflow UI:"
    echo "      http://localhost:8080 (admin/admin)"
    echo ""
    echo "   3. Ejecutar DAG de prueba:"
    echo "      test_ducklake_connections"
    echo ""
    echo "   4. Verificar logs:"
    echo "      cd airflow && astro dev logs --follow"
    echo ""
else
    echo -e "${RED}❌ Se encontraron $ERRORS error(es)${NC}"
    echo ""
    echo "📋 Acciones requeridas:"
    echo ""
    echo "   1. Revisa los errores arriba marcados con ❌"
    echo "   2. Consulta la documentación: airflow/CONFIGURACION_DUCKLAKE.md"
    echo "   3. Verifica las variables de entorno en .env"
    echo ""
fi

echo "=================================================================="

exit $ERRORS
