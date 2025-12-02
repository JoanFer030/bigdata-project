# 🚀 BigData Project - MITMA Data Pipeline

Proyecto de ingesta y procesamiento de datos de movilidad del Ministerio de Transportes, Movilidad y Agenda Urbana (MITMA) utilizando Apache Airflow, DuckDB/DuckLake, PostgreSQL y RustFS.

## 📋 Arquitectura

```
┌─────────────────────────────────────────────────────────────┐
│                   APACHE AIRFLOW                            │
│                   (Orquestación)                            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  DAGs: Bronze Layer (Ingesta de datos MITMA)        │   │
│  │         ↓                                            │   │
│  │  DuckDB + DuckLake Extension                         │   │
│  └──────────┬────────────────────┬──────────────────────┘   │
└─────────────┼────────────────────┼─────────────────────────┘
              ↓                    ↓
    ┌──────────────────┐  ┌──────────────────┐
    │   PostgreSQL     │  │     RustFS       │
    │   (Metadatos)    │  │   (Datos S3)     │
    │                  │  │                  │
    │  - Catálogo      │  │  - Parquet       │
    │  - Esquemas      │  │  - CSV           │
    │  - Tablas        │  │  - Delta         │
    └──────────────────┘  └──────────────────┘
```

## 🎯 Componentes

- **Apache Airflow** (Astronomer Runtime 3.1-5): Orquestación de pipelines
- **DuckDB + DuckLake**: Motor de consultas con arquitectura lake-house
- **PostgreSQL 15**: Catálogo de metadatos
- **RustFS/MinIO**: Object storage compatible con S3
- **Portainer**: Gestión de contenedores

## 🚀 Quick Start

### 1. Iniciar servicios

```bash
# Iniciar PostgreSQL, RustFS y Portainer
docker-compose up -d

# Iniciar Airflow (Astronomer)
cd airflow
astro dev start --build
```

### 2. Inicializar RustFS

```bash
# Crear bucket necesario para DuckLake
./scripts/init_rustfs.sh
```

### 3. Verificar conexiones

Accede a Airflow en http://localhost:8080 y ejecuta el DAG `test_ducklake_connections`

### 4. Acceder a servicios

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **RustFS UI**: http://localhost:9001 (admin/muceim-duckduck.2025!)
- **Portainer**: http://localhost:9443
- **PostgreSQL**: localhost:30432 (admin/muceim-duckduck.2025!)

## 📚 Documentación

- [**Configuración DuckLake**](airflow/CONFIGURACION_DUCKLAKE.md) - Guía completa de configuración
- [**Airflow Settings**](airflow/airflow_settings.yaml) - Conexiones y variables
- [**Test Connections**](airflow/test_connections.py) - Script de verificación

## 🔧 Configuración

### Variables de entorno (.env)

```bash
# PostgreSQL
POSTGRES_USER=admin
POSTGRES_PASSWORD=muceim-duckduck.2025!
POSTGRES_DB=mitma

# RustFS
RUSTFS_USER=admin
RUSTFS_PASSWORD=muceim-duckduck.2025!
RUSTFS_BUCKET=mitma
```

### Conexiones de Airflow

Ya configuradas en `airflow/airflow_settings.yaml`:

- `postgres_datos_externos` - PostgreSQL (catálogo)
- `rustfs_s3_conn` - RustFS (almacenamiento)

## 📦 DAGs Disponibles

- `mitma_viajes_ingest` - Ingesta de datos de viajes origen-destino
- `mitma_pernoctaciones_ingest` - Ingesta de datos de pernoctaciones
- `mitma_personas_ingest` - Ingesta de datos de personas por día
- `test_ducklake_connections` - Verificación de conexiones

## 🛠️ Desarrollo

### Estructura del proyecto

```
bigdata-project/
├── docker-compose.yml          # Servicios: PostgreSQL, RustFS, Portainer
├── .env                        # Variables de entorno
├── scripts/
│   └── init_rustfs.sh         # Inicialización de RustFS
└── airflow/
    ├── Dockerfile              # Imagen personalizada de Airflow
    ├── requirements.txt        # Dependencias Python
    ├── airflow_settings.yaml   # Conexiones y variables
    ├── test_connections.py     # Script de prueba
    ├── CONFIGURACION_DUCKLAKE.md  # Documentación detallada
    └── dags/
        ├── utils.py            # Utilidades comunes
        ├── dag_test_ducklake.py        # DAG de prueba
        ├── dag_bronze_mitma.py         # DAG de ingesta MITMA
        └── bronze/
            └── tasks/          # Tareas de ingesta
```

### Comandos útiles

```bash
# Reconstruir Airflow
cd airflow
astro dev stop
astro dev start --build

# Ver logs
astro dev logs --follow

# Ejecutar tests
astro dev run pytest

# Verificar conexiones
astro dev bash
python test_connections.py
```

## 🐛 Resolución de Problemas

### Error: "Connection not found"

```bash
# Reiniciar Airflow para cargar airflow_settings.yaml
cd airflow
astro dev restart
```

### Error: "Could not connect to PostgreSQL"

```bash
# Verificar que PostgreSQL está corriendo
docker ps | grep postgresql

# Verificar red Docker
docker network inspect airflow_9558a3_airflow
```

### Error: "S3 connection failed"

```bash
# Verificar RustFS
docker ps | grep rustfs

# Reinicializar bucket
./scripts/init_rustfs.sh
```

Ver más detalles en [CONFIGURACION_DUCKLAKE.md](airflow/CONFIGURACION_DUCKLAKE.md)

## 📊 Uso en DAGs

```python
from utils import connect_datalake_from_airflow

@task
def ingest_data():
    # Conectar a DuckLake (PostgreSQL + RustFS)
    con = connect_datalake_from_airflow()
    
    try:
        # Crear tabla (metadatos en PostgreSQL)
        con.execute("""
            CREATE TABLE IF NOT EXISTS bronze_mitma_od (
                fecha TEXT,
                origen TEXT,
                destino TEXT,
                viajes TEXT
            );
        """)
        
        # Insertar datos (archivos en RustFS)
        con.execute("""
            INSERT INTO bronze_mitma_od
            SELECT * FROM read_csv('url_mitma.csv.gz');
        """)
        
    finally:
        con.close()
```

## 📝 TODO

- [ ] Reconstruir contenedor de Airflow
- [ ] Inicializar bucket en RustFS
- [ ] Ejecutar DAG de prueba
- [ ] Actualizar DAGs existentes para usar `connect_datalake_from_airflow()`
- [ ] Implementar capa Silver
- [ ] Implementar capa Gold

## 🤝 Contribución

Este es un proyecto académico para el Máster en Big Data (MUCEIM).

## 📄 Licencia

MIT License

---

**Autor**: Bruno Gramaje  
**Institución**: MUCEIM - Máster en Big Data  
**Fecha**: Diciembre 2025
