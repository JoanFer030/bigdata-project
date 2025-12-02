# 🔧 Configurar Conexiones Manualmente en Airflow

Ya que `airflow_settings.yaml` tiene problemas al cargar las conexiones automáticamente, vamos a crearlas manualmente desde la UI de Airflow.

## 📋 Instrucciones

### 1. Acceder a Airflow UI

```
URL: http://localhost:8080
Usuario: admin
Contraseña: admin
```

### 2. Navegar a Conexiones

```
Admin → Connections
```

O directamente:
```
http://localhost:8080/connection/list/
```

---

## ✅ CONEXIÓN 1: PostgreSQL

Click en **"+"** (Add a new record)

```
Connection Id:   postgres_datos_externos
Connection Type: Postgres
Host:            postgresql
Schema:          mitma
Login:           admin
Password:        muceim-duckduck.2025!
Port:            5432
```

**Extra:** (dejar vacío o agregar):
```json
{}
```

Click **Save**

---

## ✅ CONEXIÓN 2: RustFS (S3)

Click en **"+"** (Add a new record)

```
Connection Id:   rustfs_s3_conn
Connection Type: Amazon Web Services
```

**Extra:** (copiar exactamente):
```json
{
  "endpoint_url": "http://rustfs:9000",
  "region_name": "us-east-1",
  "aws_access_key_id": "admin",
  "aws_secret_access_key": "muceim-duckduck.2025!"
}
```

**IMPORTANTE:** Dejar Login y Password **vacíos** (las credenciales van en Extra)

Click **Save**

---

## ✅ VARIABLES

Navegar a:
```
Admin → Variables
```

O directamente:
```
http://localhost:8080/variable/list/
```

### Crear estas variables:

Click en **"+"** para cada una:

| Key | Val | 
|-----|-----|
| `RUSTFS_BUCKET` | `mitma` |
| `POSTGRES_DB_NAME` | `mitma` |
| `DATALAKE_MODE` | `production` |

---

## ✅ Verificar Pool

Navegar a:
```
Admin → Pools
```

Debería existir:
```
Pool Name: bronze_ingestion_pool
Slots: 3
Description: Pool para limitar tareas de ingesta en capa Bronze
```

Si no existe, crearlo manualmente.

---

## 🧪 Probar Conexiones

### Desde la UI:

1. Ve a Admin → Connections
2. Click en el **icono de lápiz** de `postgres_datos_externos`
3. Click en **"Test"** al final del formulario
4. Debería aparecer: ✅ "Connection successfully tested"

5. Repetir para `rustfs_s3_conn`

### Desde Python (opcional):

Puedes ejecutar el task `verify_connections` en tu DAG `bronze_mitma_all_datasets` para verificar ambas conexiones.

---

## 📊 Resultado Esperado

Después de configurar todo, deberías tener:

**Connections (2):**
- ✅ postgres_datos_externos
- ✅ rustfs_s3_conn

**Variables (3):**
- ✅ RUSTFS_BUCKET = mitma
- ✅ POSTGRES_DB_NAME = mitma  
- ✅ DATALAKE_MODE = production

**Pools (1):**
- ✅ bronze_ingestion_pool (slots: 3)

---

## 🚀 Siguiente Paso

Una vez configurado todo, ejecuta el DAG:

```
DAG: bronze_mitma_all_datasets
Params:
  start: 2023-01-01
  end: 2023-01-03
```

El primer task (`verify_connections`) validará que todo esté correcto.

---

## 💡 Nota

Si ves el error al iniciar Airflow:
```
Error: error adding connections: error listing connections...
```

**Puedes ignorarlo**. Es solo un problema del CLI al leer el YAML, pero Airflow funciona perfectamente. Las conexiones creadas manualmente en la UI funcionarán sin problemas.
