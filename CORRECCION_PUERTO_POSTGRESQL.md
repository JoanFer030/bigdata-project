# 🔧 CORRECCIÓN URGENTE - Puerto PostgreSQL

## ❌ Problema Detectado

La conexión `postgres_datos_externos` está usando el puerto **30432** (puerto externo del host), pero desde Airflow debe usar el puerto **5432** (puerto interno de Docker).

## ✅ Solución

### 1. Editar la Conexión PostgreSQL

1. Ve a: http://localhost:8080/connection/list/
2. Click en el **lápiz** de `postgres_datos_externos`
3. Cambiar:
   ```
   Port: 30432  ❌ INCORRECTO
   ```
   Por:
   ```
   Port: 5432   ✅ CORRECTO
   ```
4. Click **Save**

### 2. Por qué?

**Explicación:**

```
┌─────────────────────────────────────┐
│  Host (tu máquina)                  │
│                                     │
│  localhost:30432 ← Puerto externo   │
│         ↓                           │
│  ┌──────────────────────────────┐   │
│  │  Docker Network              │   │
│  │                              │   │
│  │  postgresql:5432 ← Puerto    │   │
│  │       ↑         interno      │   │
│  │       │                      │   │
│  │  Airflow ← Usa puerto        │   │
│  │           interno 5432       │   │
│  └──────────────────────────────┘   │
└─────────────────────────────────────┘
```

- **Desde tu máquina**: Usas `localhost:30432`
- **Desde Airflow (dentro de Docker)**: Usa `postgresql:5432`

---

## 🧪 Después de Corregir

1. Guarda la conexión con `Port: 5432`
2. Ve al DAG `bronze_mitma_all_datasets`
3. Click en el task `verify_connections` que falló
4. Click en **"Clear"** para reintentarlo
5. Debería aparecer: ✅ PostgreSQL OK

---

## 📝 Configuración Correcta Final

```
Connection Id:   postgres_datos_externos
Connection Type: Postgres
Host:            postgresql        ← Nombre del contenedor
Schema:          mitma
Login:           admin
Password:        muceim-duckduck.2025!
Port:            5432             ← Puerto INTERNO ✅
```
