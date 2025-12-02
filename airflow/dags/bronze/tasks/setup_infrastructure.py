"""
Airflow task for ensuring RustFS bucket exists before data ingestion.
Creates the bucket if it doesn't exist.
"""

from airflow.sdk import task


@task
def ensure_rustfs_bucket():
    """
    Airflow task to ensure RustFS bucket exists.
    Creates the bucket if it doesn't exist.
    
    Returns:
    - Dict with status information
    """
    print("[TASK] Checking if RustFS bucket exists...")
    
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.models import Variable
        
        # Get bucket name from Airflow Variables
        bucket_name = Variable.get('RUSTFS_BUCKET', default_var='mitma')
        
        # Use S3Hook to connect to RustFS
        s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
        
        # Check if bucket exists
        if s3_hook.check_for_bucket(bucket_name):
            print(f"[TASK] ✅ Bucket '{bucket_name}' already exists")
            return {
                'status': 'exists',
                'bucket': bucket_name,
                'message': f"Bucket '{bucket_name}' already exists"
            }
        else:
            # Create bucket
            print(f"[TASK] 📦 Creating bucket '{bucket_name}'...")
            s3_hook.create_bucket(bucket_name=bucket_name)
            print(f"[TASK] ✅ Bucket '{bucket_name}' created successfully")
            return {
                'status': 'created',
                'bucket': bucket_name,
                'message': f"Bucket '{bucket_name}' created successfully"
            }
            
    except Exception as e:
        print(f"[TASK] ❌ Error with bucket: {str(e)}")
        raise


@task
def verify_connections():
    """
    Airflow task to verify PostgreSQL and RustFS connections.
    
    Returns:
    - Dict with connection status
    """
    print("[TASK] Verifying connections...")
    
    results = {
        'postgres': False,
        'rustfs': False,
        'errors': []
    }
    
    # Test PostgreSQL
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        print("[TASK] 🔍 Testing PostgreSQL connection...")
        pg_hook = PostgresHook(postgres_conn_id='postgres_datos_externos')
        
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        cursor.close()
        connection.close()
        
        print(f"[TASK] ✅ PostgreSQL OK: {version[0][:50]}...")
        results['postgres'] = True
        
    except Exception as e:
        error_msg = f"PostgreSQL connection failed: {str(e)}"
        print(f"[TASK] ❌ {error_msg}")
        results['errors'].append(error_msg)
    
    # Test RustFS
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        print("[TASK] 🔍 Testing RustFS connection...")
        s3_hook = S3Hook(aws_conn_id='rustfs_s3_conn')
        
        # Usar get_conn() para obtener el cliente boto3
        s3_client = s3_hook.get_conn()
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response.get('Buckets', [])]
        
        print(f"[TASK] ✅ RustFS OK. Buckets: {buckets}")
        results['rustfs'] = True
        
    except Exception as e:
        error_msg = f"RustFS connection failed: {str(e)}"
        print(f"[TASK] ❌ {error_msg}")
        results['errors'].append(error_msg)
    
    # Final status
    if results['postgres'] and results['rustfs']:
        print("[TASK] ✅ All connections verified successfully")
        results['status'] = 'success'
    else:
        print("[TASK] ❌ Some connections failed")
        results['status'] = 'failed'
        raise Exception(f"Connection verification failed: {results['errors']}")
    
    return results
