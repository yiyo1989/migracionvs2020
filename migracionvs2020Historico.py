import os
import csv
import re
import sys
import time
import logging
import unicodedata
import pyodbc
from datetime import datetime
from threading import local
from concurrent.futures import ThreadPoolExecutor
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from tqdm import tqdm
import pandas as pd
from azure.core.pipeline.transport import RequestsTransport

# Configuración
AZURE_STORAGE_CONNECTION_STRING = ''
SOURCE_CONTAINER = 'datosvs2020'
DESTINATION_CONTAINER = 'sip-historico'
SQL_SERVER = 'tcp:penazsqlmicred00-test.eadf80c68004.database.windows.net'
SQL_DATABASE = 'ScanVisionISO'
SQL_USERNAME = 'gbojorge'
SQL_PASSWORD = 'bojorge123*'
MAX_WORKERS = 10
MAX_RETRIES = 3
RETRY_DELAY = 5
PROCESADOS_FILE = "archivos_procesados_historico.csv"
ERRORES_FILE = "archivos_con_error_historico.csv"
NO_METADATA_FILE ="archivos_sin_metadata_historico.csv"
DIRECTORIOS_PROCESADOS_FILE="directorios_procesados_historico.csv"
# Logging
logging.basicConfig(
    #filename='migracion_tiempo.log',
    #filemode='a',
    #format='%(asctime)s [%(levelname)s] %(message)s',
    level=logging.WARNING
)

# Crear transporte con mayor pool
transport = RequestsTransport(connection_pool_maxsize=20)

# Thread-local para conexiones SQL
thread_local = local()

def get_sql_connection():
    if not hasattr(thread_local, "conn"):
        conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};'
            f'UID={SQL_USERNAME};PWD={SQL_PASSWORD}'
        )
        thread_local.conn = pyodbc.connect(conn_str)
    return thread_local.conn

# Directorio procesados
def leer_directorios_procesados():
    if not os.path.exists(DIRECTORIOS_PROCESADOS_FILE):
        return set()
    with open(DIRECTORIOS_PROCESADOS_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())


def guardar_directorio_procesado(directorio):
    with open(DIRECTORIOS_PROCESADOS_FILE, 'a', encoding='utf-8', newline='\n') as f:
        writer = csv.writer(f)        
        writer.writerow([directorio])



# Limpieza de campos
def limpiar_valor(valor, max_len=256):
    valor = str(valor).strip()
    valor = unicodedata.normalize('NFKD', valor).encode('ASCII', 'ignore').decode('ASCII')
    # Reemplazar caracteres especiales no permitidos por guion bajo
    valor = re.sub(r'[\\\\/\\?#%&+:*<>|\"\'=@]', '_', valor)
    return valor[:max_len]

def formatear_mes(fecha):    
    meses = {
    1: "01-enero", 2: "02-febrero", 3: "03-marzo", 4: "04-abril",
    5: "05-mayo", 6: "06-junio", 7: "07-julio", 8: "08-agosto",
    9: "09-septiembre", 10: "10-octubre", 11: "11-noviembre", 12: "12-diciembre"
    }
    mes_formateado = meses.get(fecha.month, f"{fecha.month:02d}")
    return mes_formateado
    

# Acumuladores en memoria
procesados = []
errores = []
sin_metadata = []

def procesar_blob(blob):
    blob_name = blob.name
    file_name = os.path.splitext(os.path.basename(blob_name))[0]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = get_sql_connection()
            cursor = conn.cursor()
            cursor.execute("{CALL METADATA_MIGRACION (?)}", file_name)
            row = cursor.fetchone()

            if not row:
                sin_metadata.append([f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",file_name])
                return

            num_caso=row.num_caso
            identificacion= row.num_identificacion
            fecha_registro = row.fec_registro
            if num_caso is None or fecha_registro is None or identificacion is None:
                raise ValueError("Faltan datos clave: num_caso o fec_registro o num_identificacion")

            # Limpieza de campos
           
            nombre = limpiar_valor(row.nombre) if row.nombre else None
            apellido1 = limpiar_valor(row.apellido1) if row.apellido1 else None
            apellido2 = limpiar_valor(row.apellido2) if row.apellido2 else None
            tipo_identificacion = row.tip_identificacion
            gaveta = row.gaveta.lower()
            contenedor = row.contenedor
            categoria = row.categoria            
            nombre_archivo_sql = limpiar_valor(row.nom_archivo)
            tipo_objeto = row.tip_objeto
            extension = row.extension
            id_registro = str(row.ide_reg)
            

            new_blob_path = f"{gaveta}/{fecha_registro.year}/{formatear_mes(fecha_registro)}/{file_name}.{extension}"
            source_blob_client = source_container_client.get_blob_client(blob_name)
            dest_blob_client = dest_container_client.get_blob_client(new_blob_path)

            dest_blob_client.start_copy_from_url(source_blob_client.url)

            metadata_dict = {
                'contenedor': contenedor,
                'categoria': categoria,
                'nombre_archivo_sql': nombre_archivo_sql,
                'fecha_registro': fecha_registro.strftime("%Y-%m-%d %H:%M:%S"),
                'tipo_objeto': limpiar_valor(tipo_objeto),
                'extension': extension,
                'ide_reg': id_registro
            }

            blobs_tags = {
                'num_caso': num_caso,
                'gaveta': gaveta,
                'fecha_registro': fecha_registro.strftime("%Y-%m-%d")
            }

            if identificacion:
                blobs_tags['identificacion'] = identificacion
            if nombre:
                blobs_tags['nombre'] = nombre
            if apellido1:
                blobs_tags['apellido1'] = apellido1
            if apellido2:
                blobs_tags['apellido2'] = apellido2
            if tipo_identificacion:
                blobs_tags['tipo_identificacion'] = tipo_identificacion

            dest_blob_client.set_blob_metadata(metadata_dict)
            dest_blob_client.set_blob_tags(blobs_tags)

            procesados.append([file_name])                        
            

            return

        except AzureError as ae:
            if '429' in str(ae):
                logging.warning(f"{file_name} -> Throttling 429, reintentando ({attempt})")
                time.sleep(RETRY_DELAY * attempt)
                continue
            else:
                raise
        except Exception as e:            
            errores.append([file_name, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {str(e)}"])
            sys.exit(1)
            return

if __name__ == "__main__":
    #Conexiones a Azure
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING,connection_pool_maxsize=50)    
    source_container_client = blob_service_client.get_container_client(SOURCE_CONTAINER)        
   
   
    if len(sys.argv) < 2:
        print("Uso: python migracionvs2020.py [contenedor_destino] <num_directorios_a_procesar>")
        sys.exit(1)

    num_directorios_a_procesar=int(sys.argv[2])    
    DESTINATION_CONTAINER=sys.argv[1]    
      
    dest_container_client = blob_service_client.get_container_client(DESTINATION_CONTAINER)
      
    # Obtener todos los blobs y extraer los directorios virtuales únicos
    #all_blobs = list(source_container_client.list_blobs())
    prefix = "migracionvs2020Historico1/"
    result = source_container_client.walk_blobs(name_starts_with=prefix, delimiter="/")
    
    directorios = [item.name for item in result if hasattr(item, "name")]
       
   
    # Leer directorios ya procesados
    dir_procesados = leer_directorios_procesados()
    pendientes = [d for d in directorios if d not in dir_procesados]

    if not pendientes:
        print("✅ Todos los directorios virtuales ya han sido procesados.")
        sys.exit(0)

    # Seleccionar los primeros N directorios pendientes
    if len(pendientes) >= num_directorios_a_procesar:
        a_procesar = pendientes[:num_directorios_a_procesar]
    else:
        a_procesar=pendientes[:len(pendientes)]
        print("Procesando la totalidad de los pendientes")    

    porcentaje_completado = ((len(directorios) - len(pendientes)) / len(directorios)) * 100
    porcentaje_restante = 100 - porcentaje_completado
    resumen_actual = f"""
    📊 RESUMEN ESTADISTICA DE MIGRACION ACTUAL PARA CONTENEDOR: {DESTINATION_CONTAINER}
    ------------------
    Directorios en contenedor: {len(directorios)}
    Directorios pendientes: {len(pendientes)}
    Directorios a procesar en actual batch: {num_directorios_a_procesar}
    Porcentaje de avance: {porcentaje_completado}
    Porcentaje pendiente de migracion: {porcentaje_restante}    
    """
    print(resumen_actual)
    
    # Filtrar blobs que pertenecen a esos directorios
    #blobs = [blob for blob in all_blobs if any(blob.name.startswith(f"migracionvs2020/{d}/") for d in a_procesar)]                
    contador_batch=0
    start_global = time.time()    
    for dir in a_procesar:
        blobs = list(source_container_client.list_blobs(name_starts_with=(f"{dir}")))
        contador_batch=contador_batch+1                                            
    #blobs = list(source_container_client.list_blobs(name_starts_with="migracionvs2020/0006/"))    
    
        total_blobs = len(blobs)
        resumen_inicio = f"""
    🚀 Iniciando migración de metadata:
    ------------------
    Directorio: {dir}
    procesando batch:{contador_batch} de {num_directorios_a_procesar}      
    """
        print(f"{resumen_inicio}")       
        
    
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            list(tqdm(executor.map(procesar_blob, blobs), total=total_blobs))        
        #Se almacena el directorio virtual procesado
        guardar_directorio_procesado(dir)


    # Guardar resultados
    # with open(PROCESADOS_FILE, "w", newline='', encoding='utf-8') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(["archivo", "num_caso", "num_identificacion"])
    #     writer.writerows(procesados)

    with open(ERRORES_FILE, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["archivo", "error"])
        writer.writerows(errores)

    with open(NO_METADATA_FILE, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["fecha","archivo"])
        writer.writerows(sin_metadata)

    duration_global = time.time() - start_global
    print("✅ Proceso finalizado.")
        
    
    # Informe resumen
    total_ok = len(procesados) #pd.read_csv(PROCESADOS_FILE).shape[0] if os.path.exists(PROCESADOS_FILE) else 0
    total_err = pd.read_csv(ERRORES_FILE).shape[0] if os.path.exists(ERRORES_FILE) else 0
    total_sin_meta = pd.read_csv(NO_METADATA_FILE).shape[0] if os.path.exists(NO_METADATA_FILE) else 0

    resumen = f"""
    📊 RESUMEN FINAL:
    ------------------
    Procesados correctamente: {total_ok}
    Con error: {total_err}
    Sin metadata en SP: {total_sin_meta}
    Total blobs analizados: {total_blobs}
    Tiempo total de ejecución: {duration_global:.2f} segundos
    """
    print(resumen)
    logging.info(resumen)
