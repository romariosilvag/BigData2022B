from hashlib import new
import numpy as np
import pandas as pd
import os
from pathlib import Path
from dateutil.parser import parse
from os import listdir
import shutil
import logging
import pandas_gbq
from google.cloud import storage
from google.cloud import bigquery

def list_file():
    """
    Carga la lista de archivos en un directorio

    Args:
        No requiere argumentos

    Returns:
        listFiles: retorna lista con los nombres de archivos
    """
  
    bucket_name="romariosilva_bucket_llamadas123"
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)

    list_files_bucket = []

    for blob in blobs:
        if blob.name.split("/")[0] == "data" and blob.name.split("/")[1] == "raw" and "data" and blob.name.split("/")[2] != "":
            list_files_bucket.append(blob.name.split("/")[2])
    
    listFiles = list_files_bucket
    
    logging.debug(f'ruta de archivos:{blob.name.split("/")[0][1]}')
    logging.debug(f'lista de archivos:{list_files_bucket}')
    logging.debug(f'finalizo funcion : {list_file.__name__}')

    return listFiles


def replace_columns(new_data):
    """
    Corrige columnas de un dataframe

    Args:
        new_data (dataframe): dataframe a corregir
        
    Returns:        
        new_data: retorna dataframe corregido
    """
    new_data.rename({'FECHA_INICIO_DESPLAZAMIENTO-MOVIL':'FECHA_INICIO_DESPLAZAMIENTO_MOVIL'}, axis=1, inplace=True)
    new_data.rename({'FECHA_DESPACHO_518':'FECHA_INICIO_DESPLAZAMIENTO_MOVIL'}, axis=1, inplace=True)

    if 'RECEPCION' in new_data.columns:
        # Validar si la columna Existe
        pass
    else:
        # Si no existe crea una columna con valores nulos
        new_data['RECEPCION'] = pd.NaT
        logging.warn(f'Se creo columna RECEPCION vacia, funcion : {replace_columns.__name__}')
    logging.debug(f'finalizo funcion : {replace_columns.__name__}')
    return new_data

def load_file(file):
    """
    Carga archivo csv, lo mueve a ruta processed, revisa estructura de columnas del dataframe y si es necesario corrige columnas del mismo

    Args:
        file (string): nombre del archivo

    Returns:
        new_data: retorna dataframe valido para continuar con limpieza de datos
    """
    bucket = "gs://romariosilva_bucket_llamadas123"
    root_dir = Path(".").resolve().parent 
    folder1 = "data"
    folder2 = "raw"
    folder3 = "processed"
    file_path = os.path.join(bucket,folder1,folder2, file) # ruta origen
    data = pd.read_csv(file_path, encoding="latin-1", sep=";") # leer archivo csv
    new_data = data.copy() # hacer copia de df
    file_path_des = os.path.join(bucket,folder1,folder3, file) # ruta destino
    move_files(file) # mover archivo a carpeta processed
    check = check_columns(new_data)

    if check == True:
        pass
    else:
        logging.warn(f'remplazando columnas, archivo : {file}')
        new_data = replace_columns(new_data)
    logging.debug(f'finalizo funcion : {load_file.__name__}')
    return new_data

def move_files(file):
    """
    mueve archivos entre carpetas de bucket

    Args:
        file (str): nombre del archivo a mover 

    Returns:
        No retorna datos
    """
    bucket_name = "romariosilva_bucket_llamadas123"
    blob_name="data/raw/"+file
    destination_blob_name = "data/processed/"+file

    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    source_bucket.delete_blob(blob_name)
    logging.debug(f'finalizo funcion : {move_files.__name__}')
    return
    

def delete_rows_duplicates(df_calls):
    """
    Elimina registros duplicados de un dataframe

    Args:
        df_calls (dataframe): dataframe con informacion de llamadas

    Returns:
        df_calls: retorna dataframe sin registros duplicados y con nuevo index
    """
    df_calls = df_calls.drop_duplicates() # eliminar registros duplicados
    df_calls = df_calls.reset_index(drop=True) # resetear indices del dataframe
    df_calls = df_calls[df_calls["NUMERO_INCIDENTE"].notna()] # filtrar dataframe por la columna NUMERO INCIDENTE y traer todos los registros no nullos
    logging.debug(f'finalizo funcion : {delete_rows_duplicates.__name__}')
    return df_calls

def str_to_datetime(value_str):
    """
    convierte string a datetime

    Args:
        value_str (str): recibe variable string

    Returns:
        var_datetime: retorna variable datetime
    """
    try:
        var_datetime2 = parse(value_str, dayfirst=False)
        var_datetime = pd.to_datetime(var_datetime2)
    except Exception as e:
        var_datetime = pd.NaT
    
    return var_datetime



def convert_column_to_datetime(df_calls, column):
    """
    convierte una columna de un dataframe a datetime

    Args:
        df_calls (df): nombre de dataframe
        column (str): nombre de columna

    Returns:
        df_calls: retorna dataframe con la columna modificada
    """
    new_column = df_calls[column]
    list_column = list()

    for i in range(0,len(new_column)):
        try:
            result_column = str_to_datetime(new_column[i])
        except:
            result_column = pd.NaT
        
        list_column.append(result_column)
        
    df_calls[column] = list_column
    logging.debug(f'finalizo funcion : {convert_column_to_datetime.__name__}')
    return df_calls


def replace_values(df_calls, column):
    """
    Remplazar valores de una columna

    Args:
        df_calls (df): nombre de dataframe
        column (str): nombre de columna

    Returns:
        df_calls: retorna dataframe con los datos de la columna corregidos
    """
    if column == 'EDAD':
        
        df_calls["EDAD"]  = df_calls["EDAD"].replace({"SIN_DATO": 0})
        df_calls["EDAD"]  = df_calls["EDAD"].replace({"Sin_dato": 0})
        try:
            df_calls = df_calls.astype({"EDAD":'int64'},errors = 'ignore') 
            logging.debug(f'finalizo funcion : {replace_values.__name__} column EDAD')
        except Exception as Argument:
            logging.exception(f'no logro finalizar funcion : {replace_values.__name__} column EDAD')
    
    elif column == "GENERO":

            df_calls["GENERO"]  = df_calls["GENERO"].replace({"SIN_DATO": np.nan}) 
            df_calls["GENERO"]  = df_calls["GENERO"].replace({"Sin_dato": np.nan})
            logging.debug(f'finalizo funcion : {replace_values.__name__} column GENERO')
    
    elif column == "UNIDAD":
            
            df_calls["UNIDAD"]  = df_calls["UNIDAD"].replace({"SIN_DATO": np.nan,"A¤os": "Años"})
            df_calls["UNIDAD"]  = df_calls["UNIDAD"].replace({"Sin_dato": np.nan})
            logging.debug(f'finalizo funcion : {replace_values.__name__} column UNIDAD')

    elif column == "TIPO_INCIDENTE":
            
            df_calls["TIPO_INCIDENTE"]  = df_calls["TIPO_INCIDENTE"].replace({"Convulsi¢n": "Convulsión","Dolor Tor cico": "Dolor Torácico","S¡ntomas Gastrointestinales": "Sintomas Gastrointestinales","Ca¡da de Altura": "Caida de Altura","Patolog¡a Ginecobsttrica": "Patologia Gineco Obstétrica","Intoxicaci¢n": "Intoxicación","Acompa¤amiento Evento": "Acompañamiento Evento","Electrocuci¢n / rescate": "Electrocución / rescate"}) 
            logging.debug(f'finalizo funcion : {replace_values.__name__} column TIPO_INCIDENTE')

    elif column == "LOCALIDAD":
            
            df_calls["LOCALIDAD"]  = df_calls["LOCALIDAD"].replace({"Engativ ": "Engativá","Ciudad Bol¡var": "Ciudad Bolívar","Fontib¢n": "Fontibón","Usaqun": "Usaquén","San Crist¢bal": "San Cristóbal","Los M rtires": "Los Mártires","Antonio Nari¤o": "Antonio Nariño"}) 
            logging.debug(f'finalizo funcion : {replace_values.__name__} column LOCALIDAD')
    else:
        pass
    
    return df_calls


def check_columns(new_data):
    """
    Revisar Columnas de un dataframe

    Args:
        new_data (dataframe): nombre de dataframe

    Returns:
        True/False: retorna binario si cumple o no con la estructura de columnas definida
    """
    tup_columns =  ("NUMERO_INCIDENTE","FECHA_INICIO_DESPLAZAMIENTO_MOVIL","CODIGO_LOCALIDAD","LOCALIDAD","EDAD","UNIDAD","GENERO","RED","TIPO_INCIDENTE","PRIORIDAD","RECEPCION")
    list_columns = list(tup_columns)
    list_columns_nd =new_data.columns.to_list()
    
    if list_columns == list_columns_nd:
        logging.debug(f'finalizo funcion : {check_columns.__name__}')
        return True
    else:
        logging.debug(f'no cumple con la estructura de columnas definida, funcion : {check_columns.__name__}')
        return False
        
def save_bq(new_data):
    """
    limpiar datos de un dataframe si cuenta con la estructura de columnas definida

    Args:
        new_data (dataframe): nombre de dataframe 
        check (binario): valor sobre requisitos del dataframe
        filename (str): nombre de archivo 

    Returns:
        No retorna datos
    """  
    try:

        client = bigquery.Client()
        job = client.load_table_from_dataframe(
            new_data, "espbigdatarsg.espbigdatarsg.llamadas_123_new"
        )  # Make an API request.
        job.result()  # Wait for the job to complete.
        logging.info(f'insertando datos en bigquery : {job.result()}')
    except Exception as Argument:
        logging.error(f'error insertando datos en bigquery : {job.result()}')
        logging.exception("Error occurred while printing GeeksforGeeks")
        
        

def clean_data(new_data,check,filename):
    """
    limpiar datos de un dataframe si cuenta con la estructura de columnas definida

    Args:
        new_data (dataframe): nombre de dataframe 
        check (binario): valor sobre requisitos del dataframe
        filename (str): nombre de archivo 

    Returns:
        No retorna datos
    """
    if check == True:
        new_data = delete_rows_duplicates(new_data) # eliminar duplicados
        new_data = convert_column_to_datetime(new_data, "FECHA_INICIO_DESPLAZAMIENTO_MOVIL") # convertir columna a datatime
        new_data = convert_column_to_datetime(new_data, "RECEPCION") # convertir columna a datatime
        new_data = replace_values(new_data, "EDAD") # remplazar valores de columna
        new_data = replace_values(new_data, "GENERO") # remplazar valores de columna
        new_data = replace_values(new_data, "UNIDAD") # remplazar valores de columna
        new_data = replace_values(new_data, "TIPO_INCIDENTE") # remplazar valores de columna
        new_data = replace_values(new_data, "LOCALIDAD") # remplazar valores de columna
        save_bq(new_data) # guardar en bq
        logging.info(f'finalizo limpieza de datos : {filename}')
    else:
        logging.critical(f'archivo omitido por que no cuenta con la estructura esperada : {filename}')
    logging.debug(f'finalizo funcion : {clean_data.__name__}')
    return

def main(filename):
    """
    Funcion Principal donde se realiza llamado de las demas funciones.
    """
    new_data = load_file(filename) # dataframe cargado
    check = check_columns(new_data) # revision de columnas
    clean_data(new_data,check,filename) # limpieza de los datos


if __name__ == '__main__':

    # Configuracoin de log
    logging.basicConfig(filename = 'eda-llamadas-123.log',
                    level = logging.DEBUG,
                    #format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s')
                    format = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')

    # Procesar lista de archivos                        
    listFiles = list_file()
    for file_name in listFiles:
        logging.info(f'Procesando : {file_name}')
        main(file_name)
    

