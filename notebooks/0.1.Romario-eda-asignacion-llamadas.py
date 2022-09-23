from hashlib import new
import numpy as np
import pandas as pd
import os
from pathlib import Path
from dateutil.parser import parse

# Calcular min y max
def load_file(file):
    """
    Carga archivo csv

    Args:
        file (string): nombre del archivo

    Returns:
        df: retorna dataframe con la informacion del archivo csv
    """
    root_dir = Path(".").resolve().parent # ruta del proyecto
    data_dir = "raw" 
    file_path = os.path.join(root_dir,"data",data_dir, file)
    data = pd.read_csv(file_path, encoding="latin-1", sep=";")
    new_data = data.copy() # hacer copia de df
    print(type(new_data))
    return new_data


def delete_rows_duplicates(df_calls):
    """
    Elimina registros duplicados del dataframe

    Args:
        df_calls (df): df con informacion de llamadas

    Returns:
        df: retorna df sin registros duplicados y con nuevo index
    """
    df_calls = df_calls.drop_duplicates() # eliminar registros duplicados
    df_calls = df_calls.reset_index(drop=True) # resetear indices de la tabla

    return df_calls

def str_to_datetime(value_str):
    """
    convierte string a datetime

    Args:
        value_str (str): recibe variable string

    Returns:
        datetime: retorna variable datetime
    """
    try:
        var_datetime2 = parse(value_str, dayfirst=False)
        var_datetime = pd.to_datetime(var_datetime2)
    except Exception as e:
        #print(value_str, e)
        var_datetime = np.nan
        
    return var_datetime



def convert_column_to_datetime(df_calls, column):
    """
    convierte una columna de un df a datetime

    Args:
        df_calls (df): nombre de df
        column (str): nombre de columna

    Returns:
        df: retorna df con la columna modificada
    """
    new_column = df_calls[column]
    list_column = list()

    for i in range(0,len(new_column)):
        result_column = str_to_datetime(new_column[i])
        list_column.append(result_column)
        
    df_calls[column] = list_column

    return df_calls


def replace_values(df_calls, column):
    """
    remplazar valores de una columna

    Args:
        df_calls (df): nombre de df
        column (str): nombre de columna

    Returns:
        df: retorna df con los datos de la columna corregidos
    """
    if column == 'EDAD':
        
        df_calls["EDAD"]  = df_calls["EDAD"].replace({"SIN_DATO": 0})
        df_calls = df_calls.astype({"EDAD":'int64'},errors = 'ignore') 
    
    elif column == "GENERO":

            df_calls["GENERO"]  = df_calls["GENERO"].replace({"SIN_DATO": np.nan}) 
    
    elif column == "UNIDAD":
            
            df_calls["UNIDAD"]  = df_calls["UNIDAD"].replace({"SIN_DATO": np.nan,"A¤os": "Años"})

    elif column == "TIPO_INCIDENTE":
            
            df_calls["TIPO_INCIDENTE"]  = df_calls["TIPO_INCIDENTE"].replace({"Convulsi¢n": "Convulsión","Dolor Tor cico": "Dolor Torácico","S¡ntomas Gastrointestinales": "Sintomas Gastrointestinales","Ca¡da de Altura": "Caida de Altura","Patolog¡a Ginecobsttrica": "Patologia Gineco Obstétrica","Intoxicaci¢n": "Intoxicación","Acompa¤amiento Evento": "Acompañamiento Evento","Electrocuci¢n / rescate": "Electrocución / rescate"}) 

    elif column == "LOCALIDAD":
            
            df_calls["LOCALIDAD"]  = df_calls["LOCALIDAD"].replace({"Engativ ": "Engativá","Ciudad Bol¡var": "Ciudad Bolívar","Fontib¢n": "Fontibón","Usaqun": "Usaquén","San Crist¢bal": "San Cristóbal","Los M rtires": "Los Mártires","Antonio Nari¤o": "Antonio Nariño"}) 
    
    else:
        pass

    return df_calls


def main():
    """
    Funcion Principal donde se realiza llamado de las demas funciones.
    """
    #filename = input("Ingrese el nombre completo del archivo: ")
    filename = "llamadas123_julio_2022.csv"
    new_data = load_file(filename) # cargar archivo

    print("\n Copia del Original \n")
    print(new_data,new_data.info())

    new_data = delete_rows_duplicates(new_data) # eliminar duplicados
    new_data = convert_column_to_datetime(new_data, "FECHA_INICIO_DESPLAZAMIENTO_MOVIL") # convertir columna a datatime
    new_data = convert_column_to_datetime(new_data, "RECEPCION") # convertir columna a datatime
    new_data = replace_values(new_data, "EDAD") # remplazar valores de columna
    new_data = replace_values(new_data, "GENERO") # remplazar valores de columna
    new_data = replace_values(new_data, "UNIDAD") # remplazar valores de columna
    new_data = replace_values(new_data, "TIPO_INCIDENTE") # remplazar valores de columna
    new_data = replace_values(new_data, "LOCALIDAD") # remplazar valores de columna

    print("\n Resultado Final \n")
    print(new_data,new_data.info())


if __name__ == '__main__':
    main()
