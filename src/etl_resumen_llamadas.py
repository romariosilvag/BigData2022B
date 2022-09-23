# Pseudo codigo
# 1. leer archivo .csv
# 2. extraer el resumen
# 3. guardar el resumen en formato .csv

import numpy as np
import pandas as pd
import os
from pathlib import Path


def main():
    # leer archivo
    filename="llamadas123_julio_2022.csv"
    data = get_data(filename)

    df_resumen = get_summary(data)
    # extraer resumen
    test = save_data(df_resumen, filename)
    # guarde el resumen

def save_data(df, filename):
    out_name = "resumen_"+filename
    root_dir = Path(".").resolve().parent
    out_path = os.path.join(root_dir, "data", "processed", out_name)
    print(out_path)

    df.to_csv(out_path)

def get_summary(data):
    dict_resumen = dict()
    # traer una lista con las cabeceras de la tabla
    for col in data.columns:
        valores_unicos = data[col].unique()
        n_valores = len(valores_unicos)
        # print(col,n_valores)
        dict_resumen[col] = n_valores

    df_resumen = pd.DataFrame.from_dict(dict_resumen, orient='index')
    df_resumen.rename({0: 'Count'}, axis=1, inplace=True)

    return df_resumen

def get_data(filename):
    #filename = "llamadas123_julio_2022.csv"
    data_dir = "raw"
    root_dir = Path(".").resolve().parent
    file_path = os.path.join(root_dir, "data",data_dir, filename)
    data = pd.read_csv(file_path, encoding="latin-1", sep=";")
    print(data.shape)

    return data


if __name__ == '__main__':
    main()