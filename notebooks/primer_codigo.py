import numpy as np
import argparse

# Calcular min y max
def calcular_min_max(lista_numeros, verbose=True):
    min_value = min(lista_numeros)
    max_value = max(lista_numeros)

    #print("valor minimo: ", min_value)
    #print("valor maximo: ", max_value)

    if verbose == True:
        print("Entro a verbose - min max : valor : ", min_value)
    else:
        pass

    return min_value, max_value


def calcular_valores_centrales(lista_numeros, verbose=True):
    """Calcula la media y desviacion estandar de una lista de numeros

    Args:
        lista_numeros (list): lista con valores numericos
        verbose (bool, optional): decidir si imprime o no mensajes en pantalla. Defaults to True.

    Returns:
        tupla: (media, dev_Std)
    """
    media = np.mean(lista_numeros)
    dev_std = np.std(lista_numeros)

    if verbose == True:
        print("Entro a verbose - centrales : valor : ", media)
    else:
        pass

    return media, dev_std




def calcular_suma(lista_numeros, verbose=True):
    
    suma = np.sum(lista_numeros)

    if verbose == True:
        print("Entro a verbose - suma : valor : ", suma)
    else:
        pass

    return suma


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", type=str, default="True", help="Para impimir en pantalla informacion")
    args = parser.parse_args()

    print(args, type(args))

    listaValores = [2,5,4]
    suma = calcular_suma(listaValores, verbose=args.verbose)
    min_max = calcular_min_max(listaValores, verbose=args.verbose)
    centrales = calcular_valores_centrales(listaValores, verbose=args.verbose)
    print("suma", suma)
    print("min_max", min_max)
    print("centrales", centrales)

if __name__ == '__main__':

    main()
