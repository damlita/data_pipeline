# ETL - Pipeline: Extract, Transform, Load
# Output geht als Input in die Funktion ein in einem Loop
# hallo
from typing import Generator

def read_large_dataset(file_name: str) -> Generator:
    with open(file_name) as f:
        for line in f:
            yield line


def split_lines(file_generator: Generator):
    """Splitte Zeilen in eine Liste(kommaseparier"""
    print("Split lines Generator: ", type(file_generator))
    result = (line.strip().split(',') for line in file_generator)
    return result


def dictify(list_generator: Generator):
    cols = next(list_generator) # spaltennamen der Datei
    return (dict(zip(cols, data)) for data in list_generator)

def load_data(file_name: str):
    file_generator = read_large_dataset(file_name)
    split_generator = split_lines(file_generator)
    result = dictify(split_generator)
    return result



"""   
    for element in d:
        print(element)
    print(d)
    print(next(d))
    print(next(d))
    print(next(d))
    print(next(d))
    #print(next(split_generator))
    #print(next(split_generator))

"""
