import csv

def csv2list(file_path, delimiter, has_header):
    data = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file, delimiter=delimiter)
        next(reader) if has_header else None
        data = [row for row in reader]
    return data