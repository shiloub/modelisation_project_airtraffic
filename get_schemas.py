import json
# from io import BytesIO
import os
import gzip
import pandas as pd
import sys

def list_folder(folder_path):
    files = os.listdir(folder_path)
    if (files):
        return files
    return (None)

def get_previous_schema_file(path):
    try:
        with open(path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except FileNotFoundError:
        return ({})
    return (data)

def get_string_schema_from_csv(path, file):
    try:
        with gzip.open(path + file, "rt") as gz_file:
                df = pd.read_csv(gz_file, nrows=0)
    except FileNotFoundError:
        print("File", file, "not found in", path)
        exit(0)
    except gzip.BadGzipFile:
        try:
            with open(path + file, "rt") as csv_file:
                df = pd.read_csv(csv_file, nrows=0)
        except Exception as e:
            print("Error reading the file:", e)
            exit(0)
    except Exception as e:
        print("Unknown exception:", e)
        exit(0)
    return (",".join(df.columns))

def make_headers_json(path:str, files, table_name, previous_schemas):
    
    """
    Passe en revue les différents schemas dans les csv du folder, puis crée une string au format json, contenant en clef le shema représenté en string,
    et en value un tableau de string avec tout les fichiers respectant ce schema dans le header.
    """
    
    if not path.endswith("/"):
        path += "/"
    same_schema_files = []
    current_string_schema=""
    schemas_files_dict = previous_schemas
    if files:
        for file in files:
            if file.startswith(table_name) and (file.endswith(".csv") or file.endswith(".CSV")):
                string_schema = get_string_schema_from_csv(path, file)
                if (current_string_schema == "" or current_string_schema != string_schema):
                    if current_string_schema != "":
                        schemas_files_dict[current_string_schema] = same_schema_files
                        same_schema_files = []
                    current_string_schema = string_schema
                same_schema_files.append(file)
        if (current_string_schema != ""):
            schemas_files_dict[current_string_schema] = same_schema_files
                # print(file, current_string_schema)     
    else:
        raise ValueError("no file found in the bucket")
    if not schemas_files_dict:
        raise ValueError(f"no csv for table {table_name} in {path}")
    with open(f"./jsons/config/csv_schemas/schemas_{table_name}.json", "w", encoding="UTF-8") as f:
        json.dump(schemas_files_dict, f, ensure_ascii=False, indent=4)
        

def main():
    if len(sys.argv) != 3:
        print("Error: argument expected: folder_path table_name")
        exit(0)
    folder_path = sys.argv[1]
    table_name = sys.argv[2]
    previous = get_previous_schema_file(f"./jsons/config/csv_schemas/schemas_{table_name}.json")
    print(previous)
    files = list_folder(folder_path)
    make_headers_json(folder_path, files, table_name, previous)
    
    
if __name__ == "__main__":
    main()