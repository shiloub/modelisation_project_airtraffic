import json
from io import BytesIO
import os
import gzip
import pandas as pd

BUCKET_NAME = "modelisation-bucket-eu"
PREFIX = "Air Traffic/actual/"
INVALID_PREFIX = "not_ready_to_load_yet_but_its_a_question_of_minutes/actual/"
VALID_PREFIX = "ready_to_load/actual/"

def list_folder(folder_path):
    files = os.listdir(folder_path)
    if (files):
        return files
    return (None)

def make_headers_json(path, files):
    
    """
    Passe en revue les différents schemas dans les csv du folder, puis crée une string au format json, contenant en clef le shema représenté en string,
    et en value un tableau de string avec tout les fichiers respectant ce schema dans le header.
    """
    same_schema_files = []
    current_string_schema=""
    schemas_files_dict = {}
    if files:
        for file in files:
            if file.startswith("pax_terminals_actual") and (file.endswith(".csv") or file.endswith(".CSV")):
                try:
                    with gzip.open(path + file, "rt") as gz_file:
                            df = pd.read_csv(gz_file, nrows=0)
                except FileNotFoundError:
                    print("File", file, "not found in", path)
                except gzip.BadGzipFile:
                    print("File", file , "corrupted or not right gzip compressed")
                except Exception as e:
                    print("Unknown exception:", e)
                string_schema = ",".join(df.columns)
                if (current_string_schema == "" or current_string_schema != string_schema):
                    if current_string_schema != "":
                        schemas_files_dict[current_string_schema] = same_schema_files
                        same_schema_files = []
                    current_string_schema = string_schema
                same_schema_files.append(file)
        schemas_files_dict[current_string_schema] = same_schema_files
                # print(file, current_string_schema)     
    else:
        raise ValueError("no file found in the bucket")
    with open("./schemas_actual.json", "w", encoding="UTF-8") as f:
        json_dict = json.dump(schemas_files_dict, f, ensure_ascii=False, indent=4)
def main():
    folder_path = "./Air Traffic/actual/"
    files = list_folder(folder_path)
    make_headers_json(folder_path, files)
    
    
if __name__ == "__main__":
    main()