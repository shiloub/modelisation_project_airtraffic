import json
import sys

def read_json(json_path):
    with open(json_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return (data)

def make_json_file(path, obj):
    with open(path, "w", encoding="UTF-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=4)
    print(f"Created {path}")



def generate_table_def_json(schema, uris):
   json_global_dict = {}
   temp_dict = {}
   temp_dict["allowJaggedRows"] = False
   temp_dict["allowQuotedNewlines"] = False
   temp_dict["encoding"] = "UTF-8"
   temp_dict["fieldDelimiter"] = ","
   temp_dict["skipLeadingRows"] = 1
   json_global_dict["csvOptions"] = temp_dict.copy()
   temp_dict = {}
   temp_dict["fields"] = schema
   json_global_dict["schema"] = temp_dict.copy()
   json_global_dict["sourceFormat"] = "CSV"
   json_global_dict["sourceUris"] = [uris]
   return (json_global_dict)
   
def main():
    if len(sys.argv) != 5:
        print("Error: arg needed: table_name different_schemas_number(1) bucket_name, path_into_bucket")
        exit(0)
    table_name = sys.argv[1]
    n = sys.argv[2]
    bucket_name = sys.argv[3]
    path = sys.argv[4]
    schema = read_json(f"./jsons/{table_name}_schema_EXTERNAL{n}.json")
    json_def = generate_table_def_json(schema, f"gs://{bucket_name}/{path}/{table_name}/schema{n}/in/*.csv")
    make_json_file(f"./jsons/{table_name}_schema_EXTERNAL{n}_def.json", json_def)
    
if __name__ == "__main__":
    main()
   