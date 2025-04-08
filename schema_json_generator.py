import json
import sys

def read_json(json_path):
    with open(json_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return (data)

def make_json(field_string, final_json_name, table_name):
    json_formated_array = []
    temp_dict = {}
    for field in field_string.split(','):
        temp_dict["name"] = field
        temp_dict["type"] = "STRING"
        json_formated_array.append(temp_dict.copy())
    with open("./jsons/config/json_parsed_fields/{table_name}/" + final_json_name, "w", encoding="UTF-8") as f:
        json.dump(json_formated_array, f, ensure_ascii=False, indent=4)
    print(f"Created ./jsons/config/json_parsed_fields/{table_name}/{final_json_name}")

def main():
    if len(sys.argv) != 2:
        print("Error: expected arg: table_name")
        exit(0)
    table_name = sys.argv[1]
    i = 0
    data = read_json(f"jsons/config/csv_schemas/schemas_{table_name}.json")
    for field_string in data:
        i+=1
        make_json(field_string, f"{table_name}_schema_EXTERNAL{i}.json")
        

if __name__ == "__main__":
    main()