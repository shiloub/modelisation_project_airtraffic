import json

def create_external_creation_script(project_name, dataset_name, table_name, uris):
    champs = ["fruit", "legumes", "age", "prix"]
    champs_string = ""
    
    for champ in champs:
        champs_string = "" + champs_string + champ + " STRING\n\t"
    
    base = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{project_name}.{dataset_name}.{table_name}`
    (
       {champs_string}
    )
    OPTIONS (
        format = 'CSV',
        uris = [{uris}],
        skip_leading_rows = 1,
        field_delimiter = ',',
        quote = '"',
    );
    """
    return base

def main():
    print(create_external_creation_script("modelisation", "air_traffic_ext", "ACTUAL_EXTERNAL", "gs://monbucket/mon_dossier/*.csv"))
    
if __name__ == "__main__":
    main()