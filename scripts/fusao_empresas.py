import json
import csv

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

def load_json(path_json):
    json_empresa_A = []

    with open(path_json) as file:
        json_empresa_A = json.load(file)

    return json_empresa_A

def load_csv(path_csv):
    csv_data = []
    with open(path_csv) as file_csv: 
        spamreader = csv.DictReader(file_csv)
        for row in spamreader:
            csv_data.append(row)
    return csv_data

def load_data(path, type):
    data = []

    if type == 'json':
        data = load_json(path)
    elif type == 'csv':
        data = load_csv(path)
    return data

def get_column_names(data):
    return list(data[0].keys())

def update_column_names(data, mapping):
    new_data = []
    for old_dict in data:
        new_dict = {}
        for old_key, value in old_dict.items():
            new_dict[mapping[old_key]] = value
        new_data.append(new_dict)    
    return new_data

def combine_sources(first_source, second_source):
    combined_list = []
    combined_list.extend(first_source)
    combined_list.extend(second_source)
    return combined_list

def validate_size_data(combined_list, first_source, second_source):
    if len(combined_list) == len(first_source) + len(second_source):
        return True
    else:
        return False

def validate_columns_data(combined_list, second_source):
    if get_column_names(combined_list) == get_column_names(second_source):
        return True
    else:
        return False

# Carregamento dos dados: 

data_json = load_data(path_json, 'json')
data_csv = load_data(path_csv, 'csv')

# Transformação dos dados: 

key_mapping = { 
    'Nome do Item': 'Nome do Produto',
    'Classificação do Produto': 'Categoria do Produto', 
    'Valor em Reais (R$)': 'Preço do Produto (R$)', 
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial', 
    'Data da Venda': 'Data da Venda'
}

columns_json = get_column_names(data_json)
columns_csv = get_column_names(data_csv)

new_data_csv = update_column_names(data_csv, key_mapping)

combined_list = combine_sources(data_json, new_data_csv)

# Resultados:

print(f"Nome das colunas JSON: {columns_json}")
print(f"Nome das colunas CSV: {columns_csv}")
print(f"Colunas CSV atualizadas: {new_data_csv[0]}")


