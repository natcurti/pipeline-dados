from data import Data

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'

key_mapping = { 
    'Nome do Item': 'Nome do Produto',
    'Classificação do Produto': 'Categoria do Produto', 
    'Valor em Reais (R$)': 'Preço do Produto (R$)', 
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial', 
    'Data da Venda': 'Data da Venda'
}

dados_csv = Data(path_csv, 'csv')
dados_csv.update_column_names(key_mapping)
print(dados_csv.data_len)

dados_json = Data(path_json, 'json')
print(dados_json.data_len)

dados_combinados = Data.combine_sources(dados_csv, dados_json)
print(dados_combinados.data_len)

dados_combinados.save_data('data_processed/dados_combinados_poo.csv')

