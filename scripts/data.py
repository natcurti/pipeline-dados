import json, csv

class Data:

    def __init__(self, path, data_type):
        self.path = path
        self.data_type = data_type
        self.data = self.load_data()
        self.columns = self.get_column_names()
        self.data_len = len(self.data)

    def load_json(self):
        json_empresa_A = []
        with open(self.path) as file:
            json_empresa_A = json.load(file)
        return json_empresa_A

    def load_csv(self):
        csv_data = []
        with open(self.path) as file_csv: 
            spamreader = csv.DictReader(file_csv)
            for row in spamreader:
                csv_data.append(row)
        return csv_data

    def load_data(self):
        data = []
        if self.data_type == 'json':
            data = self.load_json()
        elif self.data_type == 'csv':
            data = self.load_csv()
        elif self.data_type == 'list':
            data = self.path
            self.path = 'Lista em memória'
        return data
        
    def get_column_names(self):
        return list(self.data[0].keys())

    def update_column_names(self, mapping):
        new_data = []
        for old_dict in self.data:
            new_dict = {}
            for old_key, value in old_dict.items():
                new_dict[mapping[old_key]] = value
            new_data.append(new_dict)    
        self.data = new_data
    
    def combine_sources(first_source, second_source):
        combined_list = []
        combined_list.extend(first_source.data)
        combined_list.extend(second_source.data)
        return Data(combined_list, 'list')

    def transform_data_to_table(self):
        data_table = [self.columns]

        for row in self.data: 
            new_row = []
            for col in self.columns:
                new_row.append(row.get(col, 'Indisponível'))
            data_table.append(new_row)   
        
        return data_table

    def save_data(self, path):
        
        data_table = self.transform_data_to_table()

        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(data_table)