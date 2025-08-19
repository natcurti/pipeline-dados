import json, csv

class Data:

    def __init__(self, data):
        self.data = data
        self.columns = self.__get_column_names()
        self.data_len = len(self.data)

    def __load_json(path):
        json_empresa_A = []
        with open(path) as file:
            json_empresa_A = json.load(file)
        return json_empresa_A

    def __load_csv(path):
        csv_data = []
        with open(path) as file_csv: 
            spamreader = csv.DictReader(file_csv)
            for row in spamreader:
                csv_data.append(row)
        return csv_data

    @classmethod
    def load_data(cls, path, data_type):
        data = []
        if data_type == 'json':
            data = cls.__load_json(path)
        elif data_type == 'csv':
            data = cls.__load_csv(path)
        return cls(data)
        
    def __get_column_names(self):
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
        return Data(combined_list)

    def __transform_data_to_table(self):
        data_table = [self.columns]

        for row in self.data: 
            new_row = []
            for col in self.columns:
                new_row.append(row.get(col, 'Indisponível'))
            data_table.append(new_row)   
        
        return data_table

    def save_data(self, path):
        
        data_table = self.__transform_data_to_table()

        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(data_table)