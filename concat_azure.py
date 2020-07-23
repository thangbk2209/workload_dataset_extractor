import os
from pandas import read_csv
import numpy as np


INPUT_DIR = '/root/thangbk2209/thesis/datasets/azure/'
results_folder_path = '/root/thangbk2209/thesis/datasets/azure_resource'

azure_data = None

for file_name in os.listdir(results_folder_path):
    print('===: %s%s' % (results_folder_path, file_name))
    file_path = f'{results_folder_path}/{file_name}'
    colnames = ['cpu', 'timestamp']
    usecols = [0, 1]
    azure_df = read_csv(
        file_path, header=None, index_col=False, names=colnames, usecols=usecols, engine='python')
    _azure_data = azure_df.values.reshape(-1, 1)
    
    if azure_data is None:
        azure_data = _azure_data
    else:
        print(azure_data.shape)
        print(_azure_data.shape)
        azure_data = np.append(azure_data, _azure_data, axis=1)
print(azure_data.shape)