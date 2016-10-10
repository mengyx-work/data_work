import time
import multiprocessing
from os.path import join, isdir
from os import listdir
import pandas as pd
import numpy as np


def split_large_csv_file(file_path, data_file):
    file_counter = 0
    row_counter = 0
    file_row_limit = 50000
    output_file = None
    start_time = time.time()

    with open(join(file_path, data_file)) as f:
        header = f.readline()
        for line in f:

            if row_counter % file_row_limit == 0:
                if output_file is not None:
                    output_file.close()
                file_name = data_file.split('.')[0]
                file_format = data_file.split('.')[1]
                output_file_name = '{}_split_{}.{}'.format(file_name, file_counter, file_format)
                file_counter += 1
                output_file = open(output_file_name, 'w')
                output_file.write(header)
                print '{} lines had been processed using {} seconds.'.format(row_counter, round((time.time() - start_time), 0))
            output_file.write(line)
            row_counter += 1


def read_csv_files_by_columns(argument):
    data_file, columns, file_path = argument
    data = pd.read_csv(join(file_path, data_file), usecols=columns)
    return data



def read_columns_in_parallel(data_path, columns):

    if len(columns) == 0:
        raise ValueError('columns are empty...')

    data_files   = listdir(data_path)
    data_files   = [f for f in data_files if '.csv' in f]
    columns_list = [columns for f in data_files]
    path_list    = [data_path for f in data_files]

    start_time = time.time()
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    results = pool.map(read_csv_files_by_columns, zip(data_files, columns_list, path_list))
    print 'loading all the files using {} seconds'.format(round((time.time() - start_time), 2))

    combined_data = pd.concat(results, axis=0)
    return combined_data



class load_data(object):

    def __init__(self, file_path, data_format='.csv', columns=None):

        if not isdir(file_path):
            raise ValueError('the file path {} does not exists!'.format(file_path))

        files = listdir(file_path)
        self.file_path = file_path
        self.data_files = [f for f in files if data_format in f]

        if len(self.data_files) == 0:
            raise ValueError('failed to find any data file in format {} from {}'.format(data_format, file_path)) 

        if columns is not None:
            self._check_data_columns(columns)

    @classmethod
    def _read_csv_file_by_columns(self, data_file):
        data = pd.read_csv(join(self.file_path, data_file), usecols=self.columns)
        return data


    def _combine_dataFrames(self, results): 
        combined_data = pd.concat(results, axis=0)
        return combined_data


    def _check_data_columns(self, columns=None):

        if columns is not None:
            self.columns = columns
        else:
            self.columns = None

        for csv_file in self.data_files:
            columns = pd.read_csv(join(self.file_path, csv_file), nrow=0)

            if self.columns is None:
                self.columns = columns

            if self.columns != columns:
                raise ValueError('different columns are found from file {}'.format(data_file))

           
