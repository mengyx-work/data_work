import time
import multiprocessing
from os.path import join, isdir
from os import listdir
import pandas as pd
import numpy as np

class load_data(object):

    def __init__(self, file_path, data_format='.csv', columns=None):

        if not isdir(file_path):
            raise ValueError('the file path {} does not exists!'.format(file_path))

        files = listdir(file_path)
        self.file_path = file_path
        self.data_files = [f for f in files if data_format in f]

        if len(self.data_files) == 0:
            raise ValueError('failed to find any data file in format {} from {}'.format(data_format, file_path)) 

        self._check_data_columns(columns)


    def _read_csv_file_by_columns(self, data_file, columns):
        data = pd.read_csv(join(self.file_path, data_file), usecols=columns)
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


    def read_columns_in_parallel(self, columns, data_file=None): 
        
        if len(columns) == 0:
            raise ValueError('columns are empty...')

        if data_file is not None:
            return self._read_csv_file_by_columns(data_file, columns)

        start_time = time.time()
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        results = pool.map(self._read_csv_file_by_columns, self.data_files)
        print 'loading all the files using {} seconds'.format(round((time.time() - start_time), 2))

        combined_data = self._combine_dataFrames(results)
        return combined_data
            
