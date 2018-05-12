import os
from multiprocessing import Pool, cpu_count
import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error

def load_data(data_path):
    train = pd.read_csv(os.path.join(data_path, 'train.csv'), parse_dates=['activation_date'])
    #test = pd.read_csv(os.path.join(data_path, 'test.csv'), parse_dates=['activation_date'])
    return train, None

def parallel_df(df, func, num_cores=None, num_partitions=None):
    if num_cores is None:
        num_cores = cpu_count()
    if num_partitions is None:
        num_partitions = cpu_count()
        
    df_split = np.array_split(df, num_partitions)
    pool = Pool(processes=num_cores)
    try:
        df = pd.concat(pool.map(func, df_split))
        pool.close()
    except BaseException:
        pool.terminate()
        raise
    finally:
        pool.join()
    return df

def parallel_col_df(df, func, num_cores=None):    
    if num_cores is None:
        num_cores = cpu_count()
        
    #df = pd.DataFrame(in_df)
    m = df.shape[1]
    df_col_split = [df[:, i] for i in range(m)]
    pool = Pool(processes=num_cores)
    try:
        for i, res in enumerate(pool.map(func, df_col_split)):
            df[:, i] = res
        pool.close()
    except BaseException:
        pool.terminate()
        raise
    finally:
        pool.join()
        
    return df

def save_df(df, data_path, file_prefix):
    df.to_csv(os.path.join(data_path, '%s.csv' % file_prefix), index_label=False)

def load_df(data_path, file_prefix):
    return pd.read_csv(os.path.join(data_path, '%s.csv' % file_prefix))
    
def rmse(y_pred, y_true):
    return np.sqrt(mean_squared_error(y_pred, y_true))

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]