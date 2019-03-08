import datetime
import fire
from os import listdir, mkdir
from os.path import isdir, isfile
import pandas as pd
import numpy as np
import shutil
from utils.flatten import flatten_list
from utils.gcp_utility import gcpStorageUtility


def get_folders(path):
    list_ = listdir(path)
    list_ = [path + '/' + dir_ for dir_ in list_ if isdir(path + '/' + dir_)]
    if len(list_) == 0:
        return path
    for i, dir_ in enumerate(list_):
        list_[i] = get_folders(dir_)
    return list_


def split_in_datasets(df, r_train=0.8, r_val=0.1, r_test=0.1):
    m = df.shape[0]
    indices = np.random.permutation(m)
    train_indices = indices[:int(r_train*m)]
    val_indices = indices[int(r_train*m):int((r_train + r_val)*m)]
    test_indices = indices[int((1 - r_test)*m):]
    df.loc[train_indices, ['dataset']] = 'TRAIN'
    df.loc[val_indices, ['dataset']] = 'VALIDATION'
    df.loc[test_indices, ['dataset']] = 'TEST'
    return df


def check_extension(filename, extensions):
    list_extensions = extensions.split(",")
    for ext in list_extensions:
        try:
            if ext.strip() == filename.split(".")[-1].strip():
                return True
        except:
            pass
    return False


def generate_dataset(bucket_name, dataset_local_path, dataset_gcp_name='dataset', min_samples=10, extensions="png,jpg,jpeg"):
    try:
        shutil.rmtree('temp')
    except:
        print('No temporal dir to remove')
    mkdir('temp')

    time_stamp = datetime.datetime.now().strftime("%m%d%Y%H%M%S")

    gcp_storage_utility = gcpStorageUtility(
        credentials_path='keys/api-key.json', bucket_name=bucket_name)

    paths = list(flatten_list(get_folders(dataset_local_path)))
    num_paths = len(paths)
    total_df = pd.DataFrame()

    for k, path in enumerate(paths):
        columns = ['dataset', 'path']
        rel_number_folders = len(dataset_local_path.split('/'))
        abs_number_folders = len(path.split('/'))
        for j in range(abs_number_folders - rel_number_folders):
            columns.append(str(j))

        df = pd.DataFrame(columns=columns)

        files = [filename for filename in listdir(
            path) if isfile(path + '/' + filename) and check_extension(filename, extensions)]
        num_files = len(files)
        if num_files >= min_samples:
            for i, filename in enumerate(files):
                gcp_path = 'uploads/' + \
                    dataset_gcp_name + '/' + time_stamp + '/' + filename

                gcp_path = gcp_path.replace('_', '-')

                gcp_storage_utility.upload_file(
                    file_path=path + '/' + filename, bucket_filename=gcp_path)
                file_bucket_path = 'gs://' + bucket_name + '/' + gcp_path
                print(k, num_paths, i, num_files, path, filename)
                record = ['TRAIN', file_bucket_path]
                classes = path.split('/')
                for i in range(rel_number_folders, abs_number_folders):
                    record.append(classes[i].replace(
                        '-', '_').replace(' ', '_'))
                df.loc[df.shape[0]] = record

            df = split_in_datasets(df, 0.7, 0.2, 0.1)
            total_df = pd.concat([total_df, df], axis=0)

    total_df.to_csv(path_or_buf='temp/' + dataset_gcp_name + '.csv',
                    header=False, index=False)
    gcp_storage_utility.upload_file(file_path='temp/' + dataset_gcp_name + '.csv',
                                    bucket_filename='datasets/' + time_stamp + '/' + dataset_gcp_name + '.csv')


if __name__ == '__main__':
    fire.Fire(generate_dataset)
