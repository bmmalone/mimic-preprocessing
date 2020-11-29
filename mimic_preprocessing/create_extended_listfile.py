""" Create a list file which includes additional filenames needed to construct
the extended MIMIC dataset.
"""
import logging
import pyllars.logging_utils as logging_utils
logger = logging.getLogger(__name__)

import argparse
import os
import pandas as pd
import pyllars.dask_utils as dask_utils
import pyllars.pandas_utils as pd_utils
import pyllars.utils

import mimic_preprocessing.mp_filenames as mp_filenames

# This work is entirely based on predictions at 48 hours. Thus, the in-hospital
# mortality problem should always be used for building the dataset.
BENCHMARK_PROBLEM = "in-hospital-mortality"

def get_complete_stay_file(row, benchmark_base, benchmark_split):
    f = mp_filenames.get_benchmark_stays_filename(
        benchmark_base,
        benchmark_split,
        str(row['SUBJECT_ID'])
    )
    return f

def get_complete_episode_file(row, benchmark_base, benchmark_split):
    f = mp_filenames.get_benchmark_single_episode_filename(
        benchmark_base,
        benchmark_split,
        str(row['SUBJECT_ID']),
        row['EPISODE']
    )
    return f

def get_episode_ts_file(row, benchmark_base, benchmark_split):
    f = mp_filenames.get_benchmark_ts_filename(
        benchmark_base,
        BENCHMARK_PROBLEM,
        benchmark_split,
        str(row['SUBJECT_ID']),
        row['EPISODE']
    )
    return f

def get_all_episodes_file(row, benchmark_base, benchmark_split):
    f = mp_filenames.get_benchmark_episodes_filename(
        benchmark_base,
        benchmark_split,
        str(row['SUBJECT_ID'])
    )
    return f

def get_benchmark_listfile(benchmark_base, benchmark_split):
    f = os.path.join(
        benchmark_base,
        BENCHMARK_PROBLEM,
        benchmark_split,
        "listfile.csv"
    )

    return f

def build_listfile(benchmark_base, benchmark_split):
    listfile = get_benchmark_listfile(benchmark_base, benchmark_split)
    msg = "Loading listfile: '{}'".format(listfile)
    logger.info(msg)

    df_listfile = pd.read_csv(listfile)
    split = df_listfile['stay'].str.split("_")

    subject_ids = split.str.get(0)
    df_listfile['SUBJECT_ID'] = subject_ids.astype(int)

    episodes = split.str.get(1)
    df_listfile['EPISODE'] = episodes

    df_listfile['STAYS_FILE'] = pd_utils.apply(
        df_listfile, get_complete_stay_file, benchmark_base, benchmark_split
    )

    df_listfile['EPISODE_FILE'] = pd_utils.apply(
        df_listfile, get_complete_episode_file, benchmark_base, benchmark_split
    )

    df_listfile['ALL_EPISODES_FILE'] = pd_utils.apply(
        df_listfile, get_all_episodes_file, benchmark_base, benchmark_split
    )

    df_listfile['SPLIT'] = benchmark_split

    return df_listfile


###
# The main program
###
def parse_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__
    )

    parser.add_argument('config', help="The path to the yaml configuration "
        "file.")

    logging_utils.add_logging_options(parser)
    args = parser.parse_args()
    logging_utils.update_logging(args)
    return args

def main():
    args = parse_arguments()
    config = pyllars.utils.load_config(args.config)

    benchmark_split = 'train'
    df_train = build_listfile(config['benchmark_base'], benchmark_split)

    benchmark_split = 'test'
    df_test = build_listfile(config['benchmark_base'], benchmark_split)

    df_listfile = pd.concat([df_train, df_test])

    msg = "Writing extended list file: '{}'".format(config['complete_listfile'])
    logger.info(msg)
    df_listfile.to_csv(config['complete_listfile'], index=False)

if __name__ == '__main__':
    main()
