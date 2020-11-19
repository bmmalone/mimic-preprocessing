""" This notebooks joins information from the main ADMISSIONS table from MIMIC
with the "stays" and "episodes" information created by the benchmark scripts
from Harutyunyan et al.
"""
import logging
import pyllars.logging_utils as logging_utils
logger = logging.getLogger(__name__)

import argparse
import more_itertools
import numpy as np
import pandas as pd
import pathlib
import tqdm

import pyllars.collection_utils as collection_utils
import pyllars.dask_utils as dask_utils
import pyllars.physionet_utils as physionet_utils
import pyllars.shell_utils as shell_utils
import pyllars.utils

ADMISSIONS_COLS = [
    'ADMISSION_LOCATION',
    'ADMISSION_TYPE',
    'DISCHARGE_LOCATION',
    'EDOUTTIME',
    'EDREGTIME',
    'HAS_CHARTEVENTS_DATA',
    'HOSPITAL_EXPIRE_FLAG',
    'INSURANCE',
    'LANGUAGE',
    'MARITAL_STATUS',
    'RELIGION',
    'HADM_ID' # and join on the HADM_ID
]

EPISODE_RENAME_COLS = {
    'Height': 'HEIGHT',
    'Weight': 'WEIGHT',
    'Icustay': 'ICUSTAY_ID'
}

UNKNOWN_ETHNICITIES = {
    'UNKNOWN/NOT SPECIFIED',
    'PATIENT DECLINED TO ANSWER',
    'UNABLE TO OBTAIN'
}

def parse_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__
    )

    parser.add_argument('config', help="The path to the yaml configuration "
        "file.")

    parser.add_argument('--seed', type=int, default=8675309, help="The seed "
        "for the random number generator")

    parser.add_argument('--chunk-size', type=int, default=100, help="The size "
        "of chunks for parallelization")

    dask_utils.add_dask_options(parser)
    logging_utils.add_logging_options(parser)
    args = parser.parse_args()
    logging_utils.update_logging(args)
    return args

def get_diagnosis_to_replace(episode_path):
    """ Get a mapping from the "Diagnosis <code>" text to "DIAGNOSIS_ICD9_<code>" text
    
    This is a bit of a hack; it just reads in one of the episodes to find all of the
    used codes.
    """
    to_replace = {}
    
    df = pd.read_csv(episode_path)

    for c in df.columns:
        if c.startswith('Diagnosis'):
            c_upper = c.replace("Diagnosis ", "DIAGNOSIS_ICD9_")
            to_replace[c] = c_upper
            
    return to_replace

def load_and_clean_episode(episode, DIAGNOSES_TO_REPLACE, EPISODE_COLS):
    df = pd.read_csv(episode)
    episode_name = pathlib.Path(episode).stem

    df = df.rename(columns=EPISODE_RENAME_COLS)
    df = df.rename(columns=DIAGNOSES_TO_REPLACE)
    df['EPISODE'] = episode_name

    ret = df[EPISODE_COLS]
    
    return ret

def load_and_clean_episodes(episodes, DIAGNOSES_TO_REPLACE, EPISODE_COLS, progress_bar=False):
    
    it = episodes
    if progress_bar:
        it = tqdm.tqdm(it)
    
    all_episodes = [
        load_and_clean_episode(e, DIAGNOSES_TO_REPLACE, EPISODE_COLS)
            for e in it
    ]
    
    return all_episodes


def main():
    args = parse_arguments()
    config = pyllars.utils.load_config(args.config)
    
    msg = "Connecting to dask client"
    logger.info(msg)
    dask_client, dask_cluster = dask_utils.connect(args)

    msg = "Reading the \"stays\" information"
    logger.info(msg)
    df_stays = pd.read_csv(config['all_stays'])

    msg = "Loading the admissions table"
    logger.info(msg)
    df_admissions = physionet_utils.get_admissions(config['mimic_basepath'])

    msg = "Merging stays and admissions"
    logger.info(msg)
    df_extended_data = df_stays.merge(
        df_admissions[ADMISSIONS_COLS], on='HADM_ID'
    )

    msg = ("Loading the \"episode\" information. N.B. This is not efficient. "
        "The expected time is around an hour if a single processor is used. "
        "Please see the --num-procs and --cluster-location command line "
        "options for using more processors or a dask cluster.")
    logger.info(msg)
    episode_path = pathlib.Path(config['mimic_basepath'])
    episode_path = episode_path / "benchmarks" / "train" / "12741" / "episode1.csv"
        
    DIAGNOSES_TO_REPLACE = get_diagnosis_to_replace(episode_path)

    EPISODE_COLS = (
        list(EPISODE_RENAME_COLS.values()) + 
        list(DIAGNOSES_TO_REPLACE.values()) +
        ['EPISODE']
    )

    with open(config['all_episodes_list']) as in_f:
        all_episode_files = [l.strip() for l in in_f.readlines()]

    it = more_itertools.chunked(all_episode_files, args.chunk_size)

    all_episode_df_chunks = dask_utils.apply_iter(
        it,
        dask_client,
        load_and_clean_episodes,
        DIAGNOSES_TO_REPLACE,
        EPISODE_COLS,
        progress_bar=True
    )

    msg = "Joining the \"episode\" information"
    logger.info(msg)

    all_episode_dfs = collection_utils.flatten_lists(all_episode_df_chunks)
    df_episodes = pd.concat(all_episode_dfs)
    df_extended_episodes = df_extended_data.merge(df_episodes, on='ICUSTAY_ID')

    msg = "Cleaning up various \"unknown\" fields"
    logger.info(msg)

    m_unknown = df_extended_episodes['ETHNICITY'].isin(UNKNOWN_ETHNICITIES)
    df_extended_episodes.loc[m_unknown, 'ETHNICITY'] = np.nan

    m_unknown = (df_extended_episodes['ADMISSION_LOCATION'] == '** INFO NOT AVAILABLE **')
    df_extended_episodes.loc[m_unknown, 'ADMISSION_LOCATION'] = np.nan

    m_unknown = df_extended_episodes['MARITAL_STATUS'] ==  'UNKNOWN (DEFAULT)'
    df_extended_episodes.loc[m_unknown, 'MARITAL_STATUS'] = np.nan

    msg = "Adding \"episode\" filenames"
    logger.info(msg)
    episode_names = (df_extended_episodes['SUBJECT_ID'].astype(str) + "_" + df_extended_episodes['EPISODE'] + "_timeseries.csv")
    df_extended_episodes['EPISODE_NAME'] = episode_names

    msg = "Writing extended data frame to: '{}'".format(config['all_episodes'])
    logger.info(msg)
    shell_utils.ensure_path_to_file_exists(config['all_episodes'])
    df_extended_data.to_csv(config['all_episodes'], index=False)

if __name__ == '__main__':
    main()
