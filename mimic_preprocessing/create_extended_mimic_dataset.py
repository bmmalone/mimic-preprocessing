###
# 
# NAME OF THE PROGRAM THIS FILE BELONGS TO 
#  
# file: mimic-preprocessing
#  
# Authors: Brandon Malone (Brandon.malone@neclab.eu
#               Jun Cheng (jun.cheng@neclab.eu)
# 
# NEC Laboratories Europe GmbH, Copyright (c) 2020, All rights reserved. 
#     THIS HEADER MAY NOT BE EXTRACTED OR MODIFIED IN ANY WAY.
#  
#     PROPRIETARY INFORMATION --- 
# 
# SOFTWARE LICENSE AGREEMENT
# ACADEMIC OR NON-PROFIT ORGANIZATION NONCOMMERCIAL RESEARCH USE ONLY
# BY USING OR DOWNLOADING THE SOFTWARE, YOU ARE AGREEING TO THE TERMS OF THIS LICENSE AGREEMENT.  IF YOU DO NOT AGREE WITH THESE TERMS, YOU MAY NOT USE OR DOWNLOAD THE SOFTWARE.
# 
# This is a license agreement ("Agreement") between your academic institution or non-profit organization or self (called "Licensee" or "You" in this Agreement) and NEC Laboratories Europe GmbH (called "Licensor" in this Agreement).  All rights not specifically granted to you in this Agreement are reserved for Licensor. 
# RESERVATION OF OWNERSHIP AND GRANT OF LICENSE: Licensor retains exclusive ownership of any copy of the Software (as defined below) licensed under this Agreement and hereby grants to Licensee a personal, non-exclusive, non-transferable license to use the Software for noncommercial research purposes, without the right to sublicense, pursuant to the terms and conditions of this Agreement. NO EXPRESS OR IMPLIED LICENSES TO ANY OF LICENSOR’S PATENT RIGHTS ARE GRANTED BY THIS LICENSE. As used in this Agreement, the term "Software" means (i) the actual copy of all or any portion of code for program routines made accessible to Licensee by Licensor pursuant to this Agreement, inclusive of backups, updates, and/or merged copies permitted hereunder or subsequently supplied by Licensor,  including all or any file structures, programming instructions, user interfaces and screen formats and sequences as well as any and all documentation and instructions related to it, and (ii) all or any derivatives and/or modifications created or made by You to any of the items specified in (i).
# CONFIDENTIALITY/PUBLICATIONS: Licensee acknowledges that the Software is proprietary to Licensor, and as such, Licensee agrees to receive all such materials and to use the Software only in accordance with the terms of this Agreement.  Licensee agrees to use reasonable effort to protect the Software from unauthorized use, reproduction, distribution, or publication. All publication materials mentioning features or use of this software must explicitly include an acknowledgement the software was developed by NEC Laboratories Europe GmbH.
# COPYRIGHT: The Software is owned by Licensor.  
# PERMITTED USES:  The Software may be used for your own noncommercial internal research purposes. You understand and agree that Licensor is not obligated to implement any suggestions and/or feedback you might provide regarding the Software, but to the extent Licensor does so, you are not entitled to any compensation related thereto.
# DERIVATIVES: You may create derivatives of or make modifications to the Software, however, You agree that all and any such derivatives and modifications will be owned by Licensor and become a part of the Software licensed to You under this Agreement.  You may only use such derivatives and modifications for your own noncommercial internal research purposes, and you may not otherwise use, distribute or copy such derivatives and modifications in violation of this Agreement.
# BACKUPS:  If Licensee is an organization, it may make that number of copies of the Software necessary for internal noncommercial use at a single site within its organization provided that all information appearing in or on the original labels, including the copyright and trademark notices are copied onto the labels of the copies.
# USES NOT PERMITTED:  You may not distribute, copy or use the Software except as explicitly permitted herein. Licensee has not been granted any trademark license as part of this Agreement. Neither the name of NEC Laboratories Europe GmbH nor the names of its contributors may be used to endorse or promote products derived from this Software without specific prior written permission.
# You may not sell, rent, lease, sublicense, lend, time-share or transfer, in whole or in part, or provide third parties access to prior or present versions (or any parts thereof) of the Software.
# ASSIGNMENT: You may not assign this Agreement or your rights hereunder without the prior written consent of Licensor. Any attempted assignment without such consent shall be null and void.
# TERM: The term of the license granted by this Agreement is from Licensee's acceptance of this Agreement by downloading the Software or by using the Software until terminated as provided below.
# The Agreement automatically terminates without notice if you fail to comply with any provision of this Agreement.  Licensee may terminate this Agreement by ceasing using the Software.  Upon any termination of this Agreement, Licensee will delete any and all copies of the Software. You agree that all provisions which operate to protect the proprietary rights of Licensor shall remain in force should breach occur and that the obligation of confidentiality described in this Agreement is binding in perpetuity and, as such, survives the term of the Agreement.
# FEE: Provided Licensee abides completely by the terms and conditions of this Agreement, there is no fee due to Licensor for Licensee's use of the Software in accordance with this Agreement.
# DISCLAIMER OF WARRANTIES:  THE SOFTWARE IS PROVIDED "AS-IS" WITHOUT WARRANTY OF ANY KIND INCLUDING ANY WARRANTIES OF PERFORMANCE OR MERCHANTABILITY OR FITNESS FOR A PARTICULAR USE OR PURPOSE OR OF NON-INFRINGEMENT.  LICENSEE BEARS ALL RISK RELATING TO QUALITY AND PERFORMANCE OF THE SOFTWARE AND RELATED MATERIALS.
# SUPPORT AND MAINTENANCE: No Software support or training by the Licensor is provided as part of this Agreement.  
# EXCLUSIVE REMEDY AND LIMITATION OF LIABILITY: To the maximum extent permitted under applicable law, Licensor shall not be liable for direct, indirect, special, incidental, or consequential damages or lost profits related to Licensee's use of and/or inability to use the Software, even if Licensor is advised of the possibility of such damage.
# EXPORT REGULATION: Licensee agrees to comply with any and all applicable export control laws, regulations, and/or other laws related to embargoes and sanction programs administered by law.
# SEVERABILITY: If any provision(s) of this Agreement shall be held to be invalid, illegal, or unenforceable by a court or other tribunal of competent jurisdiction, the validity, legality and enforceability of the remaining provisions shall not in any way be affected or impaired thereby.
# NO IMPLIED WAIVERS: No failure or delay by Licensor in enforcing any right or remedy under this Agreement shall be construed as a waiver of any future or other exercise of such right or remedy by Licensor.
# GOVERNING LAW: This Agreement shall be construed and enforced in accordance with the laws of Germany without reference to conflict of laws principles.  You consent to the personal jurisdiction of the courts of this country and waive their rights to venue outside of Germany.
# ENTIRE AGREEMENT AND AMENDMENTS: This Agreement constitutes the sole and entire agreement between Licensee and Licensor as to the matter set forth herein and supersedes any previous agreements, understandings, and arrangements between the parties relating hereto.
###
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

    df_list = pd.read_csv(config['complete_listfile'])
    all_episode_files = df_list['EPISODE_FILE'].values.tolist()

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
    
    msg = "Adding the dataset split"
    logger.info(msg)
    on = ['SUBJECT_ID', 'EPISODE']
    cols = on + ['SPLIT']
    df_extended_episodes = df_extended_episodes.merge(df_list[cols], on=on)
    

    msg = "Adding time series features: '{}'".format(config['time_series_features'])
    logger.info(msg)
    df_time_series = pd.read_csv(config['time_series_features'])
    on = ['SUBJECT_ID', 'EPISODE']
    df_extended_episodes = df_extended_episodes.merge(df_time_series, on=on)


    msg = "Writing extended data frame to: '{}'".format(config['all_episodes'])
    logger.info(msg)
    shell_utils.ensure_path_to_file_exists(config['all_episodes'])
    df_extended_episodes.to_csv(config['all_episodes'], index=False)

if __name__ == '__main__':
    main()
