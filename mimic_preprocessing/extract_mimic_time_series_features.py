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
""" Create the hand-crafted time series features described in Harutyunyan et al.
"""
import logging
import pyllars.logging_utils as logging_utils
logger = logging.getLogger(__name__)

import argparse
import numpy as np
import pandas as pd
import pyllars.dask_utils as dask_utils
import pyllars.pandas_utils as pd_utils
import pyllars.shell_utils as shell_utils
import pyllars.utils
import toolz.dicttoolz

import mimic_preprocessing.mp_filenames as mp_filenames

###
# Load the raw time series data
###
def get_stacked_df(row, config):
    
    subject_id = row['SUBJECT_ID']
    episode_id = row['EPISODE']
    stay = row['stay']
    
    ts_path = mp_filenames.get_benchmark_ts_raw_filename(
        benchmark_base=config['benchmark_base'],
        split=row['SPLIT'],
        subject_id=row['SUBJECT_ID'],
        episode_id=row['EPISODE']
    )
    
    
    df = pd.read_csv(ts_path)
    df = pd.melt(df, id_vars=['Hours'], var_name="kind", value_name="value")
    df = df.dropna(subset=['value'])
    df = df.reset_index(drop=True)
    df['stay'] = stay
    df['SUBJECT_ID'] = subject_id
    df['EPISODE'] = episode_id
    return df


def process_chunk(df, config):
    df_chunk = pd_utils.apply(df, get_stacked_df, config)
    df_chunk = pd.concat(df_chunk)
    return df_chunk

def load_raw_ts_data(df_listfile, args, config, client) -> pd.DataFrame:
    chunks = pd_utils.split_df(df_listfile, chunk_size=args.chunk_size)
    stacked_dfs = dask_utils.apply_groups(
        chunks,
        client,
        process_chunk,
        config,
        progress_bar=True
    )

    stacked_df = pd.concat(stacked_dfs)
    stacked_df = stacked_df.reset_index(drop=True)

    return stacked_df

###
# Cleaning up the text fields
###
EYE_MAPPING = {   
    '1 No Response': 1,
    '2 To pain': 2,
    '3 To speech': 3,
    '4 Spontaneously': 4,
    'None': 1,
    'Spontaneously': 4,
    'To Pain': 2,
    'To Speech': 3
}

MOTOR_MAPPING = {
    '1 No Response': 1,
    '2 Abnorm extensn': 2,
    '3 Abnorm flexion': 3,
    '4 Flex-withdraws': 4,
    '5 Localizes Pain': 5,
    '6 Obeys Commands': 6,
    'Abnormal Flexion': 3,
    'Abnormal extension': 2,
    'Flex-withdraws': 4,
    'Localizes Pain': 5,
    'No response': 1,
    'Obeys Commands': 6
}

VERBAL_MAPPING = {
    '1 No Response': 1,
    '1.0 ET/Trach': 1,
    '2 Incomp sounds': 2,
    '3 Inapprop words': 3,
    '4 Confused': 4,
    '5 Oriented': 5,
    'Confused': 4,
    'Inappropriate Words': 3,
    'Incomprehensible sounds': 2,
    'No Response': 1,
    'No Response-ETT': 1,
    'Oriented': 5
}
    
def update_values(stacked_df, kind, mapping):
    m = stacked_df['kind'] == kind
    vals = stacked_df.loc[m, 'value']
    mapped_vals = vals.map(mapping)
    stacked_df.loc[m, 'value'] = mapped_vals

def clean_text_fields(df_ts_data) -> pd.DataFrame:
    kind = 'Glascow coma scale eye opening'
    update_values(df_ts_data, kind, EYE_MAPPING)

    kind = 'Glascow coma scale motor response'
    update_values(df_ts_data, kind, MOTOR_MAPPING)

    kind = 'Glascow coma scale verbal response'
    update_values(df_ts_data, kind, VERBAL_MAPPING)

    df_ts_data['num_value'] = pd.to_numeric(df_ts_data['value'])

    return df_ts_data

###
# Feature extraction
###
FEATURE_EXTRACTORS = {
    "KURTOSIS": pd.Series.kurt,
    "MAX_ABSOLUTE_DEVIATION": pd.Series.mad,
    "MAX": pd.Series.max,
    "MEAN": pd.Series.mean,
    "MEDIAN": pd.Series.median,
    "MIN": pd.Series.min,
    "SKEW": pd.Series.skew,
    "STD": pd.Series.std,
    "COUNT": pd.Series.count
}


# these are the fractions of the sequence for which features will be extracted
SUBSEQUENCES = np.array([
    (0,1),
    (0,0.1),
    (0,0.25),
    (0,0.5),
    (0.5,1),
    (0.75,1),
    (0.9,1)
])

PERIOD_LENGTH = 48 # hours

SUBSEQUENCE_TIMEPOINTS = SUBSEQUENCES * PERIOD_LENGTH

def extract_time_series_features(
        df_time_series,
        subsequence_timepoints,
        name_column='kind',
        time_column='Hours',
        value_column='num_value'):
    
    time_series_name = df_time_series.iloc[0][name_column]
    ret = {}

    for (start, end) in subsequence_timepoints:
        prefix = "{}__{:.2f}-{:.2f}".format(time_series_name, start, end)

        m_start = df_time_series[time_column] >= start
        m_end = df_time_series[time_column] <= end
        m_valid = m_start & m_end
        values = df_time_series.loc[m_valid, value_column]

        r = {
            "{}__{}".format(prefix, name): f(values)
                for name, f in FEATURE_EXTRACTORS.items()
        }

        ret.update(r)
        
    return ret

def extract_episode_time_series_features(
        df_episode,
        subsequence_timepoints,
        name_column='kind'):
    
    subject_id = df_episode.iloc[0]['SUBJECT_ID']
    episode_id = df_episode.iloc[0]['EPISODE']
    stay = df_episode.iloc[0]['stay']
    
    kind_groups = df_episode.groupby(name_column)    
    kind_stats = pd_utils.apply_groups(
        kind_groups,
        extract_time_series_features,
        subsequence_timepoints
    )
    kind_stats = toolz.dicttoolz.merge(*kind_stats)
    
    kind_stats['SUBJECT_ID'] = subject_id
    kind_stats['EPISODE'] = episode_id
    kind_stats['stay'] = stay
    
    return kind_stats

def extract_all_episode_time_series_features(
        df_episodes,
        subsequence_timepoints,
        name_column='kind'):
    
    episode_groups = df_episodes.groupby('stay')
    all_ts_features = pd_utils.apply_groups(
        episode_groups,
        extract_episode_time_series_features,
        subsequence_timepoints,
        name_column
    )
    
    df_ts_features = pd.DataFrame(all_ts_features)
    return df_ts_features

def extract_all_time_series_features(df_ts_data, args, dask_client,
        subsequence_timepoints=SUBSEQUENCE_TIMEPOINTS) -> pd.DataFrame:

    groupby_field = 'stay'
    stay_chunks = pd_utils.group_and_chunk_df(
        df_ts_data, groupby_field, args.chunk_size
    )

    all_ts_features = dask_utils.apply_groups(
        stay_chunks,
        dask_client,
        extract_all_episode_time_series_features,
        subsequence_timepoints,
        progress_bar=True,
        return_futures=False
    )

    df_all_ts_features = pd.concat(all_ts_features)

    return df_all_ts_features


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

    parser.add_argument('--chunk-size', type=int, default=10, help="The size "
        "of chunks for parallelization")

    parser.add_argument('--num-episodes', type=int, default=None, help="The "
        "number of episodes to process. Omit this argument to process all "
        "episodes. This is mostly intended for debugging.")

    dask_utils.add_dask_options(parser)
    logging_utils.add_logging_options(parser)
    args = parser.parse_args()
    logging_utils.update_logging(args)
    return args

def main():
    args = parse_arguments()
    config = pyllars.utils.load_config(args.config)
    
    msg = "Connecting to dask client"
    logger.info(msg)
    client, server = dask_utils.connect(args)

    msg = "Loading the list file"
    logger.info(msg)
    df_listfile = pd.read_csv(config['complete_listfile'])

    if args.num_episodes is not None:
        df_listfile = df_listfile.head(args.num_episodes)

    msg = "Loading the raw time series data"
    logger.info(msg)
    df_ts_data = load_raw_ts_data(df_listfile, args, config, client)

    msg = "Cleaning the text time series features"
    logger.info(msg)
    df_ts_data = clean_text_fields(df_ts_data)

    msg = "Extracting hand-crafted time series features"
    logger.info(msg)
    df_all_ts_features = extract_all_time_series_features(df_ts_data, args,
        client, subsequence_timepoints=SUBSEQUENCE_TIMEPOINTS)
    
    msg = "Writing features to disk: '{}'".format(config['time_series_features'])
    logger.info(msg)

    shell_utils.ensure_path_to_file_exists(config['time_series_features'])
    df_all_ts_features.to_csv(config['time_series_features'], index=False)

if __name__ == '__main__':
    main()
