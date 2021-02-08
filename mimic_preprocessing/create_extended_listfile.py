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
