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
""" This module contains helpers to construct filenames.
"""
import os
import pathlib
import sys

import numpy as np
import pandas as pd

###
# Various helpers to handle missing values
###

def _get_compression_str(compression_type=None):
    compression_str = ""
    if (compression_type is not None) and  (len(compression_type) > 0):
        compression_str = ".{}".format(compression_type)
    return compression_str

def _get_index_str(index=None):
    if index is None:
        ret = ""
    else:
        ret = "-{}".format(index)
        
    return ret

def _get_is_input_str(is_input):
    ret = "-target"
    if is_input:
        ret = "-input"
    return ret

def _get_note_str(note=None):
    note_str = ""
    if (note is not None) and  (len(note) > 0):
        note_str = ".{}".format(note)
    return note_str

def _get_row_str(row=None):
    row_str = ""
    if row is not None:
        row_str = "_row-{}".format(row)
    return row_str


def _get_wide_str(wide=False):
    wide_str = ""
    if wide:
        wide_str = ".wide"
    return wide_str

###
# Harutyunyan et al., benchmarks
###

VALID_BENCHMARK_SPLITS = set(["train", "test"])
VALID_BENCHMARK_PROBLEMS = set([
    "decompensation",
    "in-hospital-mortality",
    "length-of-stay",
    "multitask",
    "phenotyping"
])

def _validate_benchmark_problem(benchmark_problem, function_name=None):
    
    if benchmark_problem not in VALID_BENCHMARK_PROBLEMS:
        
        #https://stackoverflow.com/questions/5067604
        if function_name is None:
            function_name = sys._getframe(1).f_code.co_name
            
        msg = ("[{}] invalid problem name: {}. valid "
            "names are: {}".format(function_name, benchmark_problem, 
            VALID_BENCHMARK_PROBLEMS))
        raise ValueError(msg)

def _validate_split(split, function_name=None):
    
    if split not in VALID_BENCHMARK_SPLITS:
        
        #https://stackoverflow.com/questions/5067604
        if function_name is None:
            function_name = sys._getframe(1).f_code.co_name
            
        msg = ("[{}] invalid split name: {}. valid names are: {}".format(
            function_name, split, VALID_BENCHMARK_SPLITS))
        raise ValueError(msg)


###
# The actual filenames now
###
def get_benchmark_episodes_filename(benchmark_base, split,
        subject_id, filetype="csv.gz", note=None):
    """ Get the complete path to the file containing all demographic and
    admission information for the benchmark episodes for the subject

    Parameters
    ----------
    benchmark_base: path-like (e.g., a string)
        The path to the base data directory for the benchmark task

    split: string
        The split. Please see `VALID_BENCHMARK_SPLITS` for
        a list of all valid splits

    subject_id: string
        The identifier for the subject

    filetype: string
        The extension to use for the file. Reasonable values are "parq" and
        "csv.gz"

    note: string or None
        Any additional note to include in the filename. This should probably
        not contain spaces or special characters.

    Returns
    -------
    episode_filename: string
        The path to the episode file
    """
    _validate_split(split)

    fname = [
        "episodes-updated",
        _get_note_str(note),
        ".",
        filetype
    ]

    fname = ''.join(fname)
    fname = os.path.join(benchmark_base, split, str(subject_id), fname)
    return fname

def get_benchmark_ts_raw_filename(benchmark_base, split, subject_id, episode_id):
    """ Get the path to the original time series data created by the
    benchmark script
     
    Parameters
    ----------
    benchmark_base : path-like (e.g., a string)
        The path to the base data directory for the benchmark task

    split : string
        The split. Please see `VALID_BENCHMARK_SPLITS` for
        a list of all valid splits

    subject_id : string
        The identifier for the subject

    episode_id : string
        The identifier for the episode (e.g., "episode1")
    """
    _validate_split(split)
    benchmark_base = pathlib.Path(benchmark_base)
    
    fname = "".join([
        episode_id,
        "_timeseries.csv"
    ])
    
    fname = benchmark_base / split / str(subject_id) / fname
    return fname

def get_benchmark_single_episode_filename(benchmark_base, split, subject_id,
        episode_id, filetype="csv", note=None):
    """ Get the complete path to the file containing all demographic and
    admission information for the benchmark episodes for the subject

    Parameters
    ----------
    benchmark_base: path-like (e.g., a string)
        The path to the base data directory for the benchmark task

    split: string
        The split. Please see `VALID_BENCHMARK_SPLITS` for
        a list of all valid splits

    subject_id: string
        The identifier for the subject

    episode_id: string
        The identifier for the episode (e.g., "episode1")

    filetype: string
        The extension to use for the file. Reasonable values are "parq" and
        "csv.gz"

    note: string or None
        Any additional note to include in the filename. This should probably
        not contain spaces or special characters.

    Returns
    -------
    episode_filename: string
        The path to the episode file
    """
    _validate_split(split)

    fname = [
        str(episode_id),
        _get_note_str(note),
        ".",
        filetype
    ]

    fname = ''.join(fname)
    fname = os.path.join(benchmark_base, split, str(subject_id), fname)
    return fname

def get_benchmark_stays_filename(benchmark_base, split, subject_id,
        filetype="csv.gz", note=None, updated=True):
    """ Get the complete path to the file containing all demographic and
    admission information for the benchmark episodes for the subject

    Parameters
    ----------
    benchmark_base: path-like (e.g., a string)
        The path to the base benchmark data directory

    split: string
        The split. Please see `VALID_BENCHMARK_SPLITS` for
        a list of all valid splits

    subject_id: string
        The identifier for the subject

    filetype: string
        The extension to use for the file. Reasonable values are "parq" and
        "csv.gz"

    note: string or None
        Any additional note to include in the filename. This should probably
        not contain spaces or special characters.
        
    updated: bool
        Whether this is the 

    Returns
    -------
    episode_filename: string
        The path to the stays file
    """
    _validate_split(split)
    
    if updated:
        fname = "stays-updated"
    else:
        fname = "stays"

    fname = [
        fname,
        _get_note_str(note),
        ".",
        filetype
    ]

    fname = ''.join(fname)
    fname = [
        benchmark_base,
        split,
        str(subject_id),
        fname
    ]

    fname = os.path.join(*fname)
    return fname

def get_benchmark_ts_filename(benchmark_base, benchmark_problem, split,
        subject_id, episode_id, compression_type="gz", note=None):
    """ Get the path to a file containing the time series features for the
    indicated subject and episode

    Parameters
    ----------
    benchmark_base: path-like (e.g., a string)
        The path to the base data directory for the benchmark task

    benchmark_problem: string
        The problem of interest. Please see `VALID_BENCHMARK_PROBLEMS` for
        a list of all valid problems.

    split: string
        The split. Please see `VALID_BENCHMARK_SPLITS` for
        a list of all valid splits

    subject_id: string
        The identifier for the subject

    episode_id: string
        The identifier for the episode (e.g., "episode1")

    compression_type: string
        The extension for the type of compression. Please see the joblib docs
        for more details about supported compression types and extensions.

        Use None for no compression.

    note: string or None
        Any additional note to include in the filename. This should probably
        not contain spaces or special characters.

    Returns
    -------
    result_filename: string
        The path to the result file
    """
    _validate_benchmark_problem(benchmark_problem)
    _validate_split(split)

    f = [
        str(subject_id),
        "_",
        str(episode_id),
        "_timeseries",
        _get_note_str(note),
        ".jpkl",
        _get_compression_str(compression_type)
    ]
    
    f = ''.join(f)
    f = [
        benchmark_base,
        benchmark_problem,
        split,
        "ts-features",
        f
    ]
    f = os.path.join(*f)
    return f


def get_mimic_notes_count_vectorizer_filename(base_path):
    """ Get the path to a file containing the sklearn.CountVectorizer for
    the notes.

    Presumably, this CountVectorizer matches the tokens in 
    `get_mimic_notes_bow_filename`.

    Parameters
    ----------
    base_path: path-like (e.g., a string)
        The path to the base data directory

    Returns
    -------
    mimic_notes_bow_filename: string
        The path to the BoW file
    """
    fname = [
        "notes-bow-count-vectorizer",
        ".jpkl.gz"
    ]
    fname = ''.join(fname)
    fname = os.path.join(base_path, 'processed-note-events', fname)
    return fname

def get_note_event_filename(base_path, subject_id, hadm_id, row_id=None,
        compression_type="gz", note=None):
    """ Get the path to a processed NOTEEVENT file
    """
    fname = [
        "subject-",
        str(subject_id),
        "_",
        "hadm-",
        str(hadm_id),
        _get_row_str(row_id),
        _get_note_str(note),
        ".jpkl",
        _get_compression_str(compression_type)
    ]

    fname = ''.join(fname)
    fname = os.path.join(base_path, 'processed-note-events', fname)
    return fname

def get_note_event_filename_row(row, config, note):
    
    # only keep notes associated of admissions
    if pd.isnull(row['HADM_ID']):
        return None
    
    subject_id = int(row['SUBJECT_ID'])
    hadm_id = int(row['HADM_ID'])
    row_id = int(row['ROW_ID'])

    f = get_note_event_filename(
        config['analysis_basepath'],
        subject_id,
        hadm_id,
        row_id,
        note=note
    )
    
    return f



def get_record_filename(base_path, split,
        subject_id, episode_id, compression_type="gz", note=None):
    """ Get the complete path for the file containing all features for the
    given instance.

    Parameters
    ----------
    base_path: path-like (e.g., a string)
        The path to the base data directory

    split: string
        The split. Please see `VALID_BENCHMARK_SPLITS` for
        a list of all valid splits

    subject_id: string
        The identifier for the subject

    episode_id: string
        The identifier for the episode (e.g., "episode1")

    compression_type: string
        The extension for the type of compression. Please see the joblib docs
        for more details about supported compression types and extensions.

        Use None for no compression.

    note: string or None
        Any additional note to include in the filename. This should probably
        not contain spaces or special characters.

    Returns
    -------
    record_file: string
        The path to the record file
    """
    _validate_split(split)

    f = [
        str(subject_id),
        "_",
        str(episode_id),
        _get_note_str(note),
        ".jpkl",
        _get_compression_str(compression_type)        
    ]

    f = ''.join(f)

    f = [
        base_path,
        split,
        "complete-records",
        f
    ]

    f = os.path.join(*f)
    return f