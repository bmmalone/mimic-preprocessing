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