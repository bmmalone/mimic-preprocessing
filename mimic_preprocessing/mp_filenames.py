""" This module contains helpers to construct filenames.
"""
import os
import sys

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
# The actual filenames now
###
def get_benchmark_record_filename(benchmark_base, benchmark_problem, split,
        subject_id, episode_id, compression_type="gz", note=None):
    """ Get the complete path for the file containing all features for the
    given instance.

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
    record_file: string
        The path to the record file
    """
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
        benchmark_base,
        benchmark_problem,
        split,
        "complete-records",
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
        config['mimic_basepath'],
        subject_id,
        hadm_id,
        row_id,
        note=note
    )
    
    return f