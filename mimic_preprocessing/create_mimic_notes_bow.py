""" Convert the notes in the NOTEEVENTS table to categorized bag-of-word
representations for each episode.

**N.B.** This processing includes one filtering step (Step 4) which is specific for
the 2-day prediction task.

This entails five steps.

    1. Remove stop words, stem, etc., each note. This does not require any
       synchronization steps.

    2. Create a CountVectorizer across all of the documents which converts
       tokens to indices.

    3. Transform each note to a bag of words.

    4. Combine the bags of words for each category and each episode. This step
       also filters notes which occur more than args.episode_length after the
       annotated beginning of the episode.

    5. Convert the combined representation to a sparse (row) matrix for each
       episode. This step also joins all of the notes for each episode and
       filters those which occur.

The output is a data frame with one column giving the HADM_ID and the rest with
the concatenated bags of words for each note category.

Please note, this script was compiled from multiple jupyter notebooks, so it is
not particularly efficient. Apologies in advance.
"""
import logging
import pyllars.logging_utils as logging_utils
logger = logging.getLogger(__name__)

import argparse
import joblib
import pandas as pd

import pyllars.collection_utils as collection_utils
import pyllars.nlp_utils as nlp_utils
import pyllars.pandas_utils as pd_utils
import pyllars.physionet_utils as physionet_utils
import pyllars.shell_utils as shell_utils

from pyllars.sklearn_transformers.incremental_count_vectorizer import IncrementalCountVectorizer

import mimic_preprocessing.mp_filenames as mp_filenames

# we only need the identifiers, not the actual text, since we will load that
COUNT_VECTORIZER_COLS = [
    "ROW_ID",
    "SUBJECT_ID",
    "HADM_ID",
    "CHARTDATE",
    "CHARTTIME",
    "STORETIME",
    "CATEGORY",
    "DESCRIPTION",
    "CGID",
    "ISERROR"
]

# the fields we will keep around

# those related to the admission
ADMISSION_FIELDS = [
    "ADMISSION_TYPE",
    "ADMISSION_LOCATION",
    "DIAGNOSIS",
]

# and demographics
DEMOGRAPHIC_FIELDS = [
    "ETHNICITY",
    "GENDER",
    "AGE",
    "INSURANCE",
    "MARITAL_STATUS"
]

# and keep around bookkeeping info
BOOKKEEPING_FIELDS = [
    "EPISODE",
    "SUBJECT_ID",
    "HADM_ID",
    "ICUSTAY_ID"
]

# and the various targets we may want
TARGET_FIELDS = [
    "DISCHARGE_LOCATION",
    "LOS",
    "MORTALITY_INHOSPITAL"
]

FIELDS_TO_KEEP = BOOKKEEPING_FIELDS + DEMOGRAPHIC_FIELDS + ADMISSION_FIELDS + TARGET_FIELDS


NOTE_TYPES = [
    "NOTE_NURSING_BOW",
    "NOTE_RADIOLOGY_BOW",
    "NOTE_RESPITORY_BOW",
    "NOTE_ECG_BOW",
    "NOTE_ECHO_BOW",
    "NOTE_OTHER_BOW",
    "NOTE_DISCHARGE_SUMMARY_BOW"
]

NOTE_TYPE_MAPPINGS = {
    "Case Management": "NOTE_OTHER_BOW",
    "Consult": "NOTE_OTHER_BOW",
    "Discharge summary": "NOTE_DISCHARGE_SUMMARY_BOW",
    "ECG": "NOTE_ECG_BOW",
    "Echo": "NOTE_ECHO_BOW",
    "General": "NOTE_NURSING_BOW",
    "Nursing": "NOTE_NURSING_BOW",
    "Nursing/other": "NOTE_NURSING_BOW",
    "Nutrition": "NOTE_OTHER_BOW",
    "Pharmacy": "NOTE_OTHER_BOW",
    "Physician": "NOTE_NURSING_BOW",
    "Radiology": "NOTE_RADIOLOGY_BOW",
    "Rehab Services": "NOTE_OTHER_BOW",
    "Respiratory": "NOTE_RESPITORY_BOW",
    "Social Work": "NOTE_OTHER_BOW"
}

ZERO_DAYS = pd.Timedelta(0, 'D')
TWO_DAYS = pd.Timedelta(2, 'D')

NOTE_CLEANED = "cleaned"
NOTE_CLEANED_BOW = "cleaned-bow"
NOTE_CLEANED_BOW_COMBINED = "cleaned-bow.combined"

###
# Cleaning up the notes
###
def clean_row(row, config):
    
    # only keep notes associated of admissions
    if pd.isnull(row['HADM_ID']):
        return None
    
    if pd.isnull(row['CHARTTIME']):
        row['CHARTTIME'] = row['CHARTDATE']
        
    row['TEXT'] = nlp_utils.clean_doc(row['TEXT'])
    
    subject_id = row['SUBJECT_ID']
    hadm_id = int(row['HADM_ID'])
    row_id = int(row['ROW_ID'])

    f = mp_filenames.get_note_event_filename(
        config['mimic_basepath'],
        subject_id,
        hadm_id,
        row_id,
        note=NOTE_CLEANED
    )
    
    joblib.dump(row, f)

def process_chunk_clean_row(df, config):
    df.apply(clean_row, axis=1, args=(config,))

def clean_notes(df_notes, args, config, client):
    
    num_groups = int(len(df_notes) / args.chunksize)
    g_notes = pd_utils.split_df(df_notes, num_groups)

    _ = dask_utils.apply_groups(
        g_notes,
        client,
        process_chunk_clean_row,
        config,
        progress_bar=True
    )

    return None

###
# Creating the count vectorizer
###
def create_count_vectorizer(df_notes):
    df_notes = df_notes[COUNT_VECTORIZER_COLS]

def get_tokens(file):
    doc = joblib.load(file)
    tokens = doc['TEXT']
    tokens = tokens.split(' ')
    return tokens

def process_chunk_count_vectorizer(df, config):
    
    filenames = df.apply(
        mp_filenames.get_note_event_filename_row,
        axis=1,
        args=(config,NOTE_CLEANED)
    )
    filenames = list(filenames)
    filenames = collection_utils.remove_nones(filenames)
    
    icv = IncrementalCountVectorizer(
        prune=False,
        create_mapping=False,
        get_tokens=get_tokens
    )
    
    icv_fit = icv.fit(filenames)
    
    return icv_fit

def create_count_vectorizer(df_notes, args, config, client):
    num_groups = int(len(df_notes) / args.chunksize)
    g_notes = pd_utils.split_df(df_notes, num_groups)

    # create independent vectorizers for each group
    fit_icvs = dask_utils.apply_groups(
        g_notes,
        client,
        process_chunk_count_vectorizer,
        config,
        progress_bar=True
    )

    # merge them
    icv_fit = IncrementalCountVectorizer.merge(
        fit_icvs,
        min_df=config['min_df'],
        max_df=config['max_df'],
        get_tokens=get_tokens
    )

    # and write to disk
    f = mp_filenames.get_mimic_notes_count_vectorizer_filename(
        config['mimic_basepath']
    )

    joblib.dump(icv_fit, f)

###
# Create BoW with the count vectorizer
###
def transform_row(row, config, icv_fit):
    
    in_fn = mp_filenames.get_note_event_filename_row(row, config, NOTE_CLEANED)
    out_fn = mp_filenames.get_note_event_filename_row(row, config, NOTE_CLEANED_BOW)
    
    if in_fn is None:
        return None
        
    row = joblib.load(in_fn)
    
    row['SUBJECT_ID'] = int(row['SUBJECT_ID'])
    row['HADM_ID'] = int(row['HADM_ID'])
    row['ROW_ID'] = int(row['ROW_ID'])
    
    text = [row['TEXT']]    
    row['TEXT'] = icv_fit.transform(text)
    row['TEXT'] = row['TEXT'][0]
    
    joblib.dump(row, out_fn)

def process_chunk_transform(df, config, icv_fit):
    df.apply(transform_row, axis=1, args=(config, icv_fit))

def create_bow(df_notes, args, config, client):
    num_groups = int(len(df_notes) / args.chunksize)
    g_notes = pd_utils.split_df(df_notes, num_groups)

    f = mp_filenames.get_mimic_notes_count_vectorizer_filename(
        config['mimic_basepath']
    )

    icv_fit_load = joblib.load(f)

    _ = dask_utils.apply_groups(
        g_notes,
        client,
        process_chunk_transform,
        config,
        icv_fit_load,
        progress_bar=True
    )

###
# Create combined BoW
###
def process_group(g, config):
    note_files = g.apply(
        mp_filenames.get_note_event_filename_row,
        axis=1,
        args=(config, NOTE_CLEANED_BOW)
    )
    df_cleaned_notes = [joblib.load(f) for f in note_files]
    df_cleaned_notes = pd.DataFrame(df_cleaned_notes)
    
    subject_id = g.iloc[0]['SUBJECT_ID']
    hadm_id = g.iloc[0]['HADM_ID']
    
    df_cleaned_notes['CHARTTIME'] = pd.to_datetime(df_cleaned_notes['CHARTTIME'])
    
    out_f = mp_filenames.get_note_event_filename(
        config['mimic_basepath'],
        subject_id,
        hadm_id,
        note=NOTE_CLEANED_BOW_COMBINED
    )
    
    joblib.dump(df_cleaned_notes, out_f)

 def process_group_chunk(chunk, config):
    groups = chunk.groupby(['SUBJECT_ID', 'HADM_ID'], as_index=False)
    groups.apply(process_group, config)
    
def combine_episode_notes(df_notes, args, config, client):
    # group all notes by hadm id
    g_notes = pd_utils.group_and_chunk_df(df_notes, 'HADM_ID', args.chunk_size)

    _ = dask_utils.apply_groups(
        g_notes,
        client,
        process_group_chunk,
        args,
        progress_bar=True
    )

    return None

###
# Create the final, combined data record for each episode
###
def update_note_type(bow_row, episode_record):
    note_type = bow_row['CATEGORY'].strip()
    note_type = NOTE_TYPE_MAPPINGS[note_type]
    episode_record[note_type].extend(bow_row['TEXT'])

def add_text(episode_record, df_notes):
    
    # and now only those within the first two days
    m_two_days = df_notes['Hours'] < TWO_DAYS
    df_notes = df_notes[m_two_days]
    
    # then add the remaining notes
    df_notes.apply(update_note_type, axis=1, args=(episode_record,))

def get_final_record(
        row,
        args,
        config,
        return_record=False):
    """ Merge the text with the other data about the episode
    """
    # first, select the record for this episode
    df_episodes = pd.read_csv(row['ALL_EPISODES_FILE'])
    m_episode = df_episodes['EPISODE'] == row['EPISODE']
    episode = df_episodes[m_episode].iloc[0]
    
    subject_id = episode['SUBJECT_ID']
    hadm_id = episode['HADM_ID']
    episode_id = episode['EPISODE']

    # and keep just those fields we want
    episode_record = episode[FIELDS_TO_KEEP]
    
    # next, pull in the time series data
    ts = joblib.load(row['TS_FILE']).iloc[0]
    episode_record = episode_record.append(ts)
    
    # and finally add the text
    
    # create empty list for each type of note
    for nt in NOTE_TYPES:
        episode_record[nt] = []
        
    # extend the lists with the observed notes
    note_events = mp_filenames.get_note_event_filename(
        config['mimic_basepath'],
        subject_id,
        hadm_id,
        note=NOTE_CLEANED_BOW_COMBINED
    )
    
    if os.path.exists(note_events):
        df_notes = joblib.load(note_events)
        
        # fix the datetime data types
        admit_time = pd.to_datetime(episode['ADMITTIME'])
        df_notes['CHARTTIME'] = pd.to_datetime(df_notes['CHARTTIME'])
        df_notes['Hours'] = df_notes['CHARTTIME'] - admit_time
        
        # and add them to the record
        add_text(episode_record, df_notes)

    f = mp_filenames.get_benchmark_record_filename(
        config['benchmark_base'],
        config['benchmark_problem'],
        row['SPLIT'],
        subject_id,
        episode_id
    )
    
    shell_utils = shell_utils.ensure_path_to_file_exists(f)
    joblib.dump(episode_record, f)
    
    ret = None
    if return_record:
        ret = episode_record

    return ret

def process_chunk_final_record(df, args, config):
    pd_utils.apply(df, get_final_record, args, config)


def create_all_combined_records(df_listfile, args, config, client):

    g_listfile = pd_utils.split_df(df_listfile, chunk_size=args.chunksize)

     # this completes
    _ = dask_utils.apply_groups(
        g_listfile,
        client,
        process_chunk,
        args,
        config,
        progress_bar=True
    )

    return None



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

    parser.add_argument('--chunk-size', type=int, default=100, help="The size "
        "of chunks for parallelization")

    parser.add_argument('--num-notes', type=int, default=None, help="The "
        "number of notes to read in. This is mostly for debugging purposes.")

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
    dask_client, dask_cluster = dask_utils.connect(args)

    msg = "Loading notes... this may be slow."
    logger.info(msg)
    df_notes = physionet_utils.get_notes(
        config['mimic_basepath'], nrows=args.num_notes
    )

    msg = "Loading the list file"
    logger.info(msg)
    df_listfile = pd.read_csv(config['episode_listfile'])

    msg = "Cleaning notes"
    logger.info(msg)
    clean_notes(df_notes, args, config, client)

    msg = "Creating count vectorizer for notes"
    logger.info(msg)
    create_count_vectorizer(df_notes, args, config, client)

    msg = "Creating the bag-of-words for the notes"
    logger.info(msg)
    create_bow(df_notes, args, config, client)

    msg = "Combining bag-of-words types per episode"
    logger.info(msg)
    combine_episode_notes(df_notes, args, config, client)

    msg = "Creating the final, combined data frame"
    logger.info(msg)
    create_combined_data_frame(df_listfile, args, config, client)


    #msg = "Writing bag-of-words to disk: '{}'".format(config['episode_notes'])
    #logger.info(msg)
    #joblib.dump(df_episode_notes, config['episode_notes'])

if __name__ == '__main__':
        main()