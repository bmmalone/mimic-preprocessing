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
import os
import pandas as pd

import pyllars.collection_utils as collection_utils
import pyllars.dask_utils as dask_utils
import pyllars.nlp_utils as nlp_utils
import pyllars.pandas_utils as pd_utils
import pyllars.physionet_utils as physionet_utils
import pyllars.shell_utils as shell_utils
import pyllars.utils

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
        config['analysis_basepath'],
        subject_id,
        hadm_id,
        row_id,
        note=NOTE_CLEANED
    )
    
    shell_utils.ensure_path_to_file_exists(f)
    joblib.dump(row, f)

def process_chunk_clean_row(df, config):
    pd_utils.apply(df, clean_row, config)

def clean_notes(df_notes, args, config, client):    
    g_notes = pd_utils.split_df(df_notes, chunk_size=args.chunk_size)

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
    g_notes = pd_utils.split_df(df_notes, chunk_size=args.chunk_size)

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
        config['analysis_basepath']
    )
    
    shell_utils.ensure_path_to_file_exists(f)
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
    
    shell_utils.ensure_path_to_file_exists(out_fn)
    joblib.dump(row, out_fn)

def process_chunk_transform(df, config, icv_fit):
    pd_utils.apply(df, transform_row, config, icv_fit)

def create_bow(df_notes, args, config, client):
    g_notes = pd_utils.split_df(df_notes, chunk_size=args.chunk_size)

    f = mp_filenames.get_mimic_notes_count_vectorizer_filename(
        config['analysis_basepath']
    )

    icv_fit_load = joblib.load(f)
    
    # make sure to erase the function pointer for loading tokens
    icv_fit_load.get_tokens = None

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
    
    subject_id = int(g.iloc[0]['SUBJECT_ID'])
    hadm_id = int(g.iloc[0]['HADM_ID'])
    
    df_cleaned_notes['CHARTTIME'] = pd.to_datetime(df_cleaned_notes['CHARTTIME'])
    
    out_f = mp_filenames.get_note_event_filename(
        config['analysis_basepath'],
        subject_id,
        hadm_id,
        note=NOTE_CLEANED_BOW_COMBINED
    )
    
    shell_utils.ensure_path_to_file_exists(out_f)
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
        config,
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
    subject_id = int(row['SUBJECT_ID'])
    hadm_id = int(row['HADM_ID'])
    episode_id = row['EPISODE']
    
    episode_record = row.copy()
    
    # and add the text
    
    # create empty list for each type of note
    for nt in NOTE_TYPES:
        episode_record[nt] = []
        
    # extend the lists with the observed notes
    note_events = mp_filenames.get_note_event_filename(
        config['analysis_basepath'],
        subject_id,
        hadm_id,
        note=NOTE_CLEANED_BOW_COMBINED
    )
    
    if os.path.exists(note_events):
        df_notes = joblib.load(note_events)
        
        # fix the datetime data types
        admit_time = pd.to_datetime(episode_record['ADMITTIME'])
        df_notes['CHARTTIME'] = pd.to_datetime(df_notes['CHARTTIME'])
        df_notes['Hours'] = df_notes['CHARTTIME'] - admit_time
        
        # and add them to the record
        add_text(episode_record, df_notes)
    else:
        pass

    f = mp_filenames.get_record_filename(
        config['analysis_basepath'],
        row['SPLIT'],
        subject_id,
        episode_id
    )
    
    shell_utils.ensure_path_to_file_exists(f)
    joblib.dump(episode_record, f)
    
    ret = None
    if return_record:
        ret = episode_record

    return ret

def process_chunk_final_record(df, args, config):
    records = pd_utils.apply(
        df, get_final_record, args, config, return_record=True
    )
    df_records = pd.DataFrame(records)
    return df_records
    


def create_all_combined_records(df_episodes, args, config, client):
    g_episodes = pd_utils.split_df(df_episodes, chunk_size=args.chunk_size)

     # this completes
    all_record_dfs = dask_utils.apply_groups(
    #all_record_dfs = pd_utils.apply_groups(
        g_episodes,
        client,    
        process_chunk_final_record,
        args,
        config,
        progress_bar=True
    )
    
    df_all_records = pd.concat(all_record_dfs)
    return df_all_records

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
    client, cluster = dask_utils.connect(args)

    msg = "Loading notes... this may be slow."
    logger.info(msg)
    df_notes = physionet_utils.get_notes(
        config['mimic_basepath'], nrows=args.num_notes
    )
    
    msg = "Removing discharge summaries"
    logger.info(msg)
    
    # We do this because the first ~60k notes are discharge summaries. Since
    # we always discard those, it makes testing difficult.
    m_discharge = df_notes['CATEGORY'] == 'Discharge summary'
    df_notes = df_notes[~m_discharge]

    msg = "Loading the episode information"
    logger.info(msg)
    df_episodes = pd.read_csv(config['extended_episodes'])
    
    msg = ("Filtering the list file to only include subjects for which we have "
        "some notes")
    logger.info(msg)
    subject_ids = set(df_notes['SUBJECT_ID'])
    m_subject_ids = df_episodes['SUBJECT_ID'].isin(subject_ids)
    df_episodes = df_episodes[m_subject_ids]

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
    df_all_records = create_all_combined_records(df_episodes, args, config, client)

    msg = "Writing complete dataset to disk: '{}'".format(config['complete_episodes'])
    logger.info(msg)
    joblib.dump(df_all_records, config['complete_episodes'])

if __name__ == '__main__':
        main()