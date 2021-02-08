# Mimic III Preprocessing
This repository contains the source code necessary to pre-process the MIMIC-III data to reproduce the results in the paper "Learning Representations of Missing Data for Predicting Patient Outcomes".

# Installation

The package can be installed with pip.

```
pip3 install .
```

# Usage

The following steps must be performed in the order shown. Please update the
paths in the command line examples with the appropriate config file path.

In all cases, the `--help` flag can be used to show complete command line
options.

1. **Download and pre-process MIMIC-III**

    This code requires the MIMIC-III dataset to be preprocessed by the scripts
    provided by Harutyunyan et al. Please follow the relevant instructions
    to acquire access and prepare the data.

    * MIMIC-III: https://mimic.physionet.org/
    * Harutyunyan et al: https://github.com/YerevaNN/mimic3-benchmarks. These
      scripts only use the `in-hospital-mortality` benchmark-specific files.

2. **Create a configuration yaml file**

    All of the scripts in this package use a single configuration file to
    determine relevant parameters. Please see `etc/config.yaml` for an
    example of the file. The config file requires the following values:

    *Existing files and paths*

    * `mimic_basepath`. The path to the folder which contains the MIMIC-III csv
      files
    * `benchmark_base`. The path to the folder which contains the output of the
      scripts from Harutyunyan *et al.*
    * `all_stays`. The complete path to the `all_stays.csv` file created by the
      scripts from Harutyunyan *et al.*

    *Files created by the pipeline*

    * `analysis_basepath`. Various intermediate files for text processing will
      be created in this folder.
    * `complete_listfile`. A (csv) file containing complete path information for
      all episodes
    * `time_series_features`. A (csv) file containing processed time series
      features about all episodes
    * `extended_episodes`. A (csv) file containing details such as demographics
      and time series features for all episodes
    * `complete_episodes`. A (joblib) archive file containing all features for
      all episodes

    *Other options*

    * `min_df`. The minimum document frequency to retain tokens in text
      processing. Please see [CountVectorizer](https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html) for the interpretation of
      this value.
    * `max_df`. The maximum document frequency to retain tokens in text
      processing. Please see [CountVectorizer](https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.html) for the interpretation of
      this value.

3. **Create the extended listfile**

    ```
    create-extended-listfile etc/config.yaml --logging-level INFO
    ```

4. **Extract the time series features**

    ```
    extract-mimic-time-series-features etc/config.yaml --logging-level INFO
    ```

5. **Create the extended dataset**

    ```
    create-extended-mimic-dataset etc/config.yaml --logging-level INFO
    ```

6. **Add the bag-of-words features to the dataset**

    ```
    create-mimic-notes-bow etc/config.yaml --logging-level INFO
    ```

7. **Load the complete dataset**

    The final, complete dataset is saved as a joblib archive file. It can be
    loaded in downstream python scripts (notebooks, etc.) using code similar
    to the following.

    ```python
    import joblib
    df_complete_episodes = joblib.load("path/to/all-episodes.complete-dataset.jpkl")
    ```

# Structure of dataframe

The final data frame contains the following fields:

*Fields from the `ADMISSIONS`, `PATIENTS`, and other tables from MIMIC-III*

* `SUBJECT_ID`
* `HADM_ID`
* `EPISODE`
* `EPISODE_NAME`
* `stay`
* `ICUSTAY_ID`
* `LAST_CAREUNIT`
* `DBSOURCE`
* `INTIME`
* `OUTTIME`
* `LOS`
* `ADMITTIME`
* `DISCHTIME`
* `DEATHTIME`
* `ETHNICITY`
* `DIAGNOSIS`
* `GENDER`
* `DOB`
* `DOD`
* `AGE`
* `MORTALITY_INUNIT`
* `MORTALITY`
* `MORTALITY_INHOSPITAL`
* `ADMISSION_LOCATION`
* `ADMISSION_TYPE`
* `DISCHARGE_LOCATION`
* `EDOUTTIME`
* `EDREGTIME`
* `HAS_CHARTEVENTS_DATA`
* `HOSPITAL_EXPIRE_FLAG`
* `INSURANCE`
* `LANGUAGE`
* `MARITAL_STATUS`
* `RELIGION`
* `HEIGHT`
* `WEIGHT`

*Machine learning fields*
* `SPLIT`. The split to which this episode belongs.

*ICD diagnosis fields*

* `DIAGNOSIS_ICD9_<icd>`. Whether the episode included the relevant ICD code at
  discharge

*Time series features*

Example: `Capillary refill rate__0.00-12.00__COUNT`

These fields are of the form:

```
<vital signal>__<start time>-<end time>__<feature>
```

* `vital signal`. example: `Capillary refill rate`
* `start time` (in hours). example: `0.00`
* `end time` (in hours). example: `12.00`
* `feature`. example: `COUNT`

The value for the field is `np.nan` when enough observations are not available
in the indicated time window for the respective vital signal.

*Text features*

Example: `NOTE_NURSING_BOW`

These fields are of the form:

```
NOTE_<type>_BOW
```

`type` indicates the type of the note.

The values for each of these fields is a list which contains the base-1
index of each token which appeared in the appropriate type of note within the
first 48 hours of the respective episode.

The reverse mapping from the token index to the original token can be extracted
from the count vectorizer object stored as a joblib archive at the path:
`os.path.join(config['analysis_basepath'], 'processed-note-events', 'notes-bow-count-vectorizer.jpkl.gz')`

# Citations

If you find this work useful, please cite it. Additionally, please make sure to
cite the original MIMIC-III paper and the work of Harutyunyan *et al.*

This work

```
@Misc{Malone2018,
  author    = {Brandon Malone and Alberto Garc{\'i}a-Dur{\'a}n and Mathias Niepert},
  note      = {arXiv:1811.04752 [cs.LG]},
  title     = {Learning Representations of Missing Data for Predicting Patient Outcomes},
  year      = {2018},
}
```


The pre-processing scripts of Harutyunyan *et al.*

```
@Article{Harutyunyan2019,
  author    = {Hrayr Harutyunyan and Hrant Khachatrian and David C. Kale and Aram Galstyan},
  title     = {Multitask Learning and Benchmarking with Clinical Time Series Data},
  journal   = {Scientific Data},
  year      = {2019},
  volume    = {6},
  number    = {96},
}
```

The MIMIC-III dataset.

```
@Article{Johnson2016,
  author    = {Alistair E.W. Johnson and Tom J. Pollard and Lu Shen and Li-wei H. Lehman and Mengling Feng and Mohammad Ghassemi and Benjamin Moody and Peter Szolovits and Leo Anthony Celi and Roger G. Mark},
  title     = {{MIMIC-III}, a freely accessible critical care database},
  journal   = {Scientific Data},
  year      = {2016},
  volume    = {3},
  number    = {160035},
}
```