# -*- coding: utf-8 -*-
"""Data loading module.

This module represents the loading functions to extract pubmed data, clinical trials data
and drugs data using hamilton framework paradigm. The function declarations will be run during
the execute phase of the driver

"""
import pyspark.pandas as ps
from utils.utils import _load_csv


def df_drugs(drugs_path: str,
             drugs_mapping: dict) -> ps.DataFrame:
    """load drugs dataframe"""
    return _load_csv(drugs_path, drugs_mapping)


def df_clinical_trials(clinical_trials_path: str,
                       clinical_trials_mapping: dict) -> ps.DataFrame:
    """load clinical trials dataframe"""
    return _load_csv(clinical_trials_path, clinical_trials_mapping)


def df_pubmed(pubmed_path: str,
              pubmed_mapping: dict) -> ps.DataFrame:
    """load pubmed dataframe"""
    return _load_csv(pubmed_path, pubmed_mapping)
