# -*- coding: utf-8 -*-
"""Drugs mentions module.

This module represents the transformation functions to extract drug mentions from
pubmed data, clinical trials data and drugs data using hamilton framework paradigm
The function declarations will be run during the execute phase of the driver

"""
import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from utils.utils import _explode, _word_df, _joined_df
from hamilton.function_modifiers import check_output


@check_output(data_type=int, range=(0, 100), allow_nans=False)
def min_d_drug_len(df_drugs: ps.DataFrame) -> int:
    """Return minimum length and maximum length of drugs"""
    return df_drugs['d_drug'].str.len().min()


@check_output(data_type=int, range=(0, 100), allow_nans=False)
def max_d_drug_len(df_drugs: ps.Series) -> int:
    """Return minimum length and maximum length of drugs"""
    return df_drugs['d_drug'].str.len().max()


def exploded_ct_scientific_title(df_clinical_trials: ps.DataFrame,
                                 trials_title_col: str,
                                 trials_word_col: str
                                 ) -> ps.DataFrame:
    """Explode scientific_title to words"""
    return _explode(df_clinical_trials, trials_title_col, trials_word_col)


def exploded_pubmed_title(df_pubmed: ps.DataFrame,
                          pubmed_title_col: str,
                          pubmed_word_col: str
                          ) -> ps.DataFrame:
    """Explode pubmed Title to words"""
    return _explode(df_pubmed, pubmed_title_col, pubmed_word_col)


def word_ct_scientific_title(exploded_ct_scientific_title: ps.DataFrame,
                             trials_word_col: str,
                             min_d_drug_len: int,
                             max_d_drug_len: int) -> ps.DataFrame:
    """Filter scientific titles words removing words that have a different length from drugs"""
    return _word_df(exploded_ct_scientific_title,
                    trials_word_col, min_d_drug_len, max_d_drug_len)


def word_pubmed_title(exploded_pubmed_title: ps.DataFrame,
                      pubmed_word_col: str,
                      min_d_drug_len: int,
                      max_d_drug_len: int) -> ps.DataFrame:
    """Filter pubmed titles words removing words that have a different length from drugs"""
    return _word_df(exploded_pubmed_title,
                    pubmed_word_col, min_d_drug_len, max_d_drug_len)


def trials_joined(df_drugs: ps.DataFrame, word_ct_scientific_title: ps.DataFrame,
                  drug_title_col: str, trials_word_col: str,
                  t_date_col: str, output_df_cols: list) -> ps.DataFrame:
    """Join scientifc trials word dataframe with drugs dataframe"""
    return _joined_df(df_drugs, word_ct_scientific_title, drug_title_col, trials_word_col,
                      t_date_col, output_df_cols)


def pubmed_joined(df_drugs: ps.DataFrame, word_pubmed_title: ps.DataFrame,
                  drug_title_col: str, pubmed_word_col: str,
                  p_date_col: str, output_df_cols: list) -> ps.DataFrame:
    """Join pubmed word dataframe with drugs dataframe"""
    return _joined_df(df_drugs, word_pubmed_title, drug_title_col, pubmed_word_col,
                      p_date_col, output_df_cols)


def drugs_stats_df(pubmed_joined: ps.DataFrame, trials_joined: ps.DataFrame) -> ps.DataFrame:
    """merge the pubmed and scientific trials dataframes"""
    return pubmed_joined.append(trials_joined, ignore_index=True)


def df_stats_cleaned_df(drugs_stats_df: ps.DataFrame, pubmed_word_col: str,
                        trials_word_col: str) -> ps.DataFrame:
    """Clean drugs journal dataframe removing duplicates and formatting it to approrpriate format using GroupBy"""
    drugs_stats_s_df = drugs_stats_df.to_spark().dropDuplicates() \
        .drop(pubmed_word_col, trials_word_col) \
        .withColumn('clinical_trials', col('t_date')) \
        .withColumn('pubmed', col('p_date'))
    s_df = drugs_stats_s_df.groupBy(['d_drug', 'journal']) \
        .agg(F.collect_list('clinical_trials'), F.collect_list('pubmed')) \
        .orderBy('d_drug')
    return s_df.to_pandas_on_spark()
