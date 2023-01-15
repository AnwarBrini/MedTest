# -*- coding: utf-8 -*-
"""Utils module.

This module represents functions that are used by the hamilton modules

"""
from datetime import datetime

import pyspark.pandas as ps
from pyspark.sql.functions import explode, split, col, upper, length
import numpy as np


def _load_csv(csv_path: str,
              mapping_dict: dict) -> ps.DataFrame:
    """generic load_csv function using pandas api on pyspark"""
    return ps.read_csv(csv_path,
                       header=0,
                       index_col=list(mapping_dict.keys())[0],
                       infer_datetime_format=True)\
        .rename(columns=mapping_dict)


def _explode(df_to_explode: ps.DataFrame, old_col: str, new_col: str) -> ps.DataFrame:
    """generic function to explode a string column to a new word column"""
    s_df = df_to_explode.to_spark().withColumn(new_col, explode(split(col(old_col), '\\W+'))) \
        .withColumn(new_col, upper(col(new_col)))
    return s_df.pandas_api()


def _word_df(exploded_title: ps.DataFrame,
             word_col: str,
             min_d_drug_len: int,
             max_d_drug_len: int) -> ps.DataFrame:
    """generic function to filter dataframes according to word length"""
    s_df = exploded_title.to_spark().where(length(col(word_col)) >= min_d_drug_len) \
        .where(length(col(word_col)) <= max_d_drug_len)
    return s_df.pandas_api()


def try_parsing_date(date_filed: any):
    """Function to format date fields to appropriate format"""
    for fmt in ('%d-%m-%Y', '%d.%m.%Y', '%d/%m/%Y', '%d %B, %Y'):
        try:
            datetime.strptime(date_filed, fmt)
            return datetime.strptime(date_filed, '%d/%m/%Y')
        except (ValueError, TypeError):
            pass
    return np.nan


def _joined_df(df1: ps.DataFrame, df2: ps.DataFrame,
               key1: str, key2: str,
               date1: str, df_cols: list) -> ps.DataFrame:
    """Dataframe join function and adding appropriate columns to prepare for union operation"""
    # logic to join
    df_merged = df1.merge(df2, how="left", left_on=key1, right_on=key2)
    df_merged[date1] = df_merged['date']
    curr_cols = list(df_merged.columns)
    # Adding other dataframe columns
    for col_df in df_cols:
        if col_df not in curr_cols:
            df_merged[col_df] = np.nan
    return df_merged[df_cols]
