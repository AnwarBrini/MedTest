# -*- coding: utf-8 -*-
"""Constants module.

This file contains all the necessary constants to run the jobs

"""
import os

from pyspark.conf import SparkConf

##############################################################
# Environmental variables
##############################################################

basedir = os.path.abspath(os.path.dirname(__file__))
# load_dotenv(os.path.join(basedir, '../credentials/.env'))

SPARK_CONF = SparkConf()
SPARK_CONF.set("spark.sql.execution.arrow.pyspark.enabled", "false")

##############################################################
# Data Input schemas
##############################################################

DRUGS_MAPPING: dict[str, str] = {"atccode": "d_atccode", "drug": "d_drug"}
DRUG_TITLE_COL: str = "d_drug"
CLINICAL_TRIALS_MAPPING: dict[str, str] = {
    "id": "ct_id",
    "scientific_title": "ct_scientific_title",
    "date": "date",
    "journal": "journal"
}
TRIALS_TITLE_COL: str = "ct_scientific_title"
TRIALS_WORD_COL: str = "t_word"
PUBMED_MAPPING: dict[str, str] = {
    "id": "pubmed_id",
    "title": "pubmed_title",
    "date": "date",
    "journal": "journal"
}
PUBMED_TITLE_COL: str = "pubmed_title"
PUBMED_WORD_COL: str = "p_word"
JOURNAL_COL: str = "journal"

P_DATE_COL = "p_date"
T_DATE_COL = "t_date"

OUTPUT_DF_COLS: list[str] = ["d_drug", "journal", "p_word", "t_word", "p_date", "t_date"]
