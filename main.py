"""Main file"""
import importlib
import click
from hamilton import driver
from hamilton.experimental import h_spark
import pyspark.pandas as ps

from utils.spark import SparkProvider
from utils.constants import SPARK_CONF, DRUG_TITLE_COL, CLINICAL_TRIALS_MAPPING, PUBMED_MAPPING, \
    DRUGS_MAPPING, TRIALS_TITLE_COL, PUBMED_TITLE_COL, TRIALS_WORD_COL, PUBMED_WORD_COL, OUTPUT_DF_COLS, \
    P_DATE_COL, T_DATE_COL


@click.command()
@click.option('-s', '--source-paths', type=(str, str, str))
@click.option('-o', '--output-path', type=str)
def main(source_paths: (str, str, str), output_path: str):
    # Reading parameters
    clinical_trials_path, drugs_path, pubmed_path = source_paths
    # Initialising spark
    spark = SparkProvider("MedTest", conf=SPARK_CONF).spark
    ps.set_option('compute.ops_on_diff_frames', True)

    skga = h_spark.SparkKoalasGraphAdapter(
        spark_session=spark,
        result_builder=h_spark.base.DictResult(),
        # KoalasDataFrameResult(),
        spine_column=None,
    )
    module_names = [
        "jobs.data_loader",
        "jobs.drugs_mentions"
    ]
    initial_config = {
        'clinical_trials_path': clinical_trials_path,
        'drugs_path': drugs_path,
        'pubmed_path': pubmed_path,
        'drugs_mapping': DRUGS_MAPPING,
        'clinical_trials_mapping': CLINICAL_TRIALS_MAPPING,
        'pubmed_mapping': PUBMED_MAPPING,
        'drug_title_col': DRUG_TITLE_COL,
        'trials_title_col': TRIALS_TITLE_COL,
        'trials_word_col': TRIALS_WORD_COL,
        'pubmed_title_col': PUBMED_TITLE_COL,
        'pubmed_word_col': PUBMED_WORD_COL,
        'output_df_cols': OUTPUT_DF_COLS,
        'p_date_col': P_DATE_COL,
        't_date_col': T_DATE_COL
    }
    modules = [importlib.import_module(m) for m in module_names]

    dr = driver.Driver(initial_config, *modules, adapter=skga)
    outputs = ['df_stats_cleaned_df']
    # let's create the dataframe!
    df = dr.execute(outputs)
    dr.display_all_functions('./output/graph.dot')
    df["df_stats_cleaned_df"].to_spark().write.format('json').mode("overwrite")\
        .save(output_path)
    SparkProvider.tear_down_spark(spark)


if __name__ == "__main__":
    main()
