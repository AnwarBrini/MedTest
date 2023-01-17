"""
Filename: test_param_with_dataframe.py
"""
import unittest
from nose2.tools import params
import pyspark.pandas as ps
from parameterized import parameterized
from jobs.drugs_mentions import *


class TestMinDrugLen(unittest.TestCase):
    @parameterized.expand(
        [
            # each tuple contains (name, df_drugs, expected_output, expected_error)
            ("test_drug", ps.DataFrame([{'d_drug': 'ETHANOL'},
                                        {'d_drug': 'EPINEPHRINE'}], columns=['d_drug']), 7, None),
            ("test_empty", ps.DataFrame([], columns=['d_drug']), None, None),
            ("test_int", ps.DataFrame([{'d_drug': 2},
                                       {'d_drug': 'EPINEPHRINE'}], columns=['d_drug']), None, ValueError)
        ]
    )
    def test_min_d_drug_len(self, df_drugs, expected_output, expected_error=None):
        # If no error is expected from the test case,
        # check if the actual output matches the expected output
        if expected_error is None:
            assert min_d_drug_len(df_drugs) == expected_output

        # If an error is expected from the test case,
        # check if the actual error matches the expected error
        else:
            with self.assertRaises(expected_error):
                min_d_drug_len(df_drugs)


class TestMaxDrugLen(unittest.TestCase):
    @parameterized.expand(
        [
            # each tuple contains (name, df_drugs, expected_output, expected_error)
            ("test_drug", ps.DataFrame([{'d_drug': 'ETHANOL'},
                                        {'d_drug': 'EPINEPHRINE'}], columns=['d_drug']), 11, None),
            ("test_empty", ps.DataFrame([], columns=['d_drug']), None, None),
            ("test_int", ps.DataFrame([{'d_drug': 2},
                                       {'d_drug': 'EPINEPHRINE'}], columns=['d_drug']), None, KeyError)
        ]
    )
    def test_max_d_drug_len(self, df_drugs, expected_output, expected_error=None):
        # If no error is expected from the test case,
        # check if the actual output matches the expected output
        if expected_error is None:
            assert max_d_drug_len(df_drugs) == expected_output

        # If an error is expected from the test case,
        # check if the actual error matches the expected error
        else:
            with self.assertRaises(expected_error):
                max_d_drug_len(df_drugs)


class TestExplodedScientifcTitle(unittest.TestCase):
    @parameterized.expand(
        [
            # each tuple contains (name, df_clinical_trials, title, word , expected_output, expected_error)
            ("test_title", ps.DataFrame(['to Paclitaxel'],
                                        columns=['ct_scientific_title']), 't_word',
             ps.DataFrame([{'to': 'to Paclitaxel', 'Paclitaxel': 'to Paclitaxel'}],
                          columns=['t_word', 'ct_scientific_title']), None),
            ("test_empty", ps.DataFrame([],
                                        columns=['ct_scientific_title']), 't_word', None, KeyError),
            ("test_int", ps.DataFrame([3],
                                      columns=['ct_scientific_title']), 't_word', ps.DataFrame([{3: 3}],
                                                                                               columns=['t_word',
                                                                                                        'ct_scientific_title']),
             None)
        ]
    )
    def test_exploded_ct_scientific_title(self, df_clinical_trials,
                                          trials_title_col,
                                          trials_word_col, expected_output, expected_error=None):
        # If no error is expected from the test case,
        # check if the actual output matches the expected output
        if expected_error is None:
            assert exploded_ct_scientific_title(df_clinical_trials, trials_title_col,
                                                trials_word_col) == expected_output

        # If an error is expected from the test case,
        # check if the actual error matches the expected error
        else:
            with self.assertRaises(expected_error):
                exploded_ct_scientific_title(df_clinical_trials, trials_title_col,
                                             trials_word_col)


class TestExplodedScientifcTitle(unittest.TestCase):
    @parameterized.expand(
        [
            # each tuple contains (name, df_clinical_trials, title, word , expected_output, expected_error)
            ("test_title", ps.DataFrame(['to Paclitaxel'],
                                        columns=['ct_scientific_title']), 't_word',
             ps.DataFrame([{'to': 'to Paclitaxel', 'Paclitaxel': 'to Paclitaxel'}],
                          columns=['t_word', 'ct_scientific_title']), None),
            ("test_empty", ps.DataFrame([],
                                        columns=['ct_scientific_title']), 't_word', None, KeyError),
            ("test_int", ps.DataFrame([3],
                                      columns=['ct_scientific_title']), 't_word', ps.DataFrame([{3: 3}],
                                                                                               columns=['t_word',
                                                                                                        'ct_scientific_title']),
             None)
        ]
    )
    def test_exploded_ct_scientific_title(self, df_clinical_trials,
                                          trials_title_col,
                                          trials_word_col, expected_output, expected_error=None):
        # If no error is expected from the test case,
        # check if the actual output matches the expected output
        if expected_error is None:
            assert exploded_ct_scientific_title(df_clinical_trials, trials_title_col,
                                                trials_word_col) == expected_output

        # If an error is expected from the test case,
        # check if the actual error matches the expected error
        else:
            with self.assertRaises(expected_error):
                exploded_ct_scientific_title(df_clinical_trials, trials_title_col,
                                             trials_word_col)


class TestStatsDF(unittest.TestCase):
    @parameterized.expand(
        [
            # each tuple contains (name, drugs_stats_df, title, word , expected_output, expected_error)
            ("test_clean", ps.DataFrame(['DIPHENHYDRAMINE', 'Journal of emergency nursing',
                                         '1 January 2020', '01/01/2019'],
                                        columns=['d_drug', 'journal', 't_date', 'p_date']), 't_word', 'p_word',
             ps.DataFrame([{"d_drug": "DIPHENHYDRAMINE", "journal": "Journal of emergency nursing",
                            "collect_list(clinical_trials)": ["1 January 2020"],
                            "collect_list(pubmed)": ["01/01/2019"]}]), None),
            ("test_empty", ps.DataFrame([], 'p_word', 't_word', None, ValueError))
        ]
    )
    def test_df_stats_cleaned_df(self, drugs_stats_df,
                                 pubmed_word_col,
                                 trials_word_col, expected_output, expected_error=None):
        # If no error is expected from the test case,
        # check if the actual output matches the expected output
        if expected_error is None:
            assert df_stats_cleaned_df(drugs_stats_df, pubmed_word_col,
                                       trials_word_col) == expected_output

        # If an error is expected from the test case,
        # check if the actual error matches the expected error
        else:
            with self.assertRaises(expected_error):
                df_stats_cleaned_df(drugs_stats_df, pubmed_word_col,
                                    trials_word_col)


if __name__ == '__main__':
    import nose2

    nose2.main()
