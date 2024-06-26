import io
import pandas as pd
import requests
import zipfile
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    # Accessing the college scorecard API.
# The endpoint for querying all data is /v1/schools
    url = "https://ed-public-download.app.cloud.gov/downloads/CollegeScorecard_Raw_Data_09262023.zip"
    response = requests.get(url)
    response.raise_for_status()
    zip_file_bytes = io.BytesIO(response.content)
    
    """
    after obtaining the schema, we will paste it here
    """
    college_dtypes = {
                        'UNITID': pd.Int64Dtype(),
                        'OPEID6': pd.Int64Dtype(),
                        "INSTNM": str,
                        "CONTROL": str,
                        "MAIN": pd.Int64Dtype(),
                        "CIPCODE": pd.Int64Dtype(),
                        "CIPDESC": str,
                        "CREDLEV:": pd.Int64Dtype(),
                        "CREDDESC": str,
                        "IPEDSCOUNT1": float,
                        "IPEDSCOUNT2": float,
                        "DEBT_ALL_STGP_ANY_N": str,
                        "DEBT_ALL_STGP_ANY_MEAN": str,
                        "DEBT_ALL_STGP_ANY_MDN": str,
                        "DEBT_ALL_STGP_EVAL_N": str,
                        "DEBT_ALL_STGP_EVAL_MEAN": str,
                        "DEBT_ALL_STGP_EVAL_MDN": str,
                        "DEBT_ALL_PP_ANY_N": str,
                        "DEBT_ALL_PP_ANY_MEAN": str,
                        "DEBT_ALL_PP_ANY_MDN": str,
                        "DEBT_ALL_PP_EVAL_N": str,
                        "DEBT_ALL_PP_EVAL_MEAN": str,
                        "DEBT_ALL_PP_EVAL_MDN": str,
                        "DEBT_MALE_STGP_ANY_N": str,
                        "DEBT_MALE_STGP_ANY_MEAN": str,
                        "DEBT_MALE_STGP_ANY_MDN": str,
                        "DEBT_MALE_STGP_EVAL_N": str,
                        "DEBT_MALE_STGP_EVAL_MEAN": str,
                        "DEBT_MALE_STGP_EVAL_MDN": str,
                        "DEBT_MALE_PP_ANY_N": str,
                        "DEBT_MALE_PP_ANY_MEAN": str,
                        "DEBT_MALE_PP_ANY_MDN": str,
                        "DEBT_MALE_PP_EVAL_N": str,
                        "DEBT_MALE_PP_EVAL_MEAN": str,
                        "DEBT_MALE_PP_EVAL_MDN": str,
                        "DEBT_NOTMALE_STGP_ANY_N": str,
                        "DEBT_NOTMALE_STGP_ANY_MEAN": str,
                        "DEBT_NOTMALE_STGP_ANY_MDN": str,
                        "DEBT_NOTMALE_STGP_EVAL_N": str,
                        "DEBT_NOTMALE_STGP_EVAL_MEAN": str,
                        "DEBT_NOTMALE_STGP_EVAL_MDN": str,
                        "DEBT_NOTMALE_PP_ANY_N": str,
                        "DEBT_PELL_STGP_ANY_N": str,
                        "DEBT_PELL_STGP_ANY_MEAN": str,
                        "DEBT_PELL_STGP_ANY_MDN": str,
                        "DEBT_PELL_STGP_EVAL_N": str,
                        "DEBT_PELL_STGP_EVAL_MEAN": str,
                        "DEBT_PELL_STGP_EVAL_MDN": str,
                        "DEBT_PELL_PP_ANY_N": str,
                        "DEBT_PELL_PP_ANY_MEAN": str,
                        "DEBT_PELL_PP_ANY_MDN": str,
                        "DEBT_PELL_PP_EVAL_N": str,
                        "DEBT_PELL_PP_EVAL_MEAN": str,
                        "DEBT_PELL_PP_EVAL_MDN": str,
                        "DEBT_NOPELL_STGP_ANY_N": str,
                        "DEBT_NOPELL_STGP_ANY_MEAN": str,
                        "DEBT_NOPELL_STGP_ANY_MDN": str,
                        "DEBT_NOPELL_STGP_EVAL_N": str,
                        "DEBT_NOPELL_STGP_EVAL_MEAN": str,
                        "DEBT_NOPELL_STGP_EVAL_MDN": str,
                        "DEBT_NOPELL_PP_ANY_N": str,
                        "DEBT_NOPELL_PP_ANY_MEAN": str,
                        "DEBT_NOPELL_PP_ANY_MDN": str,
                        "DEBT_NOPELL_PP_EVAL_N": str,
                        "DEBT_NOPELL_PP_EVAL_MEAN": str,
                        "DEBT_NOPELL_PP_EVAL_MDN": str,
                        "DEBT_ALL_PP_ANY_MDN10YRPAY": str,
                        "DEBT_ALL_PP_EVAL_MDN10YRPAY": str,
                        "DEBT_ALL_STGP_ANY_MDN10YRPAY": str,
                        "DEBT_ALL_STGP_EVAL_MDN10YRPAY": str,
                        "EARN_COUNT_NWNE_HI_1YR": str,
                        "EARN_CNTOVER150_HI_1YR": str,
                        "EARN_COUNT_WNE_HI_1YR": str,
                        "EARN_MDN_HI_1YR": str,
                        "EARN_COUNT_NWNE_HI_2YR":str,
                        "EARN_CNTOVER150_HI_2YR": str,
                        "EARN_COUNT_WNE_HI_2YR": str,
                        "EARN_MDN_HI_2YR": str,
                        "BBRR2_FED_COMP_N": str,
                        "BBRR2_FED_COMP_DFLT": str,
                        "BBRR2_FED_COMP_DLNQ": str,
                        "BBRR2_FED_COMP_FBR": str,
                        "BBRR2_FED_COMP_DFR": str,
                        "BBRR2_FED_COMP_NOPROG": str,
                        "BBRR2_FED_COMP_MAKEPROG": str,
                        "BBRR2_FED_COMP_PAIDINFULL": str,
                        "BBRR2_FED_COMP_DISCHARGE": str,
                        "BBRR3_FED_COMP_N": str,
                        "BBRR3_FED_COMP_DFLT": str,
                        "BBRR3_FED_COMP_DLNQ": str,
                        "BBRR3_FED_COMP_FBR": str,
                        "BBRR3_FED_COMP_DFR": str,
                        "BBRR3_FED_COMP_NOPROG": str,
                        "BBRR3_FED_COMP_MAKEPROG": str,
                        "BBRR3_FED_COMP_PAIDINFULL": str,
                        "BBRR3_FED_COMP_DISCHARGE": str,
                        "EARN_COUNT_PELL_WNE_1YR": str,
                        "EARN_PELL_WNE_MDN_1YR": str,
                        "EARN_COUNT_NOPELL_WNE_1YR": str,
                        "EARN_NOPELL_WNE_MDN_1YR": str,
                        "EARN_COUNT_MALE_WNE_1YR": str,
                        "EARN_MALE_WNE_MDN_1YR": str,
                        "EARN_COUNT_NOMALE_WNE_1YR": str,
                        "EARN_NOMALE_WNE_MDN_1YR": str,
                        "EARN_COUNT_NE_3YR": str,
                        "EARN_NE_MDN_3YR": str,
                        "EARN_COUNT_WNE_3YR": str,
                        "EARN_CNTOVER150_3YR": str,
                        "EARN_COUNT_PELL_NE_3YR": str,
                        "EARN_PELL_NE_MDN_3YR": str,
                        "EARN_COUNT_NOPELL_NE_3YR": str,
                        "EARN_NOPELL_NE_MDN_3YR": str,
                        "EARN_COUNT_MALE_NE_3YR": str,
                        "EARN_MALE_NE_MDN_3YR": str,
                        "EARN_COUNT_NOMALE_NE_3YR": str,
                        "EARN_NOMALE_NE_MDN_3YR": str,
                        "DISTANCE": pd.Int64Dtype(),
                    }
    with zipfile.ZipFile(zip_file_bytes) as zip_file:
        
        """"
        'data/FieldOfStudyData1415_1516_PP.csv'
        'data/FieldOfStudyData1516_1617_PP.csv'
        'data/FieldOfStudyData1617_1718_PP.csv'
        'data/FieldOfStudyData1718_1819_PP.csv'
        'data/FieldOfStudyData1819_1920_PP.csv', 
        'data/MERGED2019_20_PP.csv'
        """
        
        desired_file_name = 'data/FieldOfStudyData1819_1920_PP.csv'
        with zip_file.open(desired_file_name) as file:
            df = pd.read_csv(file, dtype=college_dtypes)
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
