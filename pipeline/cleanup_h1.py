import numpy as np


def calculate_h1_income_total(df):
    """
    h1_income_cultivation
    h1_income_cultivation_amt
    h1_income_livestock
    h1_income_livestock_amt
    h1_income_othagri
    h1_income_othagri_amt
    h1_income_business
    h1_income_nonagri_amt
    h1_income_wages
    h1_income_wages_amt
    h1_income_salaries
    h1_income_salaries_amt
    h1_income_pension
    h1_income_pension_amt
    h1_income_govt
    h1_income_govt_amt
    h1_income_remittances
    h1_income_remittances_amt
    h1_income_other
    h1_income_other_specify
    h1_income_other_amt
    """
    df["h1_income_total"] = df[
        [
            "h1_income_cultivation_amt",
            "h1_income_livestock_amt",
            "h1_income_othagri_amt",
            "h1_income_nonagri_amt",
            "h1_income_wages_amt",
            "h1_income_salaries_amt",
            "h1_income_pension_amt",
            "h1_income_govt_amt",
            "h1_income_remittances_amt",
            "h1_income_other_amt",
        ]
    ].sum(axis=1, skipna=True)
    return df


def update_h1_exp_food(df):
    # Divide by 7 and multiply by 30 to get monthly exp
    df["h1_exp_food"] = (
        df["h1_exp_food"].div(7, fill_value=np.nan).mul(30, fill_value=np.nan)
    )
    return df


def update_h1_exp_water_amt(df):
    # divide by 6
    df["h1_exp_water_amt"] = df["h1_exp_water_amt"].div(6, fill_value=np.nan)
    return df


def update_h1_exp_healthcare_amt(df):
    # divide by 6
    df["h1_exp_healthcare_amt"] = df["h1_exp_healthcare_amt"].div(6, fill_value=np.nan)
    return df


def update_h1_exp_phone_amt(df):
    # Phone amount needs to be monthly amount. Divide value by 2 if reported for
    # 2 months and 3 if reported for 3 months
    raise NotImplementedError()


def update_h1_exp_educ_amt(df):
    # divide by 6
    df["h1_exp_educ_amt"] = df["h1_exp_educ_amt"].div(6, fill_value=np.nan)
    return df


def update_h1_exp_rituals_amt(df):
    # divide by 6
    df["h1_exp_rituals_amt"] = df["h1_exp_rituals_amt"].div(6, fill_value=np.nan)
    return df
