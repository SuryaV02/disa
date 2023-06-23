import re
import numpy as np
import pandas as pd
import logging


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


def blank_ht_exp_phone_unit(df):
    # Blank out the phone amount values where we see an 'Other'
    print("Blanking out: \n", df[df["h1_exp_phone_unit"] == "Other"]["hhid"])
    df.loc[df["h1_exp_phone_unit"] == "Other", "h1_exp_phone_amt"] = np.nan
    return df


def update_h1_exp_phone_amt(df):
    # Phone amount needs to be monthly amount. Divide value by 2 if reported for
    # 2 months and 3 if reported for 3 months

    def convert_text_to_months(text):
        pattern = r".*(\d+)\s*([m,M]onth|[y,Y]ear)"
        match = re.search(pattern, text)
        if match is None:
            raise Exception("Could not extract month length from text: ", text)

        base = int(match.group(1))
        multiplier = 1 if match.group(2).lower() == "month" else 12
        return base * multiplier

    df["h1_exp_phone_amt"] = df.apply(
        lambda row: row["h1_exp_phone_amt"]
        / convert_text_to_months(row["h1_exp_phone_unit"])
        if (pd.notnull(row["h1_exp_phone_amt"]) and row["h1_exp_phone_unit"])
        else np.nan,
        axis=1,
    )
    return df


def update_h1_exp_educ_amt(df):
    # divide by 6
    df["h1_exp_educ_amt"] = df["h1_exp_educ_amt"].div(6, fill_value=np.nan)
    return df


def update_h1_exp_rituals_amt(df):
    # divide by 6
    df["h1_exp_rituals_amt"] = df["h1_exp_rituals_amt"].div(6, fill_value=np.nan)
    return df


def add_exp_other_specify_to_h1_exp_substance_amt(df):
    # There is one 'other' reported as 'Beedi'. Please move this to h1_exp_substance_amt

    def fix_row(row):
        # Note we can add more conditions for other values here
        if row["h1_exp_other_specify"] == "Beedi":
            row["h1_exp_substance_amt"] == row["h1_exp_other_amt"]
            row["h1_exp_other_amt"] == np.nan
            row["h1_exp_other_specify"] = np.nan
            row["h1_exp_other"] = "No"

        return row

    # TODO - Need to output the implement the logging for these changes
    df = df.apply(fix_row, axis=1)

    return df
