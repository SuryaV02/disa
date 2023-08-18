import re
import numpy as np
import pandas as pd
import logging


def calculate_h1_income_total(df) -> pd.DataFrame:
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
    # First reassign other amount:
    df = reassign_h1_income_other_specify(df)

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
            "h1_income_business_amt",
        ]
    ].sum(axis=1, skipna=True)
    return df


def update_h1_exp_food(df) -> pd.DataFrame:
    # Divide by 7 and multiply by 30 to get monthly exp
    df["h1_exp_food"] = (
        df["h1_exp_food"].div(7, fill_value=np.nan).mul(30, fill_value=np.nan)
    )
    return df


def update_h1_exp_water_amt(df) -> pd.DataFrame:
    # divide by 6 to get monthly exp
    df["h1_exp_water_amt"] = df["h1_exp_water_amt"].div(6, fill_value=np.nan)
    return df


def update_h1_exp_healthcare_amt(df) -> pd.DataFrame:
    # divide by 6 to get monthly exp
    df["h1_exp_healthcare_amt"] = df["h1_exp_healthcare_amt"].div(6, fill_value=np.nan)
    return df


def blank_ht_exp_phone_unit(df) -> pd.DataFrame:
    # Blank out the phone amount values where we see an 'Other'
    print("Blanking out: \n", df[df["h1_exp_phone_unit"] == "Other"]["hhid"])
    df.loc[df["h1_exp_phone_unit"] == "Other", "h1_exp_phone_amt"] = np.nan
    return df


def update_h1_exp_phone_amt(df) -> pd.DataFrame:
    # Phone amount needs to be monthly amount. Divide value by 2 if reported for
    # 2 months and 3 if reported for 3 months

    def convert_text_to_months(text):
        print("TEST:")
        print(text)
        if text == "other":
            raise NotImplementedError(
                f"Cannot process value: {text} for column: 'h1_exp_phone_unit'"
            )
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


def update_h1_exp_educ_amt(df) -> pd.DataFrame:
    # divide by 6
    df["h1_exp_educ_amt"] = df["h1_exp_educ_amt"].div(6, fill_value=np.nan)
    return df


def update_h1_exp_rituals_amt(df) -> pd.DataFrame:
    # divide by 6
    df["h1_exp_rituals_amt"] = df["h1_exp_rituals_amt"].div(6, fill_value=np.nan)
    return df


def add_exp_other_specify_to_h1_exp_substance_amt(df) -> pd.DataFrame:
    # There is one 'other' reported as 'Beedi'. Please move this to h1_exp_substance_amt

    def fix_row(row):
        # Note we can add more conditions for other values here
        if row["h1_exp_other_specify"] == "Beedi":
            row["h1_exp_substance_amt"] == row["h1_exp_other_amt"]
            row["h1_exp_other_amt"] == np.nan
            row["h1_exp_other_specify"] = np.nan
            row["h1_exp_other"] = False

        return row

    # TODO - Need to output the implement the logging for these changes
    df = df.apply(fix_row, axis=1)

    return df


INCOME_REASSIGN_DICT = {
    "Ammavadi": "h1_income_govt",
    "Auto": "h1_income_business",
    "Bullero (auto)": "h1_income_business",
    "Chakali ( battalu wash)": "h1_income_business",
    "Chepalu pattadam": "h1_income_othagri",
    "Cloths Iron": "h1_income_business",
    "Contract worker": "h1_income_wages",
    "Dairy": "h1_income_wages",
    "Driver": "h1_income_business",
    "Lari": "h1_income_business",
    "Lari Driver": "h1_income_business",
    "Lari driver": "h1_income_business",
    "Millets&bullero": "h1_income_business",
    "NREGS": "h1_income_wages",
    "RMP Docter": "h1_income_business",
    "Tailor": "h1_income_business",
    "Tailoring": "h1_income_business",
    "Track tar": "h1_income_business",
    "Tracter": "h1_income_business",
    "Tractor": "h1_income_business",
    "Travels": "h1_income_business",
}


def reassign_h1_income_other_specify(df) -> pd.DataFrame:
    def fix_row(row, lookup_table, drop_list):
        if pd.isna(row["h1_income_other_specify"]) is False:
            # Look at the content and figure out where the stuff has to go
            specified_other_category = row["h1_income_other_specify"]
            # Check if this is somthing we need to do
            value = float(row["h1_income_other_amt"])
            if specified_other_category in lookup_table:
                reassignment_category_column_name = lookup_table[
                    specified_other_category
                ]
            else:
                # Add HHID to droplist
                drop_list.append(row["hhid"])
                return row
            reassignment_amount_column_name = f"{reassignment_category_column_name}_amt"
            # Set category to yes
            row[reassignment_category_column_name] = True
            if reassignment_amount_column_name not in row.index:
                row[reassignment_amount_column_name] = value
            elif pd.isna(row[reassignment_amount_column_name]):
                row[reassignment_amount_column_name] = value
            else:
                row[reassignment_amount_column_name] += value

            # Blank out the old columns
            row["h1_income_other_amt"] == np.nan
            row["h1_income_other_specify"] = np.nan
            row["h1_income_other"] = False

            print(
                f"Moved income_other_amt to {reassignment_amount_column_name} for HHID: {row['hhid']}"
            )

        return row

    # Generic whitespace cleanup
    df["h1_income_other_specify"] = df["h1_income_other_specify"].str.strip()
    drop_list = []
    df = df.apply(
        fix_row, lookup_table=INCOME_REASSIGN_DICT, drop_list=drop_list, axis=1
    )

    # Drop all rows with the following HHIDs
    print(
        "Dropping the following rows becuase income_other_specify is not found in reassignment lookup table:",
        drop_list,
    )
    df = df[~df["hhid"].isin(drop_list)]

    return df
