from typing import Dict
import pandas as pd


def process_data_with_json(raw_df: pd.DataFrame, column_name_dict: pd.DataFrame):
    for column_name in raw_df:
        if column_name in column_name_dict:
            column_mapping = dict(
                [(value, key) for key, value in column_name_dict[column_name].items()]
            )
            raw_df.replace({column_name: column_mapping}, inplace=True)

    return raw_df


def apply_patch(main_df: pd.DataFrame, patch_df: pd.DataFrame):
    def update_cell(df, column_name, row_index, new_value):
        df.loc[df["hhid"] == row_index, column_name] = new_value

    for index, patch_row in patch_df.iterrows():
        for column_name, new_value in patch_row.items():
            if column_name != "hhid":
                update_cell(main_df, column_name, index + 1, new_value)
    return main_df
