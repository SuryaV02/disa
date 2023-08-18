from typing import Dict
import pandas as pd


def process_data_with_json(
    raw_df: pd.DataFrame, column_name_dict: Dict
) -> pd.DataFrame:
    processed_df = (
        raw_df.copy()
    )  # Create a copy to avoid modifying the original DataFrame

    for column_name, value_mapping in column_name_dict.items():
        if column_name in processed_df.columns:
            processed_df[column_name] = processed_df[column_name].replace(value_mapping)

    return processed_df


def apply_patch(main_df: pd.DataFrame, patch_df: pd.DataFrame) -> pd.DataFrame:
    def update_cell(df, column_name, row_index, new_value):
        df.loc[df["hhid"] == row_index, column_name] = new_value

    for index, patch_row in patch_df.iterrows():
        for column_name, new_value in patch_row.items():
            if column_name != "hhid":
                update_cell(main_df, column_name, index + 1, new_value)
    return main_df
