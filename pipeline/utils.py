import json
from pathlib import Path
from typing import Any, Dict
import pandas as pd
from datetime import datetime
from pipeline import main_logger
from pipeline.patch.patchfile import PatchFile


def generate_timestamp() -> str:
    return datetime.now().strftime("%d-%m-%Y-%H:%M:%S")


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


def apply_patch(main_df: pd.DataFrame, patch_df: pd.DataFrame) -> None:
    def update_cell(
        df: pd.DataFrame,
        column_name: str,
        row_hhid: str,
        row_redcap_event: str,
        new_value: Any,
    ):
        df.loc[
            ((df["hhid"] == row_hhid) & (df["redcap_event_name"] == row_redcap_event)),
            column_name,
        ] = new_value

    for _, patch_row in patch_df.iterrows():
        for column_name, new_value in patch_row.items():
            if column_name != "hhid" and column_name != "redcap_event_name":
                update_cell(
                    main_df,
                    column_name,
                    patch_row["hhid"],
                    patch_row["redcap_event_name"],
                    new_value,
                )


def process_patch_files(
    patches_path: Path, main_dataframe: pd.DataFrame
) -> pd.DataFrame:
    patch_files_list = patches_path.glob("*.json")
    temp_df = main_dataframe.copy()

    # Apply all the patches sequentially
    for patch_file in patch_files_list:
        main_logger.info(f"Processing Patch File: {patch_file.name} ...")

        patch_file = PatchFile.parse_patch_file_from_path(patch_file)

        patch_file.apply_update_patches(main_dataframe)

    return temp_df
