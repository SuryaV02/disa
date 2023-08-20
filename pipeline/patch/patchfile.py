from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd


@dataclass
class Patch:
    """
    Basic Patch object that stores the changes
    """

    target: Dict[str, Any]
    deltas: Dict[str, Any]

    @staticmethod
    def parse_patch_object_json_dict(patch_object_json_dict: Dict) -> Patch:
        ret = Patch(
            target=patch_object_json_dict["target"],
            deltas=patch_object_json_dict["deltas"],
        )

        return ret

    def apply(self, df: pd.DataFrame) -> None:
        # Select the correct row using the list of target keys and values in target
        # Fix all the column values given by deltas

        # Find the row(s) in the DataFrame that match the target keys and values
        match_mask = pd.Series(True, index=df.index)
        for key, value in self.target.items():
            match_mask &= df[key] == value

        # Apply the deltas to the matching rows
        for index, _ in df[match_mask].iterrows():
            for column, delta in self.deltas.items():
                df.at[index, column] = delta


@dataclass
class PatchFile:
    """
    Container for the patch file that stores all the patches
    """

    patches: List[Patch]
    meta: Dict[str, Any]
    version: int

    @staticmethod
    def parse_patch_file_json_dict(json_dict: Dict) -> PatchFile:
        if json_dict["version"] != 1:
            raise Exception("Did not find version=1 for the patch file format")

        ret = PatchFile(
            patches=[
                Patch.parse_patch_object_json_dict(patch_object)
                for patch_object in json_dict["patches"]
            ],
            version=json_dict["version"],
            meta=json_dict["meta"],
        )

        return ret

    def get_json(self) -> str:
        """Gets the JSON of the patch file"""
        ret = {
            "patches": [patch.__dict__ for patch in self.patches],
            "version": self.version,
            "meta": self.meta,
        }
        return json.dumps(ret)


def generate_patches(
    old_df: pd.DataFrame, new_df: pd.DataFrame, id_columns: List[str]
) -> List[Patch]:
    """
    Generate patches representing changes between two DataFrames.

    Parameters:
    - source_df (pd.DataFrame): The source DataFrame.
    - target_df (pd.DataFrame): The target DataFrame.

    Returns:
    List of Patch objects representing the changes between the DataFrames.
    """

    # Check if the DataFrames have the same columns
    if set(old_df.columns) != set(new_df.columns):
        raise ValueError("Source and target DataFrames must have the same columns")

    patches = []

    for _, new_row in new_df.iterrows():
        # target_row = new_df[new_df[id_columns] == old_row[id_columns]]
        # target_row = new_df.loc[new_df[id_columns] == old_df[id_columns]].iloc[0]
        query = " & ".join([f"{col} == {new_row[col]}" for col in id_columns])
        old_row_query_result = old_df.query(query)
        old_row = old_row_query_result.iloc[0]

        deltas = {}
        for column in new_df.columns:
            # Skip the ID column
            if column in id_columns:
                continue

            if new_row[column] != old_row[column]:
                deltas[column] = new_row[column]

        if deltas:
            patch = Patch(
                target={col: new_row[col] for col in id_columns}, deltas=deltas
            )
            patches.append(patch)

    return patches


def apply_patches(df: pd.DataFrame, patches: List[Patch]) -> pd.DataFrame:
    # Make a Copy
    temp_df = df.copy()

    # Go through each of the patches
    for patch in patches:
        patch.apply(temp_df)

    return temp_df
