from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd


class PatchInterface:
    def apply(self, df: pd.DataFrame) -> None:
        raise NotImplementedError()


@dataclass
class UpdatePatch(PatchInterface):
    """
    Basic Patch object that stores the changes
    """

    target: Dict[str, Any]
    deltas: Dict[str, Any]

    @staticmethod
    def parse_patch_object_json_dict(patch_object_json_dict: Dict) -> UpdatePatch:
        ret = UpdatePatch(
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
class CreatePatch(PatchInterface):
    target: Dict[str, Any]
    payload: Dict[str, Any]

    def apply(self, df: pd.DataFrame) -> None:
        raise NotImplementedError("Need to finish the implementation")
        # First, check to ensure that the row doesn't already exist
        match_mask = pd.Series(True, index=df.index)
        for key, value in self.target.items():
            match_mask &= df[key] == value

        if match_mask.any():
            raise ValueError("Row with target values already exists, cannot create.")

        # Second, insert a new row with the payload data
        new_row = self.target.copy()
        new_row.update(self.payload)
        df = df.append(new_row, ignore_index=True)


@dataclass
class DeletePatch(PatchInterface):
    target: Dict[str, Any]

    def apply(self, df: pd.DataFrame) -> None:
        # Find the row(s) in the DataFrame that match the target keys and values
        match_mask = pd.Series(True, index=df.index)
        for key, value in self.target.items():
            match_mask &= df[key] == value

        # Delete the rows(s)
        df.drop(df[match_mask].index, inplace=True)


@dataclass
class PatchFile:
    """
    Container for the patch file that stores all the patches
    """

    patches: List[UpdatePatch]
    meta: Dict[str, Any]
    version: int
    deletions: Dict[str, Any]  # TODO: Wrap Up Implementation
    creations: Dict[str, Any]  # TODO: Wrap Up Implementation

    @staticmethod
    def parse_patch_file_json_dict(json_dict: Dict) -> PatchFile:
        if json_dict["version"] != 1:
            raise Exception("Did not find version=1 for the patch file format")

        ret = PatchFile(
            patches=[
                UpdatePatch.parse_patch_object_json_dict(patch_object)
                for patch_object in json_dict["patches"]
            ],
            version=json_dict["version"],
            meta=json_dict["meta"],
        )

        return ret

    @staticmethod
    def parse_patch_file_from_path(path: Path) -> PatchFile:
        with open(path, "r") as file_ptr:
            patch_json = json.load(file_ptr)
            patch_file = PatchFile.parse_patch_file_json_dict(patch_json)
            return patch_file

    def get_json(self) -> str:
        """Gets the JSON of the patch file"""
        ret = {
            "patches": [patch.__dict__ for patch in self.patches],
            "version": self.version,
            "meta": self.meta,
        }
        return json.dumps(ret)

    def apply_update_patches(self, df: pd.DataFrame):
        for patch in self.patches:
            patch.apply(df)

    def apply(self):
        raise NotImplementedError("Need to test and changes for all types of patches")


def generate_patches(
    old_df: pd.DataFrame, new_df: pd.DataFrame, id_columns: List[str]
) -> List[UpdatePatch]:
    """
    Generate patches representing changes between two DataFrames.

    Parameters:
    - source_df (pd.DataFrame): The source DataFrame.
    - target_df (pd.DataFrame): The target DataFrame.

    Returns:
    List of Patch objects representing the changes between the DataFrames.
    """

    # Check if the DataFrames have the same columns
    if not set(new_df.columns).issubset(set(old_df.columns)):
        raise ValueError("Source and target DataFrames must have the same columns")

    patches = []

    for _, new_row in new_df.iterrows():
        # target_row = new_df[new_df[id_columns] == old_row[id_columns]]
        # target_row = new_df.loc[new_df[id_columns] == old_df[id_columns]].iloc[0]
        query = " & ".join(
            [
                f"{col} == '{new_row[col]}'"
                if isinstance(new_row[col], str)
                else f"{col} == {new_row[col]}"
                for col in id_columns
            ]
        )
        old_row_query_result = old_df.query(query)
        old_row = old_row_query_result.iloc[0]

        deltas = {}
        for column in new_df.columns:
            # Skip the ID column
            if column in id_columns:
                continue

            if pd.isna(new_row[column]) and pd.isna(old_row[column]):
                continue

            if new_row[column] != old_row[column]:
                deltas[column] = (
                    new_row[column] if not pd.isna(new_row[column]) else None
                )

        if deltas:
            patch = UpdatePatch(
                target={col: new_row[col] for col in id_columns}, deltas=deltas
            )
            patches.append(patch)

    return patches


def apply_patches(df: pd.DataFrame, patches: List[UpdatePatch]) -> pd.DataFrame:
    # Make a Copy
    temp_df = df.copy()

    # Go through each of the patches
    for patch in patches:
        patch.apply(temp_df)

    return temp_df
