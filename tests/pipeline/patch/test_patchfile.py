import json
from typing import Any
from deepdiff import DeepDiff
from tests.conftest import sample_patch_json, sample_data_frame
from pipeline.patch.patchfile import  Patch, PatchFile,  generate_patches

json_dict = {}
with open("patchfile_example.json", "r") as file_ptr:
     json_dict = json.load(file_ptr)

# patch_file = PatchFile.parse_patch_file_json_dict(json_dict)

patch_data = {
    "target": {"id": 1, "id2": 4},
    "deltas": {"value": 100, "status": "updated"},
}
data = {
    "id": [1, 2, 3],
    "id2": [3, 4, 5],
    "value": [50, 60, 70],
    "status": ["old", "old", "old"],
}
patch_data = {
     "target": {"id": 1, "id2": 4},
     "deltas": {"value": 100, "status": "updated"},
 }
target_data = {
        "id": [1, 2, 3],
        "id2": [3, 4, 5],
        "value": [100, 60, 70],
        "status": ["updated", "old", "old"]}
class TestPatch:
    def test_parse_patch_object_json_dict(sample_patch_json: dict[str, Any]):
        json_dict = {}
        with open("patchfile_example.json", "r") as file_ptr:
            json_dict = json.load(file_ptr)

        print(json_dict)

        patch_file = PatchFile.parse_patch_file_json_dict(json_dict)

        assert not DeepDiff(patch_file.__dict__, sample_patch_json, ignore_order=True)

    def test_apply(sample_data_frame):
        patch = Patch.parse_patch_object_json_dict(patch_data)
        patch.apply(data)
        assert data == target_data


  


class TestPatchFile:
    def test_parse_patch_file_json_dict():
        assert False

    def test_save_json():
        assert False


""" TODO - Cannibalize this into the test code
import json

import pandas as pd

from pipeline.patch.patchfile import Patch, PatchFile, generate_patches


# json_dict = {}
# with open("patchfile_example.json", "r") as file_ptr:
#     json_dict = json.load(file_ptr)

# patch_file = PatchFile.parse_patch_file_json_dict(json_dict)

# print(patch_file.get_json())


# # Example usage
# patch_data = {
#     "target": {"id": 1, "id2": 4},
#     "deltas": {"value": 100, "status": "updated"},
# }

# # Creating a Patch object from the patch_data
# patch = Patch.parse_patch_object_json_dict(patch_data)

# # Creating a DataFrame for demonstration
# data = {
#     "id": [1, 2, 3],
#     "id2": [3, 4, 5],
#     "value": [50, 60, 70],
#     "status": ["old", "old", "old"],
# }
# df = pd.DataFrame(data)

# # Applying the patch to the DataFrame
# patch.apply(df)

# print(df.to_string())


# Example usage
source_data = {
    "id": [1, 2, 3],
    "id2": [3, 4, 5],
    "value": [50, 60, 7],
    "status": ["old", "old", "old"],
}
target_data = {
    "id": [1, 2, 3],
    "id2": [3, 4, 5],
    "value": [100, 60, 70],
    "status": ["updated", "old", "old"],
}

source_df = pd.DataFrame(source_data)
target_df = pd.DataFrame(target_data)

id_columns = ["id", "id2"]

# Generate patches between source_df and target_df using id_columns
patches = generate_patches(source_df, target_df, id_columns)

# Display generated patches
for patch in patches:
    print("Target:", patch.target)
    print("Deltas:", patch.deltas)
    print()


"""
