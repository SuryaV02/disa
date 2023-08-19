import pandas as pd
import pytest


@pytest.fixture
def sample_patch_json():
    ret = {
        "patches": [
            {
                "target": {"hhid": "hhid-0001", "redcap_event_name": "default_event"},
                "deltas": {"col_A": "new_value_A", "col_B": "new_value_B"},
            }
        ],
        "version": 1,
        "meta": {
            "notes": "Give the comments and other notes necessary for this. Each of the objects in the patches will be a single change we need to have"
        },
    }

    return ret


def sample_data_frame():
    return pd.DataFrame()
