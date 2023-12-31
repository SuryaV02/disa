import json
from pathlib import Path
from pipeline.cleanup_a10 import calculate_work_totals
from pipeline.cleanup_h1 import (
    add_exp_other_specify_to_h1_exp_substance_amt,
    blank_ht_exp_phone_unit,
    calculate_h1_income_total,
    update_h1_exp_educ_amt,
    update_h1_exp_food,
    update_h1_exp_healthcare_amt,
    update_h1_exp_phone_amt,
    update_h1_exp_rituals_amt,
    update_h1_exp_water_amt,
)
from pipeline.cleanup_h3 import (
    calcualte_h3_costofcult,
    calculate_h3_cropNland,
    calculate_h3_cultivateland,
    calculate_h3_farmsize,
    calculate_h3_plotNfert_synkg,
    classify_h3_fullyorganic,
    purge_h3_outliers,
)
from pipeline.cleanup_s4 import (
    merge_s4_cluster,
    cleanup_s4_hhcategory,
    merge_s4_gp,
    merge_s4_village,
)
from pipeline.merge import merge_redcap_event_rows
import pandas as pd

from pipeline.utils import (
    process_data_with_json,
    process_patch_files,
    generate_timestamp,
)

from pipeline import main_logger

CURRENT_DIR = Path(__file__).parent


def main():
    # Setup Logging

    raw_inputs_file = Path("/root/sandbox/disa/data_v0005.tsv")

    main_logger.info(f"Reading the raw inputs file: {raw_inputs_file} ...")
    raw_df = pd.read_csv(raw_inputs_file.absolute(), sep="\t", header=0)

    # Replace the data
    main_logger.info("Replacing the data values using the lookup table...")
    json_dict = {}
    with open(
        CURRENT_DIR.parent.joinpath("lookup-table/JSON_fields.json"), "r"
    ) as json_file_ptr:
        temp_json_dict = json.load(json_file_ptr)
        json_dict = {
            key: {int(rkey): rvalue for rkey, rvalue in value.items()}
            for key, value in temp_json_dict.items()
        }

    replaced_df = process_data_with_json(raw_df, json_dict)
    replaced_df.to_csv("data_replaced_latest.tsv", sep="\t", index=False)

    # Process the patches
    main_logger.info("Applying the patches to the data...")
    patched_df = process_patch_files(
        CURRENT_DIR.parent.joinpath("patches"), replaced_df
    )
    patched_df.to_csv("data_patcched_latest.tsv", sep="\t", index=False)

    # s4 Study
    main_logger.info("Cleaning study s4...")
    cleaned_df = cleanup_s4_hhcategory(patched_df)
    cleaned_df = merge_s4_cluster(cleaned_df)
    cleaned_df = merge_s4_gp(cleaned_df)
    cleaned_df = merge_s4_village(cleaned_df)

    # h1 Study
    main_logger.info("Cleaning study h1...")
    cleaned_df = calculate_h1_income_total(cleaned_df)
    cleaned_df = update_h1_exp_food(cleaned_df)
    cleaned_df = update_h1_exp_water_amt(cleaned_df)
    cleaned_df = update_h1_exp_healthcare_amt(cleaned_df)

    # Blanking out the exp_phone_unit when 'other' is encountered
    cleaned_df = blank_ht_exp_phone_unit(cleaned_df)

    cleaned_df = update_h1_exp_phone_amt(cleaned_df)
    cleaned_df = update_h1_exp_educ_amt(cleaned_df)
    cleaned_df = update_h1_exp_rituals_amt(cleaned_df)
    cleaned_df = add_exp_other_specify_to_h1_exp_substance_amt(cleaned_df)

    # a10 Study
    cleaned_df = calculate_work_totals(cleaned_df)

    # h3 Study
    cleaned_df = purge_h3_outliers(cleaned_df)
    # This one computes all the dependencies automatically
    # 1. calculate_h3_ownland()
    # 2. calculate_h3_plot1land()
    # 3. calculate_h3_plot2land()
    # 4. calculate_h3_plot3land()
    cleaned_df = calculate_h3_farmsize(cleaned_df)
    cleaned_df = calculate_h3_cultivateland(cleaned_df)
    cleaned_df = calcualte_h3_costofcult(cleaned_df)
    cleaned_df = calculate_h3_cropNland(cleaned_df)
    cleaned_df = classify_h3_fullyorganic(cleaned_df)
    # TODO: Figure out the order of operations here and potentially move it

    cleaned_df = calculate_h3_plotNfert_synkg(cleaned_df)

    cleaned_df.to_csv("/root/sandbox/disa/data_cleaned.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
