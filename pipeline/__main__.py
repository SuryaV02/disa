from pipeline.cleanup_h1 import (
    update_h1_exp_educ_amt,
    update_h1_exp_food,
    update_h1_exp_healthcare_amt,
    update_h1_exp_phone_amt,
    update_h1_exp_rituals_amt,
    update_h1_exp_water_amt,
)
from pipeline.cleanup_s4 import (
    merge_s4_cluster,
    cleanup_s4_hhcategory,
    merge_s4_gp,
    merge_s4_village,
)
from pipeline.merge import merge_redcap_event_rows
import pandas as pd


def main():
    # merged_df = merge_redcap_event_rows('/root/sandbox/disa/data_v0004.tsv')
    merged_df = pd.read_csv(
        "/root/sandbox/disa/data_merged_21-06-2023-10:36:19.tsv", sep="\t", header=0
    )

    # s4 Study
    cleaned_df = cleanup_s4_hhcategory(merged_df)
    cleaned_df = merge_s4_cluster(cleaned_df)
    cleaned_df = merge_s4_gp(cleaned_df)
    cleaned_df = merge_s4_village(cleaned_df)

    # h1 Study
    cleaned_df = update_h1_exp_food(cleaned_df)
    cleaned_df = update_h1_exp_water_amt(cleaned_df)
    cleaned_df = update_h1_exp_healthcare_amt(cleaned_df)
    # cleaned_df = update_h1_exp_phone_amt(cleaned_df)
    cleaned_df = update_h1_exp_educ_amt(cleaned_df)
    cleaned_df = update_h1_exp_rituals_amt(cleaned_df)

    cleaned_df.to_csv("/root/sandbox/disa/data_cleaned.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
