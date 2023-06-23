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
from pipeline.cleanup_h3 import calculate_h3_farmsize
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
    cleaned_df = calculate_h1_income_total(cleaned_df)
    cleaned_df = update_h1_exp_food(cleaned_df)
    cleaned_df = update_h1_exp_water_amt(cleaned_df)
    cleaned_df = update_h1_exp_healthcare_amt(cleaned_df)

    # Blanking out the exp_phone_unit when 'Other' is encountered
    cleaned_df = blank_ht_exp_phone_unit(cleaned_df)

    cleaned_df = update_h1_exp_phone_amt(cleaned_df)
    cleaned_df = update_h1_exp_educ_amt(cleaned_df)
    cleaned_df = update_h1_exp_rituals_amt(cleaned_df)
    cleaned_df = add_exp_other_specify_to_h1_exp_substance_amt(cleaned_df)

    # h3 Study
    # This one computes all the dependencies automatically
    # 1. calculate_h3_ownland()
    # 2. calculate_h3_plot1land()
    # 3. calculate_h3_plot2land()
    # 4. calculate_h3_plot3land()
    cleaned_df = calculate_h3_farmsize(cleaned_df)

    cleaned_df.to_csv("/root/sandbox/disa/data_cleaned.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
