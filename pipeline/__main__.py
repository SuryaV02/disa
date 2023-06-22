from pipeline.cleanup import merge_s4_cluster, cleanup_s4_hhcategory, merge_s4_gp
from pipeline.merge import merge_redcap_event_rows
import pandas as pd


def main():
    # merged_df = merge_redcap_event_rows('/root/sandbox/disa/data_v0004.tsv')
    merged_df = pd.read_csv(
        "/root/sandbox/disa/data_merged_21-06-2023-10:36:19.tsv", sep="\t", header=0
    )
    cleaned_df = cleanup_s4_hhcategory(merged_df)
    cleaned_df = merge_s4_cluster(cleaned_df)
    cleaned_df = merge_s4_gp(cleaned_df)
    cleaned_df.to_csv("/root/sandbox/disa/data_cleaned.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
