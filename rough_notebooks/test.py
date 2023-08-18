from pipeline.merge import merge_redcap_event_rows


merged_df = merge_redcap_event_rows("/root/sandbox/disa/data_v0004.tsv")
merged_df.to_csv("/root/sandbox/disa/data_merged_new.tsv", sep="\t", index=False)
