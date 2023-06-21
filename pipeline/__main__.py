from pipeline.merge import merge_redcap_event_rows


def main():
    merged_df = merge_redcap_event_rows('/root/sandbox/disa/data_v0004.tsv')
    cleaned_df = cleanup_col1(merged_df)
    cleaned_df = cleanup_col2(merged_df)

if __name__ == '__main__':
    main()
