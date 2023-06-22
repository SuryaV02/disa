import pandas


def cleanup_s4_hhcategory(df):
    # 7 responses are 'Used to be organic, but no longer'. There are also some blank responses. Both should be categorized as 'conventional'
    condition = (
        (df["s4_hhcategory"] == "")
        | (df["s4_hhcategory"] == "Used to be organic, but no longer")
        | (pandas.isna(df["s4_hhcategory"]))
    )

    # Print the count
    count = len(df[condition])
    print(f"Cleanup affecting {count} rows")

    # Do the replacement
    df.loc[condition, "s4_hhcategory"] = "S2S (organic in at least one plot)"
    return df


def merge_s4_cluster(df):
    # Collapse cluster names from below three rows into a single column
    df["s4_cluster"] = df["s4_cluster_ka"].fillna(
        df["s4_cluster_an"].fillna(df["s4_cluster_ku"])
    )

    new_df = df.drop(["s4_cluster_ka", "s4_cluster_an", "s4_cluster_ku"], axis=1)
    return new_df


def merge_s4_gp(df):
    # Collapse gp names into a single column
    df["s4_gp"] = df["s4_gp_ka1"].fillna(
        df["s4_gp_ka2"].fillna(
            df["s4_gp_ka3"].fillna(df["s4_gp_an1"].fillna(df["s4_gp_ku1"]))
        )
    )

    new_df = df.drop(
        ["s4_gp_ka1", "s4_gp_ka2", "s4_gp_ka3", "s4_gp_an1", "s4_gp_ku1"], axis=1
    )
    return new_df
