import re
from typing import Dict
import pandas as pd
from pipeline import main_logger


def cleanup_s4_hhcategory(df) -> pd.DataFrame:
    # 7 responses are 'Used to be organic, but no longer'. There are also some blank responses. Both should be categorized as 'conventional'
    condition = (
        (df["s4_hhcategory"] == "")
        | (df["s4_hhcategory"] == "Used to be organic, but no longer")
        | (pd.isna(df["s4_hhcategory"]))
    )

    # Print the count
    count = len(df[condition])
    main_logger.info(f"Cleanup affecting {count} rows")

    # Do the replacement
    df.loc[condition, "s4_current_hhcategory"] = "S2S (organic in at least one plot)"
    return df


def merge_s4_cluster(df) -> pd.DataFrame:
    # Collapse cluster names from below three rows into a single column
    df["s4_cluster"] = df["s4_cluster_ka"].fillna(
        df["s4_cluster_an"].fillna(df["s4_cluster_ku"])
    )

    # Drop old columns
    new_df = df.drop(["s4_cluster_ka", "s4_cluster_an", "s4_cluster_ku"], axis=1)
    return new_df


def merge_s4_gp(df) -> pd.DataFrame:
    # Collapse gp names into a single column
    df["s4_gp"] = df["s4_gp_ka1"].fillna(
        df["s4_gp_ka2"].fillna(
            df["s4_gp_ka3"].fillna(df["s4_gp_an1"].fillna(df["s4_gp_ku1"]))
        )
    )

    # Drop old columns
    new_df = df.drop(
        ["s4_gp_ka1", "s4_gp_ka2", "s4_gp_ka3", "s4_gp_an1", "s4_gp_ku1"], axis=1
    )
    return new_df


def merge_s4_village(df) -> pd.DataFrame:
    # Collapse village names into a single column

    """Columns to merge
    s4_village_ka11
    s4_village_ka12
    s4_village_ka13
    s4_village_ka21
    s4_village_ka22
    s4_village_ka23
    s4_village_ka24
    s4_village_ka31
    s4_village_ka32
    s4_village_ka33
    s4_village_ka34
    s4_village_an11
    s4_village_an12
    s4_village_an13
    s4_village_ku11
    s4_village_ku12
    s4_village_ku13
    """

    df["s4_village"] = df["s4_village_ka11"].fillna(
        df["s4_village_ka12"].fillna(
            df["s4_village_ka13"].fillna(
                df["s4_village_ka21"].fillna(
                    df["s4_village_ka22"].fillna(
                        df["s4_village_ka23"].fillna(
                            df["s4_village_ka24"].fillna(
                                df["s4_village_ka31"].fillna(
                                    df["s4_village_ka32"].fillna(
                                        df["s4_village_ka33"].fillna(
                                            df["s4_village_ka34"].fillna(
                                                df["s4_village_an11"].fillna(
                                                    df["s4_village_an12"].fillna(
                                                        df["s4_village_an13"].fillna(
                                                            df[
                                                                "s4_village_ku11"
                                                            ].fillna(
                                                                df[
                                                                    "s4_village_ku12"
                                                                ].fillna(
                                                                    df[
                                                                        "s4_village_ku13"
                                                                    ]
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    )

    # Drop old columns
    new_df = df.drop(
        [
            "s4_village_ka11",
            "s4_village_ka12",
            "s4_village_ka13",
            "s4_village_ka21",
            "s4_village_ka22",
            "s4_village_ka23",
            "s4_village_ka24",
            "s4_village_ka31",
            "s4_village_ka32",
            "s4_village_ka33",
            "s4_village_ka34",
            "s4_village_an11",
            "s4_village_an12",
            "s4_village_an13",
            "s4_village_ku11",
            "s4_village_ku12",
            "s4_village_ku13",
        ],
        axis=1,
    )
    return new_df
