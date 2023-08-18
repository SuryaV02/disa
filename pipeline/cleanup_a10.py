import re
import sys
from typing import Dict, List
import pandas as pd


TIME_USE_DICT: Dict[str, str] = {
    "Agriculture": "agri_work",
    "Care for livestock": "agri_work",
    "Eating": "non_work",
    "Household work": "other_work",
    "Other": "non_work",
    "Own business": "other_work",
    "Personal care": "non_work",
    "Shopping": "other_work",
    "Sleep/Rest": "non_work",
    "Socializing": "non_work",
    "Study": "non_work",
    "TV/Radio/Newspaper": "non_work",
    "Travel": "non_work",
    "Work/Job": "other_work",
}


def eliminate_undocumented_work_columns(df, column_names) -> pd.DataFrame:
    for column in column_names:
        drop_list = []
        if df[column].isna().all():
            drop_list.append(column)

    df = df.drop(drop_list, axis=1)
    print(f"Dropping column since its full of empty cells:")
    print(drop_list)
    return df


def calculate_work_totals(df) -> pd.DataFrame:
    # Generate variables as total number of hours spent in each category based on
    #  table in 'time_use' sheet. Note that data are collected in 30 minute
    #  blocks

    def calculate_totals(
        row,
        filtered_columns: List[str],
        lookup_table: Dict[str, str],
        total_categories_list: List[str],
        timeuse_df,
    ):
        # Now check for each event and all the corresponding columns if the values are valid or if needs to be skipped
        if row[filtered_columns].isna().any():
            print(
                f"Found missing time use data for: HHID - {row['hhid']} event - {row['redcap_event_name']}. Missing Count - {row[filtered_columns].isna().count()}"
            )
            timeuse_row = {
                "hhid": row["hhid"],
                "redcap_event_name": row["redcap_event_name"],
                "missing_count": row[filtered_columns].isna().count(),
            }

            for column_name in filtered_columns:
                timeuse_row[column_name] = row[column_name]

            # print(timeuse_row)
            new_df = pd.Series(timeuse_row)
            # timeuse_df = timeuse_df.concat(timeuse_row, ignore_index=True)
            timeuse_df = pd.concat([timeuse_df, new_df], axis=0, ignore_index=True)

        else:
            # Compute the total time metrics
            totals_dict = {key: 0 for key in total_categories_list}
            for column_name in filtered_columns:
                user_reported_work_type = row[column_name]
                computed_work_type = lookup_table[user_reported_work_type]
                totals_dict[computed_work_type] += 0.5

            for work_type, value in totals_dict.items():
                row[f"total_{work_type}"] = value
            print(
                f"Computed total time use data for: HHID - {row['hhid']} event - {row['redcap_event_name']}",
                totals_dict,
            )
        return row

    # This is the regex pattern for the column
    pattern = r"a10_\d{4}"

    filtered_columns = [column for column in df.columns if re.match(pattern, column)]

    # Eliminate all the empty columns
    # df = eliminate_undocumented_work_columns(df, filtered_columns)

    # Put all the summation categories into a set
    categories_set = set()
    for _, value in TIME_USE_DICT.items():
        categories_set.add(value)

    # Now generate all the possible summation column headers
    summation_headers = []
    for category in list(categories_set):
        summation_headers.append(f"total_{category}")

    print("summation headers:")
    print(summation_headers)

    # Extend the columns
    # Create an empty DataFrame with the new column names
    empty_df = pd.DataFrame(
        columns=summation_headers,
    )

    # Concatenate the empty DataFrame with the original DataFrame
    extended_df = pd.concat([df, empty_df], axis=1)

    print(filtered_columns)

    timeuse_column_list = [
        "hhid",
        "redcap_event_name",
        "missing_count",
    ]
    timeuse_column_list.extend(filtered_columns)

    print(timeuse_column_list)
    # Create timeuse data
    timeuse_df = pd.DataFrame({key: [] for key in timeuse_column_list})

    print(filtered_columns)
    df = extended_df.apply(
        calculate_totals,
        filtered_columns=filtered_columns,
        lookup_table=TIME_USE_DICT,
        total_categories_list=list(categories_set),
        timeuse_df=timeuse_df,
        axis=1,
    )

    print("Final Dataframe:")
    print(timeuse_df)

    return df
