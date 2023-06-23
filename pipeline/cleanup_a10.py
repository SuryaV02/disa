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


def eliminate_undocumented_work_columns(df, column_names):
    for column in column_names:
        drop_list = []
        if df[column].isna().all():
            drop_list.append(column)

    df = df.drop(drop_list, axis=1)
    print(f"Dropping column since its full of empty cells:")
    print(drop_list)
    return df


def calculate_work_totals(df):
    # Generate variables as total number of hours spent in each category based on
    #  table in 'time_use' sheet. Note that data are collected in 30 minute
    #  blocks

    def calculate_totals(
        row,
        filtered_columns: List[str],
        match_pattern: str,
        lookup_table: Dict[str, str],
        summation_headers: List[str],
        event_list: List[str],
        total_categories_list: List[str],
    ):
        # Create dictionary for all column names for every event
        event_lookup_dict: Dict[str, List[str]] = {}
        # Loop through events that fit the pattern
        for event in event_list:
            # Go through all the columns that match the pattern
            event_lookup_dict[event] = []

        # Sort the columns into event lookup dict
        for column_name in filtered_columns:
            match = re.match(pattern, column_name)
            event = match.group(1)
            event_lookup_dict[event].append(column_name)
        # print(event_lookup_dict)
        # sys.exit()
        # Now check for each event and all the corresponding columns if the values are valid or if needs to be skipped
        for event, column_list in event_lookup_dict.items():
            if row[column_list].isna().any():
                print(
                    f"Found missing time use data for: HHID - {row['hhid']} event - {event}"
                )
                print(row[column_list])
            else:
                # Compute the total time metrics
                totals_dict = {key: 0 for key in total_categories_list}
                for column_name in column_list:
                    user_reported_work_type = row[column_name]
                    computed_work_type = lookup_table[user_reported_work_type]
                    match = re.match(match_pattern, column_name)
                    totals_dict[computed_work_type] += 0.5

                for work_type, value in totals_dict.items():
                    row[f"total_{work_type}{event}"] = value

        return row

    # This is the regex pattern for the column
    pattern = r"a10_\d{4}(_visit_(\d+)_arm_(\d+))?"

    filtered_columns = [column for column in df.columns if re.match(pattern, column)]

    # Eliminate all the empty columns
    # df = eliminate_undocumented_work_columns(df, filtered_columns)

    # Generate all the event suffixes
    event_set = set()
    for column_name in filtered_columns:
        match = re.match(pattern, column_name)
        if match:
            if match.group(1):
                event_set.add(match.group(1))

    # Put all the summation categories into a set
    categories_set = set()
    for _, value in TIME_USE_DICT.items():
        categories_set.add(value)

    # Now generate all the possible summation column headers
    summation_headers = []
    for category in list(categories_set):
        for suffix in event_set:
            summation_headers.append(f"total_{category}{suffix}")

    print("summation headers:")
    print(summation_headers)

    # Extend the columns
    # Create an empty DataFrame with the new column names
    empty_df = pd.DataFrame(
        columns=summation_headers,
    )

    # Concatenate the empty DataFrame with the original DataFrame
    extended_df = pd.concat([df, empty_df], axis=1)

    df = extended_df.apply(
        calculate_totals,
        filtered_columns=filtered_columns,
        match_pattern=pattern,
        lookup_table=TIME_USE_DICT,
        summation_headers=summation_headers,
        event_list=list(event_set),
        total_categories_list=list(categories_set),
        axis=1,
    )

    return df
