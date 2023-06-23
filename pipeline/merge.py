import pandas as pd
import re
from typing import Tuple, Dict, FrozenSet
import logging
from datetime import datetime

# Setup Logging
logging.basicConfig(
    filename="log_file_test.log",
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=logging.DEBUG
)

# Global Variables
COLUMNS_TO_DELETE = set()

def parse_redcap_event(event_name: str) -> Tuple[int, int]:
    # Returns the vist number and arm number
    pattern = r'visit_(\d+)_arm_(\d+)'
    matches = re.search(pattern, event_name)
    if matches is None:
        raise Exception(f"Unable to parse the string: \'{event_name}\' as a redcap event, please check the code / clean the data")
    return int(matches.group(1)), int(matches.group(2))

def generate_column_name(visit_number, arm_number, old_column_name) -> str:
    return f"{old_column_name}_visit_{visit_number}_arm_{arm_number}"

def merge_rows(selected_rows):
    # Overall Logic -  Merge the selected rows based on the logic:
    # 1. If the column values are mutually exculusive in all the given rows, just shift it all up
    # 2. If the column values exist in multiple rows, create new columns in the new_dataframe

    # Step 1 - Start by just copying the first row into the new DF
    # Step 2 - Now loop through all the columns in the other rows to see what needs to get done

    # Step 1 - Start by just copying the first row that we mutate and append
    # to the new Dataframe
    new_row = selected_rows.head(1).copy()

    # Step 2 - Now loop through all the columns except the first two since
    # This will be the main checking loop
    for column_name in list(selected_rows.columns)[2:]:
        # print(f"Checking Colmn:{column_name}")
        # Extract the column data
        column_data = list(selected_rows[column_name])

        # Check if its mutually excusive, compute how many values aren't NaN
        not_null_count = 0
        last_not_null_index = None
        for row_index, value in enumerate(column_data):
            if (pd.isna(value) or (value is None)) is False:
                not_null_count += 1
                last_not_null_index = row_index


        # If the count is > 1 we need to create new columns
        if not_null_count > 1:
            # raise NotImplementedError(f"Implement new column creation: Column: {column_name} hhid: {new_row['hhid']}")

            logging.info(column_data)
            logging.info(f"Marked column: {column_name} for moving data")

            COLUMNS_TO_DELETE.add(column_name)

        elif not_null_count == 1:
            logging.info(column_data)
            # Copy the data into the new dataframe
            if last_not_null_index is None:
                raise Exception("Last seen index is none")
            new_row[column_name] = column_data[last_not_null_index]
            logging.info(f"Moved data for column name: {column_name} for hhid: {new_row['hhid']}")
        elif not_null_count == 0:
            continue
        else:
            raise Exception(f"invalid not_null_count value: {not_null_count}")

    # Update the new row

    return new_row

def extend_columns(data_frame):
    # Extend the columns

    # Get all unique redcap events from the dataframe
    unique_redcap_events = data_frame['redcap_event_name'].unique()

    # Now loop through all the columns marked for deletion and generate the new columns
    new_columns = []
    for redcap_event in unique_redcap_events:
        visit_number, arm_number = parse_redcap_event(redcap_event)
        for old_column_name in list(COLUMNS_TO_DELETE):
            # Make the new name
            new_columns.append(generate_column_name(visit_number, arm_number, old_column_name))

    # Create an empty DataFrame with the new column names
    empty_df = pd.DataFrame(columns=new_columns)

    # Concatenate the empty DataFrame with the original DataFrame
    extended_df = pd.concat([data_frame, empty_df], axis=1)

    return extended_df

def trim_columns(hhid_list, original_data_frame, new_data_frame):
    new_data_frame = new_data_frame.copy()
    # Get all the rows with the corresponding HHID
    for current_hhid in hhid_list:
        selected_rows = original_data_frame[original_data_frame['hhid'] == current_hhid]

        # Perform moves for all the columns marked for deletion
        new_data_frame = move_data(current_hhid, selected_rows, new_data_frame)

    # Move all the data that is l
    # Drop all the old columns (that gor renamed)
    new_data_frame = new_data_frame.drop(list(COLUMNS_TO_DELETE), axis=1)
    logging.info(f"Deleted old column: {list(COLUMNS_TO_DELETE)}")
    return new_data_frame



def move_data(current_hhid, selected_rows, data_frame):
    # Go through each columns
    for column_name in list(COLUMNS_TO_DELETE):

        # Extract the column data
        column_data = list(selected_rows[column_name])

        for row_index, value in enumerate(column_data):

            # Only perform the check and addition
            if pd.isna(value) is True or (value is None) is True:
                continue

            # Step 1 - Generate the new column name
            event_name = selected_rows.iloc[row_index]['redcap_event_name']
            visit_number, arm_number = parse_redcap_event(event_name)
            new_column_name = generate_column_name(visit_number, arm_number, column_name)


            # Step 2 -  add the data directly as the new column, let pandas handle
            # indexing
            current_row = data_frame[data_frame['hhid'] == current_hhid]

            if new_column_name in data_frame.columns:
                # current_row[new_column_name] = value

                # Define the lookup condition
                condition = data_frame['hhid'] == current_hhid

                # Update the value in the 'C' column for the matching row
                data_frame.loc[condition, new_column_name] = value

                logging.info(f"Moved data to a new column: {new_column_name} for hhid: {current_row['hhid']} arm number: {arm_number} and value: {value}")
            else:
                raise KeyError(f"Could not find column {new_column_name}")
                # print(f"Generated a new Column name: {new_column_name}")
                # #column_df = pd.DataFrame({new_column_name: [value] })
                # current_row[new_column_name] = value
                # #current_row = pd.concat([current_row, column_df], axis=1)
                # logging.info(f"Added a new column: {new_column_name} for hhid: {current_row['hhid']} arm number: {arm_number} and value: {value}")

    return data_frame

def merge_redcap_event_rows(filename: str):
    """Runs the redcap event merge

    Args:
        filename (_type_): _description_
    """
    # Main execution code:

    # Load the csv as a pandas dataframe that makes it easy to manipulate and query
    # the data (we point out that the header is in row 0)
    dataframe_original = pd.read_csv(filename, sep='\t', header=0)



    # To be changed later on but we are taking the first 50 rows as our testing data
    df = dataframe_original #.head(50)



    # Generate a list of all unique hhid's, we will be using these are the means to
    # merge the rows

    unique_hhids = df['hhid'].unique()

    # Create a new dataframe where we store all the data
    new_df = pd.DataFrame(columns=df.columns)

    # Loop through each of the HHID's and then perform the merging for each hhid
    for i, current_hhid in enumerate(unique_hhids):
        print(current_hhid)
        # Get the rows corresponding to the given hhid
        selected_rows = df[df['hhid'] == current_hhid]
        # Now merge the rows with the same HHID in the new Dataframe
        new_row = merge_rows(selected_rows)
        print(f"Appending the row for HHID: {new_row['hhid']}")
        new_df = pd.concat([new_df, new_row], axis=0, ignore_index=True)

    # Extend all the columns
    extended_df = extend_columns(new_df)

    # Move all the data from columns marked to be deleted into their new columns,
    trimmed_df = trim_columns(unique_hhids, df, extended_df)

    date_time = datetime.now()
    str_date_time = date_time.strftime("%d-%m-%Y-%H:%M:%S")

    # Now save the new dataframe as a csv
    trimmed_df.to_csv(f'/root/sandbox/disa/data_merged_{str_date_time}.tsv', sep="\t", index=False)

    print("Deleted Columms:")
    print(COLUMNS_TO_DELETE)

    return trimmed_df
