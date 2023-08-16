# DISA Cleanup and Analysis Pipeline 


Fully automated data preprocessing pipeline for future verification. Can be reused as a skeleton template for future data analysis projects.


Run it using
```
poetry install
poetry shell
python -m pipeline
```

## Pipeline Structure

### Process overview
1. Cleanup
2. Compute
3. 

#### Notes:
- Do not drop statistical outliers directly in cleanup here, because it compromises data integrity 


## Project Structure
```
pyproject.toml -> Requirements, etc.
pipeline
| - cleanup
| |  - cleanup_***.py -> Cleanup code for specific study within the dataset
| |  - cleanup_***.py
| |  - ...
| - __main__.py -> Entry point
| - utils.py -> Common Utility functions
lookup-tables
| - main.json -> Main column data renaming
| - ***.json
README.md
```

## clean_***.py Structure

1. Clean up for each study should be divided into separate files
2. Each column's cleanup logic should be in a separate function where the function should follow the following convention `<verb>_<column_name>(dataframe)->dataframe`, with the dataframe being the input and out of each of these methods. This enables easy chaining of methods.
3. The methods called inside `__main__.py` should be orderless, i.e. can be called in any order. That means that all the methods called from each of the cleanup files should call their corresponding cleanup function if they have a dependency.
4. 


| Type       | Naming Scheme                 | Versioning        | Rationale                                                                                                                                                              | Dataframe Copy | Order |
|------------|-------------------------------|-------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|-------|
| RAW inputs | data_v0001.tsv                | Serial            | To keep track of what input data we are getting                                                                                                                        | N/A            | 0     |
| Row Merged | data_row_merged-date-time.tsv | Date Time         | In case there are errors that we need to fix                                                                                                                           | No             | 1     |
| Cleanup    | data_cleanup_1                | None (Use latest) | Any replacement will be in place, mistakes are going to be traceable  New columns will have unique names that will break analysis scrips when run with old data schema | No             | 2     |
| Analysis   | data_analysis_1               | None(Use latest)  |                                                                                                                                                                        | No             | N/A   |

Pipeline:



1. Merging
2. Cleanup

    N/A
