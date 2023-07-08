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


