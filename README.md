# Zomato-Data-Analysis

Food Delivery App Data Analysis using Python and Integrating the data to the target database(Oracle)


![Logo](https://www.netsolutions.com/insights/wp-content/uploads/2021/11/essential-feature-of-building-an-on-demand-food-ordering-app.jpg)


## Description

This project aims to perform data analysis on Zomato restaurant data and create a pipeline for storing the cleaned data in an Oracle Database. The pipeline is designed to update the target file in the database whenever there is a modification in the source file.


## Files in the Repository

The repository contains the following files:

1. **zomato.csv**
   - This file contains the uncleaned data extracted from Zomato.

2. **cleaning_script**
   - This script is responsible for cleaning and preprocessing the Zomato data.

3. **pipeline_Auto**
   - The dynamic pipeline script that stores the cleaned data into the Oracle Database. Users need to provide necessary details through the command line for execution.
   Example:
   ```bash
   python pipeline_Auto.py <oracle_user> <oracle_password> <oracle_host> <oracle_port> <oracle_sid> <csv_file_path>
   ```
   
4. **runQuery.py**
   - This script reads a query written in a .txt file and outputs the result in a .csv file. Similar to the pipeline script, it is designed to be dynamic and accepts input through the command line.
   Example:
   ```bash
   python runQuery.py <oracle_user> <oracle_password> <oracle_host> <oracle_port> <oracle_sid> <query_file_path>
   ```


## Installation

To use this project, you need the following dependencies:

- **Python 3.x**: If you don't have Python 3.x installed, you can download it from the [Python official website](https://www.python.org/downloads/).

- **Oracle Database**: Ensure you have access to an Oracle Database where you can store the cleaned data.

- **Python packages**: You'll need to install the following Python packages:

  - `pandas`: Used for data manipulation and analysis.
  - `cx_Oracle`: Required for database connectivity with Oracle Database.

To install the Python packages, you can use the following commands:

```bash
# Install pandas
pip install pandas

# Install cx_Oracle
pip install cx_Oracle
```


## Usage

1. Cleaning Script:

  - Execute the cleaning script on the zomato.csv file to prepare the data for further analysis.

2. Pipeline Script:

  - Run the pipeline script, providing Oracle database credentials and the file paths as command-line arguments to store the cleaned data.

Example:

```bash
python pipeline_Auto.py user pass host port sid path/to/zomato.csv
```
3. Query Execution:

  - Use runQuery.py to execute queries on the Oracle database. Provide the query file path and the desired output CSV file path.

Example:

```bash
python runQuery.py user pass host port sid path/to/query.txt
```


## Contributing

If you would like to contribute to this project, please follow these steps:

- Fork the repository.
- Create a new branch for your feature or bug fix.
- Make changes and commit them.
- Push to your fork and submit a pull request.
