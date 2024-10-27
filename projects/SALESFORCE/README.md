
# Salesforce Data Connector

This project is a Python-based solution for connecting to Salesforce, querying data, and storing results in CSV and JSON formats. It is designed for developers and data analysts working with Salesforce data. The solution supports date presets, query construction, and local storage of data.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Setup](#setup)
- [Usage](#usage)
  - [DatePresets](#datepresets)
  - [SalesforceConnector](#salesforceconnector)
  - [LitifyObject](#litifyobject)
  - [QueryConstructor](#queryconstructor)
- [Example Code](#example-code)
- [Requirements](#requirements)
- [License](#license)

---

## Overview

This project provides classes and methods to:
1. Securely connect to Salesforce.
2. Generate date-based presets for querying.
3. Construct dynamic Salesforce Object Query Language (SOQL) queries.
4. Retrieve, transform, and save data from Salesforce objects.

## Features

- **Salesforce Connection**: Establishes a secure connection to Salesforce.
- **Date Presets**: Provides commonly used date ranges (last 30 days, last 7 days, etc.).
- **Dynamic Query Construction**: Creates customized SOQL queries based on fields, conditions, and date filters.
- **Data Storage**: Exports data in both CSV and JSON formats for analysis and storage.

## Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your_username/salesforce_data_connector.git
   cd salesforce_data_connector
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Salesforce Credentials**:
   - Create a file `entities.py` with the following structure:

     ```python
     class SF_Credentials:
         SALESFORCE_USERNAME = 'your_username'
         SALESFORCE_PASSWORD = 'your_password'
         SALESFORCE_TOKEN = 'your_security_token'
         SALESFORCE_INSTANCE = 'your_instance_url'  # E.g., 'https://login.salesforce.com'
     ```

   - Alternatively, use environment variables to securely store credentials.

## Usage

### DatePresets

This class provides commonly used date range presets for querying records.

- **Example**:
  ```python
  date_presets = DatePresets()
  print(date_presets.get_presets())
  ```

### SalesforceConnector

A singleton class that manages a secure connection to Salesforce.

- **Example**:
  ```python
  sf_connector = SalesforceConnector()
  ```

### LitifyObject

Represents a Salesforce object (e.g., Account, Contact). It handles data fetching, file writing, and query management.

- **Methods**:
  - `get_updated_records(date_preset)`: Retrieves records updated within a date range.
  - `get_all_records(fields_to_query)`: Fetches all records based on specified fields.
  - `get_records(fields_to_query, conditions_dict, date_preset, date_range)`: General method to fetch records based on conditions, date presets, or date ranges.

- **Example**:
  ```python
  litify_object = LitifyObject('Account')
  records = litify_object.get_all_records()
  ```

### QueryConstructor

Creates SOQL queries based on selected fields, conditions, and date ranges.

- **Example**:
  ```python
  query_builder = QueryConstructor(
      api_name='Account',
      fields_names=['Id', 'Name'],
      conditions_dict={'Id': ['0011K00002BB5z1QAD']}
  )
  query = query_builder.construct_query()
  print(query)  # Outputs the constructed SOQL query
  ```

## Example Code

Below is a basic example showing how to initialize an object and query records:

```python
from main import LitifyObject, DatePresets

# Initialize a LitifyObject for an Account
account_obj = LitifyObject(api_name='Account')

# Fetch all Account records
account_obj.get_all_records()

# Fetch records updated in the last 7 days
recent_updates = account_obj.get_updated_records('last_7d')

# Query records with specific conditions
conditions = {'Id': recent_updates['ids']}
account_obj.get_records(conditions_dict=conditions)
```

## Requirements

- Python 3.7+
- Dependencies listed in `requirements.txt`:
  - `simple_salesforce`
  - `pandas`

Install dependencies with:
```bash
pip install -r requirements.txt
```

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
