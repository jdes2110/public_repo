# %% Imports
import json
import datetime
from simple_salesforce import Salesforce
import pandas as pd
import os
import csv
from io import StringIO
from entities import SF_Credentials



# %% DatePresets class
class DatePresets:
    def __init__(self):
        self.date_presets = self._generate_date_presets()

    def _generate_date_presets(self):
        today = datetime.datetime.now(tz=datetime.timezone.utc)
        return {
            "last_30d": {"start": today - datetime.timedelta(days=30), "end": today},
            "last_7d": {"start": today - datetime.timedelta(days=7), "end": today},
            "last_24h": {"start": today - datetime.timedelta(days=1), "end": today},
            "last_week": {"start": today - datetime.timedelta(days=7), "end": today},
            "yesterday": {"start": today - datetime.timedelta(days=1), "end": today},
        }

    def get_presets(self):
        return self.date_presets

# %% SalesforceConnector class with singleton pattern
class SalesforceConnector:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SalesforceConnector, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "sf"):
            self.sf = self.connect_to_salesforce()

    def connect_to_salesforce(self):
        if not hasattr(self, 'SF_Credentials'):
            self.SF_Credentials = SF_Credentials()
        return Salesforce(
            username=self.SF_Credentials.SALESFORCE_USERNAME,
            password=self.SF_Credentials.SALESFORCE_PASSWORD,
            security_token=self.SF_Credentials.SALESFORCE_TOKEN,
            instance_url=self.SF_Credentials.SALESFORCE_INSTANCE,
        )

# %% LitifyObject class
class LitifyObject:
    def __init__(self, api_name):
        self.sf = SalesforceConnector().sf
        self.api_name = api_name
        self.description = self.get_description()
        self.label = self.description["label"]
        self.fields = self.get_all_fields()
        self.date_presets = DatePresets().get_presets()
        self.df_records = None
        self.recent_updated = None
        print("Object created")

    def get_description(self):
        print("Fetching description...", end="")
        self.description = self.sf.__getattr__(self.api_name).describe()
        print("Done")
        return self.description

    def get_all_fields(self):
        print("Fetching fields...", end="")
        self.fields = [field["name"] for field in self.description["fields"]]
        print("Done")
        return self.fields

    def csv_string_to_dict(self, csv_string):
        csv_file = StringIO(csv_string)
        reader = csv.DictReader(csv_file)
        return list(reader)

    def write_dict_results_in_file(self, result_in_dict, root_path, filename=None):
        if os.path.exists(f"{root_path}/{filename}.json") and os.path.exists(f"{root_path}/{filename}.csv"):
            os.remove(f"{root_path}/{filename}.json")
            os.remove(f"{root_path}/{filename}.csv")

        with open(f"{root_path}/{filename}.json", "a", encoding="utf-8-sig") as jsonfile, \
             open(f"{root_path}/{filename}.csv", "a", newline="", encoding="utf-8-sig") as csvfile:
                csv_writer = None
                for row in result_in_dict:
                    json.dump(row, jsonfile, indent=4)
                    jsonfile.write("\n")
                    if csv_writer is None:
                        csv_writer = csv.DictWriter(csvfile, fieldnames=row.keys())
                        csv_writer.writeheader()
                    csv_writer.writerow(row)

    def write_results_in_file(self, result, root_path, filename=None):
        if not os.path.exists(root_path):
            os.makedirs(root_path)

        if filename is None:
            filename = f'{self.api_name}_data'
            
        if isinstance(result, list):
            self.write_dict_results_in_file(result, root_path, filename)
        else:
            print('second condition')
            for x in result:
                print(f'type of x', type(x)) 
                list_results_in_dict = self.csv_string_to_dict(x)
                for dictionary in list_results_in_dict:
                    self.write_dict_results_in_file(dictionary, root_path, filename)

    def get_updated_records(self, date_preset: str) -> dict:
        from_date = self.date_presets[date_preset].get("start")
        to_date = self.date_presets[date_preset].get("end")
        start = from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end = to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        base_url = self.sf.__getattr__(self.api_name).base_url
        url = f"{base_url}updated/?start={start}&end={end}"
        response = self.sf.__getattr__(self.api_name)._call_salesforce(method='GET', url=url, headers=None).json()
        self.recent_updated = response
        return response

    def query_data_bulk1(self, fields_to_query=[], conditions_dict=None, date_range_query=None) -> pd.DataFrame:
        if date_range_query:
            start_date = date_range_query.get("start")
            end_date = date_range_query.get("end")
            root_path = f"results/{self.api_name}/by_date"
            filename = f"from_{start_date}_to_{end_date}_data"
        else:
            root_path = f"results/{self.api_name}/queries"
            today_formated = datetime.datetime.now().strftime("%Y-%m-%d")
            filename = f"{today_formated}_query_results"

        query = QueryConstructor(self.api_name, fields_to_query, conditions_dict, start_date, end_date).construct_query()
        results = self.sf.bulk.__getattr__(self.api_name).query(query, lazy_operation=True)

        if not os.path.exists(root_path):
            os.makedirs(root_path)
        for result in results:
            self.write_results_in_file(result, root_path, filename)

    def get_all_records(self, fields_to_query=[]) -> pd.DataFrame:
        print("Fetching data...", end="")
        if self.df_records is not None:
            return self.df_records

        fields_to_query = fields_to_query or self.fields
        query = QueryConstructor(self.api_name, fields_names=fields_to_query).construct_query()
        results = self.sf.bulk2.__getattr__(self.api_name).query(
            query=query,
            max_records=100000,
            column_delimiter="COMMA",
            line_ending="LF",
        )
        root_path = f"results/all_records/{self.api_name}/"
        filename = "all_records2"
        for result in results:
            self.write_results_in_file(result, root_path, filename)
        print("Done")

    def get_records(self, fields_to_query: list = None, conditions_dict: dict = None, date_preset: str = None, date_range: dict = None) -> pd.DataFrame:
        fields_names = fields_to_query or self.fields
        date_range_query = self.date_presets.get(date_preset) if date_preset else date_range

        if not any([fields_names, conditions_dict, date_preset, date_range]):
            return self.get_all_records()  # uses bulk2 api
        else:
            return self.query_data_bulk1(fields_names, conditions_dict, date_range_query)

# %% QueryConstructor class
class QueryConstructor:
    def __init__(self, api_name, fields_names=None, conditions_dict=None, start_date=None, end_date=None):
        self.api_name = api_name
        self.fields_names = fields_names
        self.conditions_dict = conditions_dict
        self.start_date = start_date
        self.end_date = end_date

    def construct_query(self) -> str:
        query_fields = self.construct_fields()
        query_where = self.construct_where()
        return f"{query_fields} FROM {self.api_name} {query_where}"

    def construct_fields(self) -> str:
        compound_fields = ["BillingAddress", "ShippingAddress", "Geolocation"]
        field_names = [field for field in self.fields_names if field not in compound_fields]
        return f"SELECT {', '.join(field_names)}"

    def format_date(self, date: datetime.datetime) -> str:
        return date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def construct_where(self) -> str:
        where_clauses = []
        if self.start_date and self.end_date:
            where_clauses.append(
                f"LastModifiedDate >= '{self.format_date(self.start_date)}' AND LastModifiedDate <= '{self.format_date(self.end_date)}'"
            )
        if self.conditions_dict:
            for field, condition in self.conditions_dict.items():
                condition_str = f"{field} IN ({', '.join([f'\'{item}\'' for item in condition])})"
                where_clauses.append(condition_str)
        return f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
