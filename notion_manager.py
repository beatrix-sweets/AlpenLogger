import requests
import re
import os
from urllib.parse import urljoin, urlsplit

NOTION_KEY = "secret_uaHHd5to29CM0D1u5KkM43OORsyHWZ9GVgVQBLstLme"
GIRI_DB = "2b84c722b7c54f6faff82148fea7586c"
AVVENTURIERI_DB = "d17d959dc5014d939fe569c8b276991a"
SPESE_DB = "14e74f81fcc3468bbe19386060dfaf55"

class NotionClient():
    def __init__(self, notion_key):
        self.notion_key = notion_key
        self.default_headers = {'Authorization': f"Bearer {self.notion_key}",
                                'Content-Type': 'application/json', 'Notion-Version': '2022-06-28'}
        self.session = requests.Session()
        self.session.headers.update(self.default_headers)
        self.NOTION_BASE_URL = "https://api.notion.com/v1/"

    def query_database(self, db_id, filter_object=None, sorts=None, start_cursor=None, page_size=None):
        db_url = urljoin(self.NOTION_BASE_URL, f"databases/{db_id}/query")
        params = {}
        if filter_object is not None:
            params["filter"] = filter_object
        if sorts is not None:
            params["sorts"] = sorts
        if start_cursor is not None:
            params["start_cursor"] = start_cursor
        if page_size is not None:
            params["page_size"] = page_size

        return self.session.post(db_url, json=params)


class PandasConverter():
    date_regex = r"\d{4}-?\d{2}-?\d{2}"
    text_types = ["rich_text", "title"]

    def response_to_records(self, db_response):
        records = []
        for result in db_response["results"]:
            records.append(self.get_record(result))
        return records

    def get_record(self, result):
        record = {}
        record["id"] = result["id"]
        for name in result["properties"]:
            if self.is_supported(result["properties"][name]):
                record[name] = self.get_property_value(result["properties"][name])
        return record

    def is_supported(self, prop):
        if prop.get("type") in ["checkbox", "date", "number", "rich_text", "title", "multi_select", "select", "files", "formula", "rollup", "relation"]:
            return True
        else:
            return False

    def get_property_value(self, prop):
        prop_type = prop.get("type")
        if prop_type in self.text_types:
            return self.get_text(prop)
        elif prop_type == "date":
            return self.get_date(prop)
        elif prop_type == "select":
            return self.get_select(prop)
        elif prop_type == "multi_select":
            return self.get_multiselect(prop)
        elif prop_type == "files":
            return self.get_files(prop)
            #return None
        elif prop_type == "formula":
            return self.get_formula(prop)
        elif prop_type == "rollup":
            return self.get_rollup(prop)
        elif prop_type == "relation":
            return self.get_relation(prop)
        else:
            return prop.get(prop_type)

    def get_text(self, text_object):
        text = ""
        text_type = text_object.get("type")
        for rt in text_object.get(text_type):
            text += rt.get("plain_text")
        return text

    def get_date(self, date_object):
        date_value = date_object.get("date")
        if date_value is not None:
            date = date_value.get("start")
            try:
                dateonly = re.findall(self.date_regex, date)
                if len(dateonly) > 0:
                    return dateonly[0]
                else:
                    return date
            except Exception as e:
                print(e)
        return None

    def get_select(self, sel_object):
        sel_type = sel_object.get("type")
        rt = sel_object.get(sel_type)
        if rt is not None:
            sel_value = rt.get("name")
            if sel_value is not None:
                return sel_value
        return None

    def get_multiselect(self, sel_object):
        sel_values = []
        sel_type = sel_object.get("type")
        for rt in sel_object.get(sel_type):
            sel_value = rt.get("name")
            if sel_value is not None:
                sel_values.append(sel_value)

        if len(sel_values) > 0:
            return sel_values
        else:
            return None

    def get_files(self, files_object):
        filenames = []
        files_type = files_object.get("type")
        rt = files_object.get(files_type)
        for files in rt:
            fileprop = files.get("file")
            url = fileprop.get("url")
            r = requests.get(url, allow_redirects=True)
            filename = files.get("name")
            if not os.path.exists(os.getcwd() + "\\gpx\\"):
                os.makedirs(os.getcwd() + "\\gpx\\")
            open(os.getcwd() + "\\gpx\\" + filename, 'wb').write(r.content)
            filenames.append(filename)

        if len(filenames) > 0:
            return filenames
        else:
            return None

    def get_formula(self, formula_object):
        formula_type = formula_object.get("type")
        rt = formula_object.get(formula_type)
        if rt is not None:
            formula_type = rt.get("type")
            formula_value = rt.get(formula_type)
            if formula_value is not None:
                return formula_value
        return None

    def get_rollup(self, rollup_object):
        rollup_type = rollup_object.get("type")
        rt = rollup_object.get(rollup_type)
        if rt is not None:
            rollup_value = rt.get("number")
            if rollup_value is not None:
                return rollup_value
        return None

    def get_relation(self, rel_object):
        rel_values = []
        rel_type = rel_object.get("type")
        for rt in rel_object.get(rel_type):
            rel_value = rt.get("id")
            if rel_value is not None:
                rel_values.append(rel_value)

        if len(rel_values) > 0:
            return rel_values
        else:
            return None

import pandas as pd


class PandasLoader():
    def __init__(self, notion_client, pandas_converter):
        self.notion_client = notion_client
        self.converter = pandas_converter

    def load_db(self, db_id):
        page_count = 1
        print(f"Loading page {page_count}")
        db_response = self.notion_client.query_database(db_id)
        records = []
        if db_response.ok:
            db_response_obj = db_response.json()
            records.extend(self.converter.response_to_records(db_response_obj))

            while db_response_obj.get("has_more"):
                page_count += 1
                print(f"Loading page {page_count}")
                start_cursor = db_response_obj.get("next_cursor")
                db_response = self.notion_client.query_database(db_id, start_cursor=start_cursor)
                if db_response.ok:
                    db_response_obj = db_response.json()
                    records.extend(self.converter.response_to_records(db_response_obj))
        return pd.DataFrame(records)

client = NotionClient(NOTION_KEY)
converter = PandasConverter()
loader = PandasLoader(client, converter)
df = loader.load_db(GIRI_DB)
print(df)
df.to_csv('data_giri.csv')

df = loader.load_db(AVVENTURIERI_DB)
print(df)
df.to_csv('data_avventurieri.csv')

df = loader.load_db(SPESE_DB)
print(df)
df.to_csv('data_spese.csv')