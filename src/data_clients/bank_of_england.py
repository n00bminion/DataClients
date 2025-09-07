from common_utils.io_handler import external
from common_utils import config_handler
from data_clients import logger, _base
import pandas as pd
from datetime import datetime
import pytz
import re
from io import BytesIO


class BankOfEnglandIADBClient(_base.BaseDataClient):

    def __init__(self):
        super().__init__()

        # since we import this into other modules, this config won't work
        self.config = config_handler.get_config(
            "src/data_clients/config/bank_of_england.yaml", check_possible_paths=False
        )

        self.time_series_null_coverage_tolerance = (
            0.8  # 80% maximum null values tolerated
        )

        self.base_url = (
            "http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp?csv.x=yes"
        )
        self.timeout_seconds = 60

        self.payload = dict(
            Datefrom="01/Jan/2000",
            Dateto=None,
            SeriesCodes=None,
            CSVF="TN",
            UsingCodes="Y",
            VPD="Y",
            VFD="N",
        )

        self.headers = {
            "User-Agent": "Chrome/54.0.2840.90 Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"
        }

    @property
    def call_params(self):
        return {
            "params": self.payload,
            "headers": self.headers,
            "timeout": self.timeout_seconds,
        }

    def fill_payload_missing_attributes(self, series_codes, date_to):
        # checking bits here
        assert series_codes is not None, "SeriesCodes must be set"
        assert isinstance(
            series_codes, (str, list)
        ), f"SeriesCodes must be a comma-seperated string or a list of values. Values given is of type {type(series_codes)}."
        if date_to is not None:
            try:
                datetime.strptime(date_to, "%d/%b/%Y")
            except (ValueError, TypeError):
                raise ValueError(
                    f"Dateto must be in the format dd/Mon/yyyy. Value given is '{date_to}' of type {type(date_to)}."
                )

        # fix up and formatting here
        if isinstance(series_codes, str):
            self.payload["SeriesCodes"] = series_codes.replace(" ", "")

        if isinstance(series_codes, list):
            self.payload["SeriesCodes"] = ",".join(series_codes)

        logger.info(f"Payload SeriesCodes is set to '{self.payload['SeriesCodes']}'")

        self.payload["Dateto"] = date_to or datetime.now(tz=pytz.utc).strftime(
            "%d/%b/%Y"
        )

        logger.info(f"Payload Dateto is set to '{self.payload['Dateto']}'")
        logger.warning(
            f"Note that the payload in {self.__class__.__name__}.call_params is persisted after the call is made. "
            "This should not be a problem but if the call_params property is used directly, be aware of this."
        )

    def get_data(self, series_codes, date_to):
        # call_params reference self.payload which is updated here

        self.fill_payload_missing_attributes(series_codes, date_to)

        response = external.get_request(self.base_url, **self.call_params)
        logger.info(
            f"Successfully fetched data from Bank of England IADB for code(s): {series_codes}"
        )

        data = pd.read_csv(
            BytesIO(response.content),
            parse_dates=["DATE"],
            index_col=["DATE"],
        )

        logger.info("Successfully converted fetched data into DataFrame.")
        return data

    def get_mortgage_data(self, date_to=None):

        mortgage_code_mappings = self.config["mortgage-code"]
        series_codes = list(mortgage_code_mappings.keys())
        logger.info(f"Successfully loaded mortgage series codes metadata from config.")

        data = self.get_data(series_codes, date_to)

        data = (
            data.stack()
            .reset_index()
            .rename(columns={"level_1": "Code", "DATE": "Date", 0: "Rate"})
            .replace("..", None)
        )

        meta = (
            pd.DataFrame(self.config["mortgage-code"], index=["Description"])
            .T.rename_axis("Code")
            .reset_index()
        )

        meta = meta.assign(
            Year=meta["Description"]
            .map(lambda x: re.search("(?<=sterling)(.*)(?=year)", x)[0])
            .str.strip(),
            LTV=meta["Description"]
            .map(lambda x: re.search("(?<=year)(.*)(?=% LTV)", x)[0])
            .str.replace("(", "")
            .astype(int)
            .div(100),
        ).drop(columns=["Description"])

        data = (
            data.merge(meta, on=["Code"])
            .drop(columns=["Code"])
            .set_index(["Date", "Year", "LTV"])
            .unstack("Year")
            .unstack("LTV")
            .astype(float)
        )

        data.columns = data.columns.droplevel(level=0)
        logger.info("Successfully processed mortgage data.")

        # a bit backward but we get columns with null values coverage less than 80%
        # or put it the other way, requires minimum 20% of the values in the time series

        unsorted_tolerated_columns = [
            column
            for column in data.columns
            if sum(data[column].isna()) / len(data[column])
            < (self.time_series_null_coverage_tolerance)
        ]

        sorted_tolerated_columns = sorted(
            list(unsorted_tolerated_columns), key=lambda x: (int(x[0]), float(x[1]))
        )

        logger.info(
            f"Successfully filtered data to {len(sorted_tolerated_columns)} columns with null coverage tolerance of {self.time_series_null_coverage_tolerance * 100}%."
        )

        return data[sorted_tolerated_columns]

    def get_household_credit_data(self, series_codes):
        return


if __name__ == "__main__":
    client = BankOfEnglandIADBClient()
    data = client.get_mortgage_data()
    client.call_params
