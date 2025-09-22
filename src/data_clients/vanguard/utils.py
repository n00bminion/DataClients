import pandas as pd
import html
from functools import wraps


STR_TO_BOOL = {"false": False, "true": True}
STR_NUM_SCALE_TO_NUM = {"billion": 1e9, "million": 1e6, "thousand": 1e3}
CURR_SIGN_TO_CURR_NAME = {"£": "GBP"}


def assert_port_ids_list(function):
    allowed_type = (list, pd.Index)

    @wraps(function)
    def raise_exception(self, *args, **kwargs):
        param_name = "port_ids"
        if len(args) > 0:
            assert isinstance(
                args[0], allowed_type
            ), f"{type(args[0])} was passed in but the allowed type of product_ids are {allowed_type}"

            args = list(args)
            args[0] = args[0] if type(args[0]) == list else args[0].to_list()

        if param_name in kwargs.keys():
            param = kwargs[param_name]
            assert isinstance(
                param, allowed_type
            ), f"{type(param)} was passed in but the allowed type of product_ids are {allowed_type}"

            kwargs[param_name] = param if type(args[0]) == list else param.to_list()

        return function(self, *args, **kwargs)

    return raise_exception


def derive_asset_allocations_from_all_product_details(all_product_details):
    asset_allocations = (
        all_product_details["assetAllocations"]
        .explode()
        .apply(pd.Series)
        .set_index("label", append=True)
        .drop(columns=["code"])
    )
    derived_cash_allocation = (
        (100 - asset_allocations.groupby(level=0)["value"].sum())
        .where(lambda x: x != 0)
        .dropna()
        .to_frame()
        .assign(label="Cash")
        .set_index("label", append=True)
    )
    return pd.concat([asset_allocations, derived_cash_allocation], axis=0).sort_index()


def derive_historical_fund_distributions(all_product_details):
    historical_fund_distribution = (
        all_product_details.fundDataDistributionHistoryFundDistributionList.explode()
        .dropna()
        .apply(pd.Series)
        .set_index(["exDividendDate", "payableDate", "recordDate"], append=True)
    )
    return (
        historical_fund_distribution.join(
            historical_fund_distribution.mostRecent.apply(pd.Series).drop(
                columns=["value"]
            ),
            how="left",
        )
        .drop(columns=["mostRecent", "recordDateUnformatted"])
        .sort_index()
    )


def derive_total_assets_column(column):
    dfs = [
        # non na values we convert the billion/million suffix to number and multiply
        column[~column.isna()]
        .str.split(" ")
        .map(lambda x: float(x[0]) * STR_NUM_SCALE_TO_NUM.get(x[1].lower(), 1)),
        # concat with na values
        column[column.isna()],
    ]

    return pd.concat(
        [df for df in dfs if not df.empty],
        axis=0,
    )


def derive_fee_column(column):
    # these fee columns are a bit of an unknown as they are only None or '' so might
    # need to change logic in future if they get populated with values in future
    # but for now change them to float
    return column.str.replace("None", "").replace("", None).astype(float)


def derive_ocf_column(column):
    return column.str.rstrip("%").astype("float64") / 100


def derive_currency_column(column):
    return pd.concat(
        [
            column[~column.isna()].map(html.unescape),
            column[column.isna()],
        ],
        axis=0,
    )


def derive_tax_status_column(column):
    return column.fillna("N/A").str.upper()


def derive_numerical_float_column(column):
    return column.replace("", None).astype("float")


def derive_numerical_int_column(column):
    return column.str.replace(",", "").replace("", None).astype("Int32")


def derive_noa_value_column(column):
    return column.str.replace("-", "").replace("", None).astype("Int64")
