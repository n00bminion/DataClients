from common_utils.io_handler import external
from common_utils.data_handler.decorator import add_created_audit_columns
from data_clients import logger, _base
import pandas as pd
import re
import html
from datetime import datetime


STR_TO_BOOL = {"false": False, "true": True}
STR_NUM_SCALE_TO_NUM = {"billion": 1e9, "million": 1e6, "thousand": 1e3}
CURR_SIGN_TO_CURR_NAME = {"£": "GBP"}


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


class VanguardAPIClient(_base.BaseDataClient):

    def __init__(self, config_file="vanguard.yaml"):
        super().__init__(config_file)
        self.base_url = "https://www.vanguardinvestor.co.uk/api"
        self.base_graphql_url = "https://www.vanguardinvestor.co.uk/gpx/graphql"

    def get_product_comprehensive_details(self, product_id):
        response = external.get_request(f"{self.base_url}/funds/{product_id}")
        json_resp = response.json()
        logger.info(f"Successfully fetched product detail for {product_id=}")
        return json_resp

    @add_created_audit_columns
    def get_all_product_general_details(self):
        response = external.get_request(f"{self.base_url}/productList")
        json_resp = response.json()
        logger.info("Successfully fetched all Vanguard products generic details")

        raw_data = pd.DataFrame(json_resp)
        stripped_data = (
            raw_data.assign(
                noaValue=derive_noa_value_column(raw_data.noaValue),
            )
            # remove dictionary columns
            .drop(
                columns=[
                    "annualReturns",
                    "annualReturnData",
                    "ids",
                    "inceptionDates",
                    "ocfValues",
                ]
            )
            .astype(
                dict(
                    # Id="string",
                    portId="string",
                    parentPortId="string",
                    allocation="string",
                    assetClass="string",
                    domicileType="string",
                    esgType="string",
                    fundCategory="string",
                    id="string",
                    inceptionDate="datetime64[ns]",
                    managementType="string",
                    name="string",
                    productType="string",
                    readyMadeType="string",
                    regionType="string",
                    riskLevel="Int64",
                    shareClass="string",
                    sedol="string",
                    fundGroupHedgedFunds="bool",
                    closeIndicator="bool",
                    upcomingChanges="string",
                )
            )
            .set_index(["portId"])
        )
        return stripped_data

    def get_all_product_details(self):
        all_products = self.get_all_product_general_details()

        # Combine all product details into a single DataFrame
        all_product_details_combined = pd.concat(
            map(
                pd.json_normalize,
                [
                    self.get_product_comprehensive_details(product_id)
                    for product_id in all_products.id
                ],
            )
        ).set_index("portId")

        all_product_details_combined.columns = [
            # anything after a dot, capitalize the first letter and remove the dot
            re.sub(r"(\.\w)", lambda w: w.group().upper(), column).replace(".", "")
            for column in all_product_details_combined.columns
        ]

        # remove columns that are not needed
        all_product_details_combined = (
            all_product_details_combined.drop(
                columns=[
                    "currencySymbol",
                    "siblings",
                    "marketPriceCurrencySymbol",
                    "navPriceCurrencySymbol",
                    "fundDataAnnualNAVReturnsReturns",
                    *[
                        column
                        for column in all_product_details_combined.columns
                        if "overviewLabels" in column
                    ],
                    "totalNetAssetsCurrencySymbol",
                    "navPriceValue",
                    "navPriceMmAmountChange",
                    "navPriceMmValue",
                    "navPricePercentChange",
                    "marketPricePercentChange",
                    "marketPriceValue",
                    "marketPriceMmAmountChange",
                    "marketPriceMmValue",
                    "navPriceAmountChange",
                ]
            )
            .assign(
                isGlobalBalanced=all_product_details_combined.isGlobalBalanced.str.lower().map(
                    STR_TO_BOOL
                ),
                OCF=derive_ocf_column(all_product_details_combined.OCF),
                purchaseFee=derive_fee_column(all_product_details_combined.purchaseFee),
                redemptionFee=derive_fee_column(
                    all_product_details_combined.redemptionFee
                ),
                totalAssets=derive_total_assets_column(
                    all_product_details_combined.totalAssets
                ),
                totalAssetsCurrency=derive_currency_column(
                    all_product_details_combined.totalAssetsCurrency
                ),
                taxStatus=derive_tax_status_column(
                    all_product_details_combined.taxStatus
                ),
                marketPriceAmountChange=derive_numerical_float_column(
                    all_product_details_combined.marketPriceAmountChange
                ),
                totalNetAssetsValue=derive_total_assets_column(
                    all_product_details_combined.totalNetAssetsValue
                ),
                benchmarkLabelsAboutTheBenchmark=all_product_details_combined.benchmarkLabelsAboutTheBenchmark.astype(
                    "string"
                ).replace(
                    "", None
                ),
                exclusionsCostsAndMinimumsPeerComparison=all_product_details_combined.exclusionsCostsAndMinimumsPeerComparison.convert_dtypes().fillna(
                    False
                ),
                exclusionsOverviewGrowthOf10k=all_product_details_combined.exclusionsOverviewGrowthOf10k.convert_dtypes().fillna(
                    False
                ),
                exclusionsPriceAndPerformanceGrowthOf10k=all_product_details_combined.exclusionsPriceAndPerformanceGrowthOf10k.convert_dtypes().fillna(
                    False
                ),
                numberOfBonds=derive_numerical_int_column(
                    all_product_details_combined.numberOfBonds
                ),
                numberOfStocks=derive_numerical_int_column(
                    all_product_details_combined.numberOfStocks
                ),
                numberOfIssuers=derive_numerical_int_column(
                    all_product_details_combined.numberOfIssuers
                ),
            )
            .astype(
                dict(
                    assetClass="string",
                    benchmark="string",
                    currencyCode="string",
                    displayName="string",
                    extendedFundType="string",
                    fundType="string",
                    id="string",
                    inceptionDate="datetime64[s]",
                    managementType="string",
                    name="string",
                    sedol="string",
                    fundGroupLifeStrategyFunds="bool",
                    distributionStrategyType="string",
                    distributionStrategyTypeDescription="string",
                    shareclassCode="string",
                    benchmarkNameFromECS="string",
                    cutOffTime="string",
                    shareClassCode="string",
                    shareClassCodeDescription="string",
                    shareclassDescription="string",
                    stampDutyReserveTax="string",
                    ticker="string",
                    totalAssetsCurrency="string",
                    totalAssetsAsOfDate="datetime64[s]",
                    upcomingChanges="string",
                    investmentStructure="string",
                    peerGroupAvg="float",
                    region="string",
                    sectorText="string",
                    marketPriceAsOfDate="datetime64[s]",
                    marketPriceCurrency="string",
                    navPriceAsOfDate="datetime64[s]",
                    navPriceCurrency="string",
                    riskValue="uint8",
                    riskAsOfDate="datetime64[s]",
                    fundDataProfileBenchmark="string",
                    fundDataLabelsInvestmentObjective="string",
                    fundDataLabelsInvestmentStrategy="string",
                    fundDataLabelsPortfolioManagerNoFirmDescription="string",
                    fundDataLabelsSdrOverseasFunds="string",
                    fundDataLabelsSdrNonLabelledFunds="string",
                    fundDataHighLowPricesNavPriceHigh="float",
                    fundDataHighLowPricesNavPriceLow="float",
                    fundDataDistributionHistoryPortId="string",
                    fundDataDistributionHistoryDistributionFrequency="string",
                    fundDataFeedetailsLabelFeePurchasetransactionfee="string",
                    fundDataFeedetailsLabelFeeStampdutyreservetax="string",
                    totalNetAssetsAsOfDate="datetime64[s]",
                    totalNetAssetsCurrency="string",
                    benchmarkTicker="string",
                    benchmarkLabelsBenchmarkNameLongWithoutCurrency="string",
                    fundDataHighLowPricesMarketPriceHigh="float",
                    fundDataHighLowPricesMarketPriceLow="float",
                    benchmarkLabelsBenchmarkDescription="string",
                    overviewFootnote="string",
                )
            )
        )

        # convert the yield dict into columns
        for column in (
            "fundDataDistributionHistoryYield",
            "netYield",
        ):
            converted_column = (
                all_product_details_combined[column].explode().dropna().apply(pd.Series)
            )
            converted_column = converted_column.rename(
                columns={
                    column_name: column + column_name[0].upper() + column_name[1:]
                    for column_name in converted_column.columns
                }
            )

            all_product_details_combined = all_product_details_combined.join(
                converted_column, how="left"
            ).drop(columns=[column])

        # extract the other stuff out into seperate tables
        asset_allocations = derive_asset_allocations_from_all_product_details(
            all_product_details_combined
        )
        historical_fund_distributions = derive_historical_fund_distributions(
            all_product_details_combined
        )

        final = all_products.drop(
            columns=list(set(all_products.columns) & set(all_product_details_combined))
        ).join(
            all_product_details_combined.drop(
                columns=[
                    "assetAllocations",
                    "fundDataDistributionHistoryFundDistributionList",
                ]
            ),
            how="left",
        )

        return (
            final,
            asset_allocations,
            historical_fund_distributions,
        )

    def get_products_price_time_series(
        self, product_ids, start_date=None, end_date=None, load_historical=False
    ):
        today = datetime.now().strftime("%Y-%m-%d")

        if load_historical:
            start_date = self.config["default start date"]
            end_date = today

        if not start_date:
            start_date = today

        if not end_date:
            end_date = today

        assert isinstance(
            product_ids, list
        ), f"{type(product_ids) = } was passed in but only a list of product_ids is allowed"

        price_detail_query = """
            query PriceDetailsQuery($portIds: [String!]!, $startDate: String!, $endDate: String!, $limit: Float) {
            funds(portIds: $portIds) {
                pricingDetails {
                navPrices(startDate: $startDate, endDate: $endDate, limit: $limit) {
                    items { 
                        price
                        asOfDate
                        currencyCode
                        __typename
                    }
                    __typename
                }
                marketPrices(startDate: $startDate, endDate: $endDate, limit: $limit) {
                    items {
                        portId
                        items {
                            price
                            asOfDate
                            currencyCode
                            __typename
                        }
                        __typename
                    }
                    __typename
                }
                __typename
                }
                __typename
            }
            }
        """

        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                "Incorrect date format, should be YYYY-MM-DD. Double check start/end date being passed in"
            )
        finally:
            json_data = {
                "query": price_detail_query,
                "variables": {
                    "portIds": product_ids,
                    "startDate": start_date,
                    "endDate": end_date,
                    "limit": 0,
                },
                "operationName": "PriceDetailsQuery",
            }

        response = external.post_request(
            self.base_graphql_url,
            json=json_data,
        )

        def _extract_price_data_from_response():
            yield from zip(product_ids, response.json()["data"]["funds"])

        nav_prices_list, market_prices_list = [], []

        for product_id, json_data in _extract_price_data_from_response():
            market_prices = json_data["pricingDetails"]["marketPrices"]["items"]
            nav_prices = json_data["pricingDetails"]["navPrices"]["items"]

            if not nav_prices:
                logger.warning(
                    f"No nav price available for {product_id} between {start_date=} and {end_date=}"
                )
            else:
                nav_prices_list.append(
                    pd.DataFrame(nav_prices).assign(portId=product_id)
                )
                del nav_prices

            if not market_prices:
                logger.warning(
                    f"No market price available for {product_id} between {start_date=} and {end_date=}"
                )

            else:
                # market prices is a bit different as it might have multiple ts
                market_prices_list.append(
                    pd.concat(
                        [
                            pd.DataFrame(child_market_price["items"]).assign(
                                portId=child_market_price["portId"],
                                parentPortId=product_id,
                            )
                            for child_market_price in market_prices
                        ]
                    )
                )
                del market_prices

            logger.info(
                f"Successfully retrieved and processed NAV and market prices for {product_id=} between {start_date=} and {end_date=}"
            )

        return nav_prices_list, market_prices_list


if __name__ == "__main__":
    client = VanguardAPIClient()
    data = client.get_all_product_general_details()

    price = client.get_products_price_time_series(data.index.to_list())
