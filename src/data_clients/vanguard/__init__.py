from common_utils.io_handler import external
from common_utils.data_handler.decorator import add_created_audit_columns
from data_clients import logger, _base
import pandas as pd
import re
from datetime import datetime
from data_clients.vanguard import utils


class VanguardClient(_base.BaseDataClient):

    def __init__(self, config_file="vanguard.yaml"):
        super().__init__(config_file)
        self.base_url = "https://www.vanguardinvestor.co.uk/api"
        self.base_graphql_url = "https://www.vanguardinvestor.co.uk/gpx/graphql"

    @add_created_audit_columns
    def get_all_product_general_details(self):
        response = external.get_request(f"{self.base_url}/productList")
        json_resp = response.json()
        logger.info("Successfully fetched all Vanguard products generic details")

        raw_data = pd.DataFrame(json_resp)
        stripped_data = (
            raw_data.assign(
                noaValue=utils.derive_noa_value_column(raw_data.noaValue),
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

        def get_product_comprehensive_details(product_id):
            response = external.get_request(f"{self.base_url}/funds/{product_id}")
            json_resp = response.json()
            logger.info(f"Successfully fetched product detail for {product_id=}")
            return json_resp

        # Combine all product details into a single DataFrame
        all_product_details_combined = pd.concat(
            map(
                pd.json_normalize,
                [
                    get_product_comprehensive_details(product_id)
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
                    utils.STR_TO_BOOL
                ),
                OCF=utils.derive_ocf_column(all_product_details_combined.OCF),
                purchaseFee=utils.derive_fee_column(
                    all_product_details_combined.purchaseFee
                ),
                redemptionFee=utils.derive_fee_column(
                    all_product_details_combined.redemptionFee
                ),
                totalAssets=utils.derive_total_assets_column(
                    all_product_details_combined.totalAssets
                ),
                totalAssetsCurrency=utils.derive_currency_column(
                    all_product_details_combined.totalAssetsCurrency
                ),
                taxStatus=utils.derive_tax_status_column(
                    all_product_details_combined.taxStatus
                ),
                marketPriceAmountChange=utils.derive_numerical_float_column(
                    all_product_details_combined.marketPriceAmountChange
                ),
                totalNetAssetsValue=utils.derive_total_assets_column(
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
                numberOfBonds=utils.derive_numerical_int_column(
                    all_product_details_combined.numberOfBonds
                ),
                numberOfStocks=utils.derive_numerical_int_column(
                    all_product_details_combined.numberOfStocks
                ),
                numberOfIssuers=utils.derive_numerical_int_column(
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
        asset_allocations = utils.derive_asset_allocations_from_all_product_details(
            all_product_details_combined
        )
        historical_fund_distributions = utils.derive_historical_fund_distributions(
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

    @utils.assert_port_ids_list
    def get_products_price_time_series(
        self, port_ids, start_date=None, end_date=None, load_historical=False
    ):
        today = datetime.now().strftime("%Y-%m-%d")

        if load_historical:
            start_date = self.config["default start date"]
            end_date = today

        if not start_date:
            start_date = today

        if not end_date:
            end_date = today

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
                    "portIds": port_ids,
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
            yield from zip(port_ids, response.json()["data"]["funds"])

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

    @add_created_audit_columns
    @utils.assert_port_ids_list
    def get_products_sector_exposure(self, port_ids):
        weight_exposure_query = """
            query WeightExposureQuery($portIds: [String!]!) {
                funds(portIds: $portIds) {
                    sectorDiversification {
                        sectorName
                        benchmarkPercent
                        fundPercent
                        date
                        __typename
                        }
                    __typename
                    }
                }
        """

        json_data = {
            "query": weight_exposure_query,
            "variables": {
                "portIds": port_ids,
                "limit": 0,
            },
            "operationName": "WeightExposureQuery",
        }

        response = external.post_request(
            self.base_graphql_url,
            json=json_data,
        )

        return pd.concat(
            [
                pd.DataFrame(data["sectorDiversification"]).assign(portId=portId)
                for portId, data in zip(port_ids, response.json()["data"]["funds"])
            ]
        )

    @utils.assert_port_ids_list
    def get_products_market_allocation(self, port_ids):
        market_allocation_query = """
            query MarketAllocationQuery($portIds: [String!], $holdingStatCodes: [HOLDING_STAT_CODES!]) {
                funds(portIds: $portIds) {
                    profile {
                    primaryMarketEquityClassification
                    __typename
                    }
                    marketAllocation(holdingStatCodes: $holdingStatCodes) {
                    countryName
                    regionName
                    fundMktPercent
                    benchmarkMktPercent
                    holdingStatCode
                    date
                    __typename
                    }
                    __typename
                }
                }
        """
        holding_stat_codes = ["FTCTYATPCS", "MSCTYATPCS"]
        json_data = {
            "query": market_allocation_query,
            "variables": {
                "holdingStatCodes": holding_stat_codes,
                "portIds": port_ids,
                "limit": 0,
            },
            "operationName": "MarketAllocationQuery",
        }

        response = external.post_request(
            self.base_graphql_url,
            json=json_data,
        )
        return pd.concat(
            [
                pd.DataFrame(data["marketAllocation"]).assign(
                    primaryMarketEquityClassification=data["profile"][
                        "primaryMarketEquityClassification"
                    ],
                    portId=portId,
                )
                for portId, data in zip(port_ids, response.json()["data"]["funds"])
            ]
        )

    @add_created_audit_columns
    @utils.assert_port_ids_list
    def get_products_region_exposure(self, port_ids):
        region_exposure_query = """
            query RegionExposureQuery($portIds: [String!]!) {
                funds(portIds: $portIds) {
                    regionExposure {
                    items {
                        regionName
                        effectiveDate
                        fundMktPercent
                        holdingStatCode
                        __typename
                    }
                    __typename
                    }
                    __typename
                }
            }
        """
        json_data = {
            "query": region_exposure_query,
            "variables": {
                "portIds": port_ids,
                "limit": 0,
            },
            "operationName": "RegionExposureQuery",
        }
        response = external.post_request(
            self.base_graphql_url,
            json=json_data,
        )

        return pd.concat(
            [
                pd.DataFrame(data["regionExposure"]["items"]).assign(portId=portId)
                for portId, data in zip(port_ids, response.json()["data"]["funds"])
            ]
        )

    @add_created_audit_columns
    @utils.assert_port_ids_list  # even though only 1 item can be passed, it still need to be wrapped in a list
    def get_product_holdings_details(self, port_id):

        if (len(port_id) > 1) or (len(port_id) == 0):
            raise ValueError(
                f"Only 1 port_id is allowed and must be wrapped in a list. {len(port_id)} port_ids was passed as parameter"
            )

        security_types = [
            # mutual funds
            "MF.MF",
            # fixed income
            "FI.ABS",
            "FI.CONV",
            "FI.CORP",
            "FI.IP",
            "FI.LOAN",
            "FI.MBS",
            "FI.MUNI",
            "FI.NONUS_GOV",
            "FI.US_GOV",
            # money markets
            "MM.AGC",
            "MM.BACC",
            "MM.CD",
            "MM.CP",
            "MM.MCP",
            "MM.RE",
            "MM.TBILL",
            "MM.TD",
            "MM.TFN",
            # stocks
            "EQ.DRCPT",
            "EQ.ETF",
            "EQ.FSH",
            "EQ.PREF",
            "EQ.PSH",
            "EQ.REIT",
            "EQ.STOCK",
            "EQ.RIGHT",
            "EQ.WRT",
        ]

        holding_details_query = """
            query HoldingDetailsQuery($portIds: [String!], $securityTypes: [String!], $lastItemKey: String) {
            borHoldings(portIds: $portIds) {
                holdings(limit: 1500, securityTypes: $securityTypes, lastItemKey: $lastItemKey) {
                totalHoldings
                lastItemKey
                items {
                    effectiveDate
                    marketValuePercentage
                    issuerName
                    securityLongDescription
                    couponRate
                    securityType
                    finalMaturity
                    __typename
                }
                __typename
                }
                __typename
            }
            funds(portIds: $portIds) {
                profile {
                assetClassificationLevel1
                __typename
                }
                __typename
            }
            }
        """

        json_data = {
            "query": holding_details_query,
            "variables": {
                "portIds": port_id,
                "lastItemKey": None,
                "securityTypes": security_types,
            },
            "operationName": "HoldingDetailsQuery",
        }

        holdings_list = []

        def _extract_holdings_data_from_response():
            while True:
                response = external.post_request(
                    self.base_graphql_url,
                    json=json_data,
                )

                holdings = response.json()["data"]["borHoldings"][0]["holdings"]
                total_holdings = holdings.get("totalHoldings", 0)
                new_last_item_key = holdings.get("lastItemKey", None)

                logger.info(
                    f"Successfully fetched {len(holdings_list)} holdings for port_id = {port_id[0]}. {len(holdings_list)/total_holdings:.2%} completed"
                )

                yield holdings["items"]

                if not new_last_item_key:
                    logger.info(
                        f"Successfully fetched all data. {total_holdings - len(holdings_list)} holdings did not get fetched. {1-len(holdings_list)/total_holdings:.2%} missing"
                    )
                    break

                # we assign the new last item key to the json payload so it can run in the next loop
                json_data["variables"]["lastItemKey"] = new_last_item_key
                logger.info(
                    f"Fetching for addition holdings for port_id = {port_id[0]}, {total_holdings - len(holdings_list)} holdings left to fetch. "
                )

        for holdings in _extract_holdings_data_from_response():
            holdings_list.extend(holdings)

        return pd.DataFrame(holdings_list).assign(portId=port_id[0])


if __name__ == "__main__":
    client = VanguardClient()
    port_ids = client.get_all_product_general_details().index
    data = client.get_products_market_allocation(port_ids)
    data

    # data = client.get_products_sector_exposure(["9678", ])
    # data
