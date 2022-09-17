"""Implements the Azure data extractions service"""

from datetime import datetime

import pandas as pd
from common.main import Main
from common.rest import RestClient
from extractor.services.base import AbstractSource


class AzureSource(AbstractSource):
    """Implements the Azure Source"""

    _AZURE_DEFAULT_DATE_FORMAT = "%Y%m%d"
    _DATE_FORMAT = "%Y-%m-%d"

    def __init__(self, job_id, source):
        self._job_id = job_id
        connection = source.azure.connection
        self._input_schema_2_output_schema = {}
        for schema_mapping in source.schemaMapping:
            self._input_schema_2_output_schema[schema_mapping.input] = schema_mapping.output
        self._azure_client = RestClient(bearer_token=connection.authToken)
        self._cost_management_url = f"https://management.azure.com/subscriptions/{connection.subscriptionId}/providers/Microsoft.CostManagement/query?api-version=2021-10-01&$top={source.batchSize}"
        groupings = []
        for dimension in source.azure.dimensions:
            groupings.append({
                "type": "Dimension",
                "name": dimension
            })
        self._cost_retrieval_payload = {
            "type": "ActualCost",
            "dataSet": {
                "granularity": "Daily",
                "aggregation": {
                    "totalCost": {
                        "name": "Cost",
                        "function": "Sum"
                    },
                    "totalCostUSD": {
                        "name": "CostUSD",
                        "function": "Sum"
                    }
                },
                "grouping": groupings
            },
            "timeframe": "Custom",
            "timePeriod": {
                "from": f"{source.fromTimestamp}",
                "to": f"{source.toTimestamp}"
            }
    }

    def _to_dataframe(self, properties, url):
        """Parses the data into data frame"""
        Main.logger().debug(
            "parsing the data from the azure using %s of the %s job", url, self._job_id)
        result = []
        columns = properties.columns
        for record in properties.rows:
            usage_record = {}
            for index, value in enumerate(record):
                descriptor = columns[index].name
                usage_record[descriptor] = value
            result.append(usage_record)
        Main.logger().debug(
            "parsed the data from the azure using %s of the %s job", url, self._job_id)
        return pd.DataFrame(result)
    
    def _extract(self, url):
        """Extracts the data from the Azure"""
        Main.logger().debug(
            "extracing the data from the azure using %s of the %s job", url, self._job_id)
        response_data = self._azure_client.send_post_request(url, self._cost_retrieval_payload)
        Main.logger().debug(
            "extracted the data from the azure using %s of the %s job", url, self._job_id)
        next_link = response_data.properties.nextLink
        self._cost_management_url = next_link if next_link and len(next_link) > 0 else None
        return self._to_dataframe(response_data.properties, url), url
    
    @staticmethod
    def _convert_string_to_datetime(value):
        """Converts date time string to proper datetime string"""
        date_value = datetime.strptime(str(value), AzureSource._AZURE_DEFAULT_DATE_FORMAT)
        return date_value.strftime(AzureSource._DATE_FORMAT)

    def _transform(self, dataframe, url):
        """Transforms the data from the Azure"""
        Main.logger().debug(
            "transforming the data from the azure using %s of the %s job", url, self._job_id)
        dataframe["UsageDate"] = dataframe["UsageDate"].apply(lambda x: AzureSource._convert_string_to_datetime(x))
        Main.logger().debug(
            "transformed the data from the azure using %s of the %s job", url, self._job_id)

    def extract_and_transform(self, result_handler):        
        """Implements the extraction and performs transformation"""
        dataframe, url = self._extract(self._cost_management_url)
        self._transform(dataframe, url)
        descriptors = []
        for key in list(dataframe.keys()):
            descriptors.append(self._input_schema_2_output_schema[key])
        result_handler({
            "descriptors": descriptors,
            "values": dataframe.values.tolist()
        })
        if self._cost_management_url is None:
            return
        self.extract_and_transform(result_handler)
