from common.main import Main
from extractor.services.snowflake import SnowflakeSource
from common.json_loader import RecursiveNamespace

Main.initialize("test_snowflake_src")

job_id = "test-job-id"

source = {
    "snowflake": {
        "connection": {
            "account": "unscramblpartner.southeast-asia.azure",
            "user": "HSHRIVASTAVA",
            "password": "Asdf@1234",
            "warehouse": "DEMO_WH",
            "role": "ACCOUNTADMIN",
            "database": "snowflake",
            "schema": "account_usage"
        }
    },
    "batchSize": 100,
    "fromTimestamp": "2022-09-01",
    "schemaMapping": [
        {
            "input": "Cost",
            "output": "Cost",
        },
        {
            "input": "CostUSD",
            "output": "CostUSD",
        },
        {
            "input": "UsageDate",
            "output": "UsageDate",
        },
        {
            "input": "ResourceId",
            "output": "ResourceId",
        },
        {
            "input": "ResourceType",
            "output": "ResourceType",
        },
        {
            "input": "ResourceLocation",
            "output": "ResourceLocation",
        },
        {
            "input": "ResourceGroupName",
            "output": "ResourceGroupName",
        },
        {
            "input": "ServiceName",
            "output": "ServiceName",
        },
        {
            "input": "Meter",
            "output": "Meter",
        },
        {
            "input": "Currency",
            "output": "Currency",
        }
    ],
    "toTimestamp": "2022-09-30",
    "type": "snowflake"
}
with SnowflakeSource(job_id, RecursiveNamespace.map_entry(source)) as sd:
    sd.extract_and_transform(lambda x: print(x))