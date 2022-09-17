from common.kafka import KafkaSink
import json

from common.main import Main
from common.json_loader import RecursiveNamespace

Main.initialize("test_producer")
configuration = {
    "bootstrapServers": ["localhost:9092"],
    "topic": "MINI-DOG-JOBS"
}
s = KafkaSink(RecursiveNamespace.map_entry(configuration))
token="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSIsImtpZCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldCIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzIzMWE5Nzc0LWJjYTctNDM3OS04OGYzLTg1MDljODdjYjViYi8iLCJpYXQiOjE2NjM1OTA1NzIsIm5iZiI6MTY2MzU5MDU3MiwiZXhwIjoxNjYzNTk0OTg4LCJhY3IiOiIxIiwiYWlvIjoiQVRRQXkvOFRBQUFBblV0MWNUaDJUZ2I5bDA0Z0pZNWZ6VGFOM1NwaU80anUyK29jV2l6R3JaMDdOUUw4VmE4RjJDQlF4MkxYTzZCdSIsImFtciI6WyJwd2QiXSwiYXBwaWQiOiIxOGZiY2ExNi0yMjI0LTQ1ZjYtODViMC1mN2JmMmIzOWIzZjMiLCJhcHBpZGFjciI6IjAiLCJncm91cHMiOlsiMzI1Yjc4MmUtYWQwNi00MzA0LTgxMGUtMDIyODRlNWYzMWUzIl0sImlwYWRkciI6IjQ5LjIwNy4yMjIuMjIyIiwibmFtZSI6Ik1vaGl0IFNyaXZhc3RhdmEiLCJvaWQiOiIzZGFkZjBkNi0wNGNlLTQ4OWEtOWZjMi1lMzgxNDQwNTlkODQiLCJwdWlkIjoiMTAwMzIwMDA0NjRGREFCNyIsInJoIjoiMC5BVlFBZEpjYUk2ZThlVU9JODRVSnlIeTF1MFpJZjNrQXV0ZFB1a1Bhd2ZqMk1CTlVBRWMuIiwic2NwIjoidXNlcl9pbXBlcnNvbmF0aW9uIiwic3ViIjoia3dBemg0QVlsS3ctNW8wSVBoVHpoN3htZHNRdWxURURVOXZXTUh6WEliMCIsInRpZCI6IjIzMWE5Nzc0LWJjYTctNDM3OS04OGYzLTg1MDljODdjYjViYiIsInVuaXF1ZV9uYW1lIjoibW9oaXRAdW5zY3JhbWJsLmNvbSIsInVwbiI6Im1vaGl0QHVuc2NyYW1ibC5jb20iLCJ1dGkiOiJWWjk2RUdyNU5VdUFKcER2ZHNoVUFBIiwidmVyIjoiMS4wIiwid2lkcyI6WyJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXSwieG1zX3RjZHQiOjE0OTkxNDk1MDR9.sdlVcZFlaOVpp_3IX-IR01H-4w50nRJHa9-VoLKdVAEbqKfJCd217uTYPBw--7Lpd3f4vbKvmPeNEZu6XlnPdcAFn2VHQuw8m0ZdOMYqLGzr44nM1DTznBEFl2ZPdWph8PUymoTAqgDwmK5TE2qpi3n1ckYUaDlfKzZqEPXu7TmwqzO9kBqhboGGO2rrSMOlyR-0aXLEP8v5N_WwXldhe6qaOiJihE2n-aTsQhCaC4PJAHWCOFvU-bU9IuHerIrb_5Y726lJ0qFDoz3ZNZgMHcXxcvmkTNse3Oo7F44Ktzdm_xBjo8JCgLncXY89T9WILMD1Mcy4W9307J8ikIcw5A"
azure_data = {
    "jobId": "azure-test-job-id",
    "source": {
        "azure": {
            "connection": {
                "authToken": token,
                "subscriptionId": "24059f52-6d0a-4167-820e-c715818f7380"
            },
            "dimensions": [
                "ResourceId",
                "ResourceType",
                "ResourceLocation",
                "ResourceGroupName",
                "ServiceName",
                "Meter"
            ]
        },
        "batchSize": 100,
        "fromTimestamp": "2022-09-01T00:00:00.000Z",
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
        "toTimestamp": "2022-09-30T23:59:59.000Z",
        "type": "azure"
    },
    "destination": {
    "type": "snowflake",
    "snowflake": {
        "account": "unscramblpartner.southeast-asia.azure",
        "user": "HSHRIVASTAVA",
        "password": "Asdf@1234",
        "warehouse": "DEMO_WH",
        "role": "SYSADMIN"
	},
	"fullyQualifiedTableName": "MINI_DOG_COST.PUBLIC.AZURE_COST",
    "schemaMapping":
        [
            {
                "input": "Cost",
                "output": "COST"
            },
            {
                "input": "CostUSD",
                "output": "COST_USD"
            },
            {
                "input": "UsageDate",
                "output": "USAGE_DATE"
            },
            {
                "input": "ResourceId",
                "output": "RESOURCE_ID"
            },
            {
                "input": "ResourceType",
                "output": "RESOURCE_TYPE"
            },
            {
                "input": "ResourceLocation",
                "output": "RESOURCE_LOCATION"
            },
            {
                "input": "ResourceGroupName",
                "output": "RESOURCE_GROUP_NAME"
            },
            {
                "input": "ServiceName",
                "output": "SERVICE_NAME"
            },
            {
                "input": "Meter",
                "output": "METER"
            },
            {
                "input": "Currency",
                "output": "CURRENCY"
            }
        ]   
    }
}
snowflake_data = {
    "jobId": "snowflake-test-job-id",
    "source": {
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
                "input": "usage_date",
                "output": "UsageDate",
            },
            {
                "input": "warehouse_name",
                "output": "WarehouseName",
            },
            {
                "input": "service_type",
                "output": "ServiceType",
            },
            {
                "input": "total_credits_used_compute",
                "output": "TotalCreditsUsedCompute",
            },
            {
                "input": "total_credits_used_cloud_services",
                "output": "TotalCreditsUsedCloudServices",
            },
            {
                "input": "total_compute_cost",
                "output": "TotalComputeCost",
            },
            {
                "input": "total_cloud_services_cost",
                "output": "TotalCloudServicesCost",
            },
            {
                "input": "total_credits_used_storage",
                "output": "TotalCreditsUsedStorage",
            },
            {
                "input": "total_storage_cost",
                "output": "TotalStorageCost",
            }
        ],
        "toTimestamp": "2022-09-30",
        "type": "snowflake"
    },
    "destination": {
    "type": "snowflake",
    "snowflake": {
        "account": "unscramblpartner.southeast-asia.azure",
        "user": "HSHRIVASTAVA",
        "password": "Asdf@1234",
        "warehouse": "DEMO_WH",
        "role": "SYSADMIN"
	},
	"fullyQualifiedTableName": "MINI_DOG_COST.PUBLIC.SNOWFLAKE_COST",
    "schemaMapping":
        [
            {
                "output": "USAGE_DATE",
                "input": "UsageDate",
            },
            {
                "output": "WAREHOUSE_NAME",
                "input": "WarehouseName",
            },
            {
                "output": "SERVICE_TYPE",
                "input": "ServiceType",
            },
            {
                "output": "TOTAL_CREDITS_USED_COMPUTE",
                "input": "TotalCreditsUsedCompute",
            },
            {
                "output": "TOTAL_CREDITS_USED_CLOUD_SERVICES",
                "input": "TotalCreditsUsedCloudServices",
            },
            {
                "output": "TOTAL_COMPUTE_COST",
                "input": "TotalComputeCost",
            },
            {
                "output": "TOTAL_CLOUD_SERVICES_COST",
                "input": "TotalCloudServicesCost",
            },
            {
                "output": "TOTAL_CREDITS_USED_STORAGE",
                "input": "TotalCreditsUsedStorage",
            },
            {
                "output": "TOTAL_STORAGE_COST",
                "input": "TotalStorageCost",
            }
        ]   
    }
}
#print(json.dumps(len(RecursiveNamespace.map_entry(data).to_dict())))
s.send_message([json.dumps(snowflake_data)])