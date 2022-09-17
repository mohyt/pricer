from common.main import Main
from loader.services.snowflake import SnowflakeDestination
from common.json_loader import RecursiveNamespace

Main.initialize("test_snowflake")

job_id = "test-job-id"
values = [
    [1.144, 1.144, "2022-09-18", "/subscriptions/24059f52-6d0a-4167-820e-c715818f7380/resourcegroups/unscrambl-buildbot/providers/microsoft.dbformysql/servers/qbo-buildbot", "Microsoft.DBforMySQL/servers", "ap southeast", "unscrambl-buildbot", "Azure Database for MySQL", "2 vCore", "USD"],
    [0.000502956, 0.000502956, "2022-09-18", "/subscriptions/24059f52-6d0a-4167-820e-c715818f7380/resourcegroups/production-vulcan/providers/microsoft.storage/storageaccounts/productiondiag840", "Microsoft.Storage/storageAccounts", "in central", "production-vulcan", "Storage", "Read Operations", "USD"]
]
destination = {
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
data = {
     	"descriptors": [
            "Cost",
            "CostUSD",
            "UsageDate",
            "ResourceId",
            "ResourceType",
            "ResourceLocation",
            "ResourceGroupName",
            "ServiceName",
            "Meter",
            "Currency"
        ],
     	"values": values
    }              


with SnowflakeDestination(job_id, RecursiveNamespace.map_entry(destination)) as sd:
    sd.load(RecursiveNamespace.map_entry(data))