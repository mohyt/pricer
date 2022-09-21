from common.main import Main
from common.kafka import KafkaSink
from extractor.services.shopify import ShopifySource
from common.json_loader import RecursiveNamespace
import json

Main.initialize("test_shopify")

job_id = "test-job-id"
source = {
    "shopify": {
        "urls": ["https://www.pogocycles.ie"]
    },
    "schemaMapping": [
        {
            "input": "available",
            "output": "available"
        },
        {
            "input": "category",
            "output": "category"
        },
        {
            "input": "code",
            "output": "code"
        },
        {
            "input": "collection",
            "output": "collection"
        },
        {
            "input": "grams",
            "output": "grams"
        },
        {
            "input": "imageUrl",
            "output": "imageUrl"
        },
        {
            "input": "name",
            "output": "name"
        },
        {
            "input": "price",
            "output": "price"
        },
        {
            "input": "requiresShipping",
            "output": "requiresShipping"
        },
        {
            "input": "url",
            "output": "url"
        },
        {
            "input": "variantName",
            "output": "variantName"
        }
    ],
    "type": "shopify"
}

destination = {
    "type": "snowflake",
    "snowflake": {
        "account": "unscramblpartner.southeast-asia.azure",
        "user": "HSHRIVASTAVA",
        "password": "Asdf@1234",
        "warehouse": "DEMO_WH",
        "role": "SYSADMIN"
    },
    "fullyQualifiedTableName": "PRICER.PUBLIC.SHOPIFY_PRODUCTS",
    "schemaMapping": [
        {
            "input": "available",
            "output": "AVAILABLE"
        },
        {
            "input": "category",
            "output": "CATEGORY"
        },
        {
            "input": "code",
            "output": "CODE"
        },
        {
            "input": "collection",
            "output": "COLLECTION"
        },
        {
            "input": "grams",
            "output": "GRAMS"
        },
        {
            "input": "imageUrl",
            "output": "IMAGE_URL"
        },
        {
            "input": "name",
            "output": "NAME"
        },
        {
            "input": "price",
            "output": "PRICE"
        },
        {
            "input": "requiresShipping",
            "output": "REQUIRES_SHIPPING"
        },
        {
            "input": "url",
            "output": "URL"
        },
        {
            "input": "variantName",
            "output": "VARIANT_NAME"
        }
    ]   
}

data = {
    "jobId": job_id,
    "source": source,
    "destination": destination
}

configuration = {
    "bootstrapServers": ["localhost:9092"],
    "topic": "PRICER-JOBS"
}
s = KafkaSink(RecursiveNamespace.map_entry(configuration))
s.send_message([json.dumps(data)])
#s = ShopifySource(job_id, RecursiveNamespace.map_entry(source))
#s.extract_and_transform(lambda x: print(x))