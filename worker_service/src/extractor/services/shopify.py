"""Implements the shopify soure"""

import pandas as pd
from common.main import Main
from common.rest import RestClient
from extractor.services.base import AbstractSource


class ShopifySource(AbstractSource):
    """Implements the shopify soure"""

    def __init__(
        self, job_id, source):
        self._job_id = job_id
        self._input_schema_2_output_schema = {}
        for schema_mapping in source.schemaMapping:
            self._input_schema_2_output_schema[schema_mapping.input] = schema_mapping.output
        self._page_number = 1
        self._urls = source.shopify.urls
        self._rest_client = RestClient()
      
    def _extract(self, url, page_number):
        """Extracts the data from the Shopify"""
        Main.logger().debug(
            "extracting the data from the shopify using %s with page number %d", url, page_number)
        response_data = self._extract_products(url, page_number)
        Main.logger().debug(
            "extracted the data from the shopify using %s with page number %d", url, page_number)
        self._page_number += 1
        return self._to_dataframe(response_data, url), page_number
    
    def _extract_products(self, url, page_number):
        """Extracts the product data from the Shopify"""
        extraction_url = f"{url}/products.json"
        return self._rest_client.send_get_request(f"{extraction_url}?page={page_number}")
    
    def _product_variant_image(self, product, variant_id):
        """Returns the product image"""
        image_url = None
        images = product.images
        for image in images:
            if variant_id in image.variant_ids:
                image_url = image.src
                break
        if image_url:
            return image_url
        return images[0].src if images else ""
    
    def _to_dataframe(self, response_data, url):
        """converts data into dataframe"""
        result = []
        Main.logger().debug(
            "parsing the data from the shopify using %s of the %s job", url, self._job_id)
        products = response_data.products
        for product in products:
            title = product.title
            product_type = product.product_type
            product_url = f"{url}/products/{product.handle}"
            for variant in product.variants:
                options = []
                if variant.option1:
                    options.append(variant.option1)
                if variant.option2:
                    options.append(variant.option2)
                if variant.option3:
                    options.append(variant.option3)
                usage_record = {}
                usage_record["available"] = variant.available
                usage_record["category"] = product_type
                usage_record["code"] = variant.sku
                usage_record["collection"] = ""
                usage_record["grams"] = variant.grams
                usage_record["imageUrl"] = self._product_variant_image(product, variant.id)
                usage_record["name"] = title
                usage_record["price"] = variant.price
                usage_record["requiresShipping"] = variant.requires_shipping
                usage_record["url"] = product_url
                usage_record["variantName"] = " / ".join(options)
                result.append(usage_record)
        Main.logger().debug(
            "parsed the data from the shopify using %s of the %s job", url, self._job_id)
        return pd.DataFrame(result)

    def _transform(self, dataframe, url, page_number):
        """Transforms the data from the Azure"""
        Main.logger().debug(
            "transforming the data from the shopify using %s with page number %d", url, page_number)
        Main.logger().debug(
            "transformed the data from the shopify using %s with page number %d", url, page_number)
    
    def _extract_and_transform_by_url(self, url, result_handler):        
        """Implements the extraction and performs transformation"""
        dataframe, page_number = self._extract(url, self._page_number)
        if dataframe.empty:
           return
        self._transform(dataframe, url, page_number)
        descriptors = []
        for key in list(dataframe.keys()):
            descriptors.append(self._input_schema_2_output_schema[key])
        result_handler({
            "descriptors": descriptors,
            "values": dataframe.values.tolist()
        })
        self._extract_and_transform_by_url(url, result_handler)

    def extract_and_transform(self, result_handler):        
        """Implements the extraction and performs transformation"""
        for url in self._urls:
            self._page_number = 1
            self._extract_and_transform_by_url(url, result_handler)