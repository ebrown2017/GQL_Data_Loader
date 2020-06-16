import xlrd
import os
from xlrd import open_workbook, cellname
from saleor_gql_loader import ETLDataLoader
from saleor_gql_loader.utils import graphql_request, graphql_multipart_request, override_dict, handle_errors, get_payload
from decouple import Config, RepositoryEnv


DOTENV_FILE = '/home/eric/sdep-ecommerce/.env'
env_config = Config(RepositoryEnv(DOTENV_FILE))

# ! @ERIC Constants are defined with ALL_UPPERCASE_UNDERSCORE
# ! @ERIC Add this to your .env file (This is the location where you will put your excel files)
# EXCEL_FILE_LOCATION='/home/eric/'

EXCEL_FILE_LOCATION = env_config('EXCEL_FILE_LOCATION')

# ! @ERIC Add this to your .env file (This is the filename of your excel file since this is different than the name that was already on it)
# EXCEL_FILE_NAME='clevitemahlebearingsspreadsheet.xlsx'
EXCEL_FILE_NAME = env_config('EXCEL_FILE_NAME')

# ! @ERIC Add this to your .env file (This is your key)
# ETL_SECRET_ID='k1oNgxYBwDYT9w8O89oim3g8V99tMJ'

ETL_SECRET_ID = env_config('ETL_SECRET_ID')

class ETLDataGetter(ETLDataLoader):
	def get_product(self, product_id):
		"""get_product.
		Parameters
		----------
		product_id : str
			product id required to query the product.
		Returns
		-------
		product : dict
			the product object.
		"""

		variables = {
			"id": product_id
		}

		# * Definition: product(id: ID, slug: String): Product
		query = """
			fragment TaxedMoneyFields on TaxedMoney {
				currency
				gross {
					amount
					localized
				}
				net {
					amount
					localized
				}
				tax {
					amount
					localized
				}
			}
			fragment TaxedMoneyRangeFields on TaxedMoneyRange {
				start {
					...TaxedMoneyFields
				}
				stop {
					...TaxedMoneyFields
				}
			}
			fragment ProductPricingFields on ProductPricingInfo {
				onSale
				discount {
					...TaxedMoneyFields
				}
				discountLocalCurrency {
					...TaxedMoneyFields
				}
				priceRange {
					...TaxedMoneyRangeFields
				}
				priceRangeUndiscounted {
					...TaxedMoneyRangeFields
				}
				priceRangeLocalCurrency {
					...TaxedMoneyRangeFields
				}
			}
			fragment ProductVariantFields on ProductVariant {
				id
				sku
				name
				stockQuantity
				isAvailable
				pricing {
					discountLocalCurrency {
						...TaxedMoneyFields
					}
					price {
						currency
						gross {
							amount
							localized
						}
					}
					priceUndiscounted {
						currency
						gross {
							amount
							localized
						}
					}
					priceLocalCurrency {
						currency
						gross {
							amount
							localized
						}
					}
				}
				attributes {
					attribute {
						id
						name
					}
					values {
						id
						name
						value: name
					}
				}
			}
			query get_product($id: ID!) {
				product(id: $id) {
					id
					seoTitle
					seoDescription
					name
					description
					descriptionJson
					publicationDate
					isPublished
					productType {
						id
						name
					}
					slug
					category {
						id
						name
					}
					updatedAt
					chargeTaxes
					weight {
						unit
						value
					}
					thumbnail {
						url
						alt
					}
					pricing {
						...ProductPricingFields
					}
					isAvailable
					basePrice {
						currency
						amount
					}
					taxType {
						description
						taxCode
					}
					variants {
						...ProductVariantFields
					}
					images {
						id
						url
					}
				}
			}
		"""

		response = graphql_request(
			query, variables, self.headers, self.endpoint_url)

		return response["data"]["product"]

	def update_product(self, product_id, product):
		"""update_product.
		Parameters
		----------
		product_id : str
			product id required to query the product.
		product : Product
			product with fields to update to
		Returns
		-------
		product : dict
			updates the product object.
		"""

		updated_product = {
			"category": product["category"],
			"chargeTaxes": product["chargeTaxes"],
			# "description": product["description"],
			# "descriptionJson": product["descriptionJson"],
			"isPublished": product["isPublished"],
			"name": product["name"],
			"basePrice": product["basePrice"],
			"taxCode": "",
			"seo": {
				"title": product["seo"]["title"],
				"description": product["seo"]["description"]
			}
		}

		variables = {
			"id": product_id,
			"input": updated_product
		}

		# * Definition: product(id: ID, input: Product): Product
		query = """
			mutation productUpdate($id: ID!, $input: ProductInput!) {
				productUpdate(id: $id, input: $input) {
					product {
						id
						name
					}
					productErrors {
						field
						message
						code
					}
				}
			}
		"""

		response = graphql_request(
			query, variables, self.headers, self.endpoint_url)

		errors = response["data"]["productUpdate"]["productErrors"]
		handle_errors(errors)

		return response["data"]["productUpdate"]["product"]["name"] + " was updated."


	def product_excel_import_all(self):
		# declare location of excel file to be imported
		location = open_workbook(EXCEL_FILE_LOCATION + EXCEL_FILE_NAME, 'r')
		sheet = location.sheet_by_index(0)

		# ! @ERIC See task list
		# create a product type of car parts, save ID
		product_type_id = self.create_product_type(
			name = "Car Parts"
		)

		# create dictionary to hold all the objects imported form excel sheet
		products = []

		# iterate over each row in the sheet, pass the variables gotten from each col
		for row in range(1, sheet.nrows)[:50]:
			product_name = sheet.cell_value(row, 0)
			product_sku = sheet.cell_value(row, 1)

			# If product has no price, do not add it
			if sheet.cell_value(row, 2):
				product_price = float(sheet.cell_value(row, 2))
			else:
				continue

			product_description = sheet.cell_value(row, 4)

			# Check to see if product has weight, set if true
			if sheet.cell_value(row, 8):
				product_weight = {
					'unit': 'LB',
					'value': float(sheet.cell_value(row, 8))
				}
			else:
				product_weight = None

			# get and split categories into parent and child
			product_categories = sheet.cell_value(row, 11).split(', ')[-1].split('/')[1:]
			parent_category = product_categories[0]
			child_category = product_categories[1]

			# determine if the parent/child categories already exist, create them if not
			if self.get_category_by_name(parent_category) is not None:
				if self.get_category_by_name(child_category) is not None:
					product_category_id = self.get_category_by_name(child_category)
				else:
					product_category_id = self.category_create(child_category, parent_category_id)
			else:
				parent_category_id = self.create_category(name=parent_category)
				product_category_id = self.category_create(child_category, parent_category_id)


			product_image_url = sheet.cell_value(row, 12)
			product_seo_title = sheet.cell_value(row, 13)
			product_seo_description = sheet.cell_value(row, 14)

			#  declare and initalize a product object to pass to the products dict
			product_object = {
				"product_name" : product_name,
				"product_sku" : product_sku,
				"product_description" : product_description,
				"product_price" : product_price,
				"product_weight" : product_weight,
				"product_category" : product_category_id,
				"product_image_url" : product_image_url,
				"product_seo_title" : product_seo_title,
				"product_seo_description" : product_seo_description,
				"product_category_id" : product_category_id
			}

			# add product obj to products dict
			products.append(product_object)

		# * for product in products[:10]:
		for product in products[:50]:
			product_obj = {
				'name': product["product_name"],
				'sku': product["product_sku"],
				# 'descriptionJson': product["product_description"],
				'chargeTaxes': True,
				'isPublished': True,
				'trackInventory': False,
				'category': product["product_category_id"],
				'basePrice': product["product_price"],
				'weight': product["product_weight"],
				'seo': {
					"title" : product["product_seo_title"],
					"description" : product["product_seo_description"]
				}
				# ? add to createProductImage Later
				# ? imageURL = product["product_image_url"],
			}

			try:
				product_id = self.create_product(product_type_id, **product_obj)
			except:
				print("Product with SKU: " + product["product_sku"] + " already exists. Updating Product...")
				update_id = self.get_product_by_sku(product["product_sku"])
				self.update_product(update_id, product_obj)
				print(product["product_sku"] + " sucessfully updated")

	def get_product_by_sku(self, product_sku):
		"""get_product_by_sku.
		Parameters
		----------
		product_sku : str
			product sku to search for.
		Returns
		-------
		id : ID!
			ID of the product with the matching sku.
		"""

		variables = {
			"search": product_sku
		}

		query = """
			query products($search: String!) {
				products(first: 100, filter: {search: $search}) {
					edges {
						node {
							id
							variants {
								sku
							}
						}
					}
				}
			}
		"""

		response = graphql_request(
			query, variables, self.headers, self.endpoint_url)

		return self.get_matching_sku_helper(response["data"]["products"], product_sku)

	def get_matching_sku_helper(self, products, product_sku):
		for product_edge in products["edges"]:
			for product_variants in product_edge["node"]["variants"]:
				if product_variants["sku"] == product_sku:
					return product_edge["node"]["id"]

	def get_category_by_name(self, category_name):
		"""get_product_by_sku.
		Parameters
		----------
		category_name : str
			category name to search for.
		Returns
		-------
		id : ID!
			ID of the category with the matching name.
		"""

		variables = {
			"search": category_name
		}

		query = """
			query categories($search: String!) {
				categories(first: 100, filter: {search: $search}) {
					edges {
						node {
							id
							name
						}
					}
				}
			}
		"""

		response = graphql_request(
			query, variables, self.headers, self.endpoint_url)

		return self.get_category_by_name_helper(response["data"]["categories"], category_name)

	# ? Do I need this helper method? Because unlike products categories cant have variants with different ids
	def get_category_by_name_helper(self, categories, category_name):
		for category_edge in categories["edges"]:
			if category_edge["node"]["name"] == category_name:
				return category_edge["node"]["id"]


	def category_create(self, name, parent_id):
	    """create a category
	    Parameters
	    ----------
	    **kwargs : dict, optional
	    overrides the default value set to create the category refer to
	    the productTypeCreateInput graphQL type to know what can be
	    overriden.
	    Returns
	    -------
	    id : str
		the id of the productType created.
	    Raises
	    ------
	    Exception
		when productErrors is not an empty list.
	    """

	    category = {
			"name" : name
		}

	    variables = {
		    "input": category,
		    "parent": parent_id
	    }

	    query = """
		    mutation createCategory($input: CategoryInput!, $parent: ID!) {
			    categoryCreate(input: $input, parent: $parent) {
				    category {
					    id
				    }
				    productErrors {
					    field
					    message
					    code
				    }
			    }
		    }
	    """

	    response = graphql_request(
		    query, variables, self.headers, self.endpoint_url)

	    errors = response["data"]["categoryCreate"]["productErrors"]
	    handle_errors(errors)

	    return response["data"]["categoryCreate"]["category"]["id"]
