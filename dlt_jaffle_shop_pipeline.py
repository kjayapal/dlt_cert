import dlt
import os

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

base_url = 'https://jaffle-shop.scalevector.ai/api/v1'

def get_rest_client():
  return RESTClient(
    base_url=base_url,
    headers={"User-Agent": "MyApp/1.0"},
    paginator=HeaderLinkPaginator(),
  )

@dlt.resource(table_name="customers", primary_key="id", parallelized=True)
def get_customers_o():
  client = get_rest_client()
  for page in client.paginate("/customers"):
    yield page


@dlt.resource(table_name="orders", primary_key="id", parallelized=True)
def get_orders_o():
  # making the page_size = 5000
  client = get_rest_client()
  for page in client.paginate("/orders", params={'page_size' : '5000'}):
    yield page
    break


@dlt.resource(table_name="products", primary_key="sku", parallelized=True)
def get_products_o():
  client = get_rest_client()
  for page in client.paginate("/products"):
    yield page


@dlt.source
def jaffle_shop_data():
  return get_customers_o, get_orders_o, get_products_o


os.environ['EXTRACT__WORKERS'] = '3'
# os.environ['NORMALIZE__WORKERS'] = '10'
# os.environ['LOAD__WORKERS'] = '10'

os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '10000'
os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = '10000'
os.environ["NORMALIZE_EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = '10000'


pipeline_o = dlt.pipeline(
    pipeline_name="dlt_lesson_9_hw_optimzed",
    destination="duckdb",
    dataset_name="mydata_o",
    progress="log"
)

load_info = pipeline_o.run(jaffle_shop_data(), write_disposition="replace")
print(pipeline_o.last_trace)