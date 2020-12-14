PROJECT='projectx-dbm' 
BUCKET='projectx-beam-pipeline'
REGION='northamerica-northeast1'

from google.cloud import bigquery

bq = bigquery.Client(project=PROJECT)
query = """
SELECT duration
FrOM `bigquery-public-data.london_bicycles.cycle_hire`
WHERE start_station_id = 708
"""

df = bq.query(query, location='EU').to_dataframe()
print(df.describe())