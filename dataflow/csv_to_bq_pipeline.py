
import apache_beam as beam
import logging
import datetime, os

PROJECT='projectx-dbm' 
BUCKET='projectx-beam-pipeline'
REGION='northamerica-northeast1'
JOB_NAME='beam_pipeline'
OUTPUT_DIR='./output_dir'
RUNNER='DirectRunner'
COLNAMES='name,gender,total'.split(',') 

options = {
    'staging_location': os.path.join(OUTPUT_DIR, 'tmp', 'staging'),
    'temp_location': os.path.join(OUTPUT_DIR, 'tmp'),
    'job_name': JOB_NAME,
    'project': PROJECT,
    'region': REGION,
    'teardown_policy': 'TEARDOWN_ALWAYS',
    'no_save_main_session': True
}

def parse_csv(line):
    values = line.split(',')
    rowdict = {}

    for colname, value in zip(COLNAMES, values):
        rowdict[colname] = value
    yield rowdict


def pull_fields(rowdict):
    results = {}

    #string fields
    for col in 'name,gender'.split(','):
        if col in rowdict:
            results[col] = rowdict[col]
        else:
            logging.info('Ignoring line missing {}'.format(col))

    # integer fields
    for col in 'total'.split(','):
        try:
            results[col] = (int) (rowdict[col])
        except:
            results[col] = None

    yield results 

opts = beam.pipeline.PipelineOptions(flags = [], **options)

with beam.Pipeline(RUNNER, options = opts) as p:
    (p
      | 'read' >> beam.io.ReadFromText('./names.txt')
      | 'parse_csv' >> beam.FlatMap(parse_csv)
      | 'pull_fields' >> beam.FlatMap(pull_fields)
      | 'write_bq' >> beam.io.gcp.bigquery.WriteToBigQuery(table='names', dataset='babynames',schema='name:STRING,gender:STRING,total:INT64')
    )
