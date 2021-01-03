import argparse

import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

def run(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', default='./names.txt', help='Input file')
    parser.add_argument('--output', dest='output', default='./names_transfored.txt', help='Output file')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend(['--runner=DirectRunner'])

    print(known_args)
    print(pipeline_args)


if __name__ == '__main__':
    run()

