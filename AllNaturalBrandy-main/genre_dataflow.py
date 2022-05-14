import logging, datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery


'''
5/6/2021
The following ParDo 'MakeGenre' function takes in a dictionary containing a genre from the previously made has_genre_Beam table.
It simply creates a record of genreID and genre and returns it.
The FARM_FINGERPRINT method would not work for the creation of a unqiue id for genreID,
so that was performed after in bigquery with MD5.
'''
class MakeGenre(beam.DoFn):
    def process(self, element):
        g = element['genreID']
        if g is not None:
            record = {'genreID': g, 'genre': g}
            return [record]


def run():
    PROJECT_ID = 'coastal-well-303101'
    BUCKET = 'gs://allnaturalbrandy2021/temp'
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
    
    
    options = PipelineOptions(
    flags=None,
    runner='DataflowRunner',
    project=PROJECT_ID,
    job_name='genre',
    temp_location=BUCKET + '/temp',
    region='us-central1')
    
    p = beam.pipeline.Pipeline(options=options)
    
    sql = 'SELECT DISTINCT genreID FROM datamart.movie_title'
    bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)
    
    query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
    
    genre_pcoll = query_results | 'Make Genre Table' >> beam.ParDo(MakeGenre())
    
    genre_pcoll | 'genre results' >> WriteToText('output.txt')

    dataset_id = 'datamart'
    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'genre_Dataflow'
    schema_id = 'genreID:STRING,genre:STRING'
    
    genre_pcoll | 'Write genre to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
    
    
    result = p.run()
    result.wait_until_finish()
    
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()