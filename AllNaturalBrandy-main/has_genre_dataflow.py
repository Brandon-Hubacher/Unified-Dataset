import logging, datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

    
'''
5/11/2021
The following ParDo 'MakeHasGenre' function creates a junction table for the genres of a particular titleID.
It takes in the titleID and list of genres as a string and creates a record for each unique titleID
and genre combination. The FARM_FINGERPRINT method would not work for the creation of a unqiue id for genreID,
so that was performed after in bigquery with MD5.
'''
class MakeHasGenre(beam.DoFn):
    def process(self, element):
        titleID = element['titleID']
        genres = element['genres']
        if genres is not None:
            genres_list = genres.split(',')
            records_list = []
            for genre in genres_list:
                record = {'titleID': titleID, 'genreID': genre}
                records_list.append(record)
            return records_list

        
def run():
    PROJECT_ID = 'coastal-well-303101'
    BUCKET = 'gs://allnaturalbrandy2021/temp' # should we change bucket?
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
    
    
    options = PipelineOptions(
    flags=None,
    runner='DataflowRunner',
    project=PROJECT_ID,
    job_name='hasgenre',
    temp_location=BUCKET + '/temp',
    region='us-central1')
    
    p = beam.pipeline.Pipeline(options=options)
    
    sql = 'SELECT titleID, genres FROM datamart.movie_title'
    bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)
    
    query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)
    
    has_genre_pcoll = query_results | 'Make Genres Junction Table' >> beam.ParDo(MakeHasGenre())
    
    has_genre_pcoll | 'has_genre results' >> WriteToText('output.txt')
    
    dataset_id = 'datamart'
    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'has_genre_Dataflow'
    schema_id = 'titleID:STRING,genreID:STRING'
    
    has_genre_pcoll | 'Write has_genre to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
    
    
    result = p.run()
    result.wait_until_finish()
    
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()