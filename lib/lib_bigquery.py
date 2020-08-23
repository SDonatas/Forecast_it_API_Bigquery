from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import json
import logging
import pathlib
import os
import csv

#Logging
logger = logging.getLogger(__name__)

#environmental path
path = str(pathlib.Path(__file__).parent)

#Bigquery, settings
settingsJson = json.load(open(path + "/../conf/bigquery_settings.json", 'r'))

#Bigquery Database        
class bigqueryWrapper:
    def __init__(self, settingsJson = settingsJson):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path + "/.." + settingsJson['GOOGLE_APPLICATION_CREDENTIALS']
        self.settings = settingsJson
        self.client = bigquery.Client(project=self.settings['projectName'])
        
    def dropTable(self):
      identifier = self.settings['projectName'] + "." + self.settings['dataset'] + "." + self.settings['table']
      try:
        self.client.delete_table(identifier, not_found_ok=False)
        logger.info("Deleted table '{}'.".format(identifier))
      except Exception as e:
        logger.info("Failed to deleted table '{}'.".format(e))


    def deleteLoad(self, field, listOfIds):
        if len(listOfIds) > 0:
            logger.info("Removing old ids... ")
            queryRD = """
            DELETE FROM `"""+self.settings['dataset'] + """.""" + self.settings['table']+"""` 
            WHERE {} IN ({})
            """.format(field, ", ".join(listOfIds))
            query_job = self.client.query(queryRD)
            results = query_job.result()  # Waits for job to complete.  
            logger.info(", ".join(listOfIds))
            logger.info("Removing old ids... Done")
        else:
            logger.info("Passed empty list (no ids to remove)")


    def runQuery(self, queryString:str):
        try:
            query_job = self.client.query(queryString)
            results = query_job.result()  # Waits for job to complete.  
            logger.info("Query run... Success")
        except BadRequest as e:
            for e in results.errors:
                logger.info('ERROR: {}'.format(e['message']))



    def AddTable(self):
        if 'table' in self.settings.keys():
            try:
                schema_bq = []
                for field, datatype in self.settings['schema'][self.settings['table']].items():
                    schema_bq.append(bigquery.SchemaField(field, datatype[0], mode=datatype[1]))
                
                table_ref = self.client.dataset(self.settings['dataset']).table(self.settings['table'])
                table = bigquery.Table(table_ref, schema=schema_bq)
                logger.info("Table {} has been added".format(self.settings['table']))
            except Exception as e:
                logger.info("Table already exists: {}".format(e))
        else:
            raise ValueError("Please add table to settings property dictionary")
        

        try:
            table = self.client.create_table(table)  # API request
            assert table.table_id == self.settings['table']
            logger.info("Table has been created {}".format(self.settings['table']))
        except Exception as e:
            logger.info("Failed to create table. {}".format(e))


    def load_csv_from_file(self, fileName):
        
        dataset_id = self.settings['dataset']
        table_id = self.settings['table']
        
        print("Loading file to bigquery...")
        logger.info("Loading file to bigquery...")
        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        with open(fileName, 'rb') as source_file:
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = 'text/csv'
            job = bigquery_client.load_table_from_file(
                source_file, table_ref, job_config=job_config)

        try:
            job.result()  # Waits for job to complete

            logger.info('Loaded {} rows into {}:{}.'.format(
                job.output_rows, dataset_id, table_id))
            
            logger.info("Loading file to bigquery...Done")
            
        except BadRequest as e:
            for e in job.errors:
                logger.info('ERROR: {}'.format(e['message']))


    def load_json_from_file(self, fileName):
        
        dataset_id = self.settings['dataset']
        table_id = self.settings['table']
        
        print("Loading file to bigquery...")
        logger.info("Loading file to bigquery...")
        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        with open(fileName, 'rb') as source_file:
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = 'NEWLINE_DELIMITED_JSON'
            job = bigquery_client.load_table_from_file(
                source_file, table_ref, job_config=job_config)

        try:
            job.result()  # Waits for job to complete

            logger.info('Loaded {} rows into {}:{}.'.format(
                job.output_rows, dataset_id, table_id))
            
            logger.info("Loading file to bigquery...Done")
        except BadRequest as e:
            for e in job.errors:
                logger.info('ERROR: {}'.format(e['message']))



    def loadRows(self, rows_to_insert):

      rowsLen = len(rows_to_insert)

      if rowsLen > 500:

        for row_set in range(0, rowsLen, 500):
            table = self.client.get_table(self.settings['dataset'] + "." + self.settings['table'])
            errors = self.client.insert_rows(table, rows_to_insert[row_set: row_set+500])
            
            if errors == []:
                print("New 500 rows batch have been added.")
            else:
                print(errors)
      else:

        table = self.client.get_table(self.settings['dataset'] + "." + self.settings['table'])
        errors = self.client.insert_rows(table, rows_to_insert)
          
        if errors == []:
            logger.info("All {} rows have been added.".format(rowsLen))
        else:
            logger.info(errors)

                
        logger.info("Operation completed")