import os
import logging
from lib import client
import json
from lib import lib_bigquery
from conf import forecast as forecastConf
import asyncio

#path
path = os.path.dirname(os.path.realpath(__name__))

#creds
forecast = json.load(open(path + "/auth/forecast.json", "r"))

#Logging
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename=path + '/forecast.log',level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

#global cache to some during run pulled ids of object for pulling childs
globalCache = {}

#Configuration
days_look_back = 30 * 6

class Hanlder(client.ForecastApiClient):
    def __init__(self, url, token = forecast['token']):
        super().__init__(url, token)
        self.data = None

    def runAPI(self, CallType, report, parameters, parent_nodes = None):
        logger.info("Running api for {}".format(report))
        if parent_nodes == None:
            #print(report + parameters['paramenters'] if parameters['paramenters'] != None else report)
            if "response_process_function" in parameters.keys():
                    resp = getattr(self, CallType)(report + parameters['paramenters'] if parameters['paramenters'] != None else report)
                    resp = parameters['response_process_function'](resp)
                    self.data = resp
                    del resp
            else:
                self.data = getattr(self, CallType)(report + parameters['paramenters'] if parameters['paramenters'] != None else report)
        else:
            assert "{}" in report, "report name does not contain {} placeholder"
            self.data = []
            for parent in parent_nodes:
                reportPath = report.format(parent)
                resp = getattr(self, CallType)(reportPath + parameters['paramenters'] if parameters['paramenters'] != None else reportPath)
                #print(resp)
                
                #apply response_process_function if it exist
                if "response_process_function" in parameters.keys():
                    resp = parameters['response_process_function'](resp)

                self.data.extend([{**x, **{'parent_id': parent}} for x in resp if len(x) != 0])
                del resp, reportPath
        

    def saveData(self, filename, schema = None):
        assert self.data != None, "Data object is None, please run API first"

        #Remove old file and save data for upload withim temp
        def removeOldfile(filename):
            try:
                os.remove(filename)
            except:
                logger.info("Old file {} not found not removed".format(filename))

        removeOldfile(filename)

        #Json field parser
        def parseJsonArray(fieldValue):
            if type(fieldValue) in [dict, list]:
                return json.dumps(fieldValue)
            else:
                return fieldValue


        #Save to file
        with open(filename, 'a') as writeFile:
            if schema != None:
                for row in self.data:
                    writeFile.write(json.dumps({k:parseJsonArray(v) for k, v in row.items() if k in schema}) + "\n")
            else:
                for row in self.data:
                    writeFile.write(json.dumps({k:parseJsonArray(v) for k, v in row.items()}) + "\n")


    
async def runReport(report, parameters):

    #Run API
    HanlderInstance = Hanlder(parameters['api_url'])

    if "/{}/" in report or "parent_node" in parameters.keys():
        assert "/{}/" in report, "Report name does not contain /{}/ parent node placeholder"
        assert "parent_node" in parameters.keys(), "'parent_node' key was not found within forecast.py keys"
        filename = "/tmp/" + report.strip().replace("/{}/", "_") + ".json"

        while True:
            if parameters['parent_node'] in globalCache.keys():
                break
            else:
                await asyncio.sleep(5)
        
        #run api for sub-items
        HanlderInstance.runAPI(parameters['api_call'], report, parameters, globalCache[parameters['parent_node']])
        await asyncio.sleep(1)

    else:
        filename = "/tmp/" + report + ".json"
        #run api for regular parent level items
        HanlderInstance.runAPI(parameters['api_call'], report, parameters)
        await asyncio.sleep(1)

    #Database
    db = lib_bigquery.bigqueryWrapper()
    if "{}" in report:
        db.settings['table'] = report.strip().replace("/{}/", "_")
    else:
        db.settings['table'] = report

    #Save handler data and filter out only fields defined within schema
    HanlderInstance.saveData(filename,  [k for k in db.settings['schema'][db.settings['table']].keys()])
    
    #db.dropTable()
    db.AddTable()
    globalCache[report] = [x[parameters["id_field"]] for x in HanlderInstance.data]
    db.deleteLoad(parameters["id_field"], [str(x[parameters["id_field"]]) for x in HanlderInstance.data])
    
    #minId = min([x[parameters["id_field"]] for x in HanlderInstance.data])
    #db.runQuery( """
    #            DELETE FROM `"""+db.settings['dataset'] + """.""" + db.settings['table']+"""` 
    #            WHERE {} >= {}""".format(parameters["id_field"], str(minId)))

    #await asyncio.sleep(1)
    
    del HanlderInstance.data
    db.load_json_from_file(filename)


    if "post_processing_query" in parameters.keys():
        db.runQuery(parameters["post_processing_query"].format(db.settings['projectName'], db.settings['dataset']))

async def main():
    await asyncio.gather(*[runReport(report, parameters) for report, parameters in forecastConf.conf.items()])


def Accrease_forecast_api(*args):
    asyncio.run(main())


#if __name__ == "__main__":
    #Accrease_forecast_api()
    #HanlderInstance = Hanlder("https://api.forecast.it/api/v1/")

    #params = {"paramenters": None,
    #            "api_call": "get",
    #            "id_field": "id",
    #            "api_url": "https://api.forecast.it/api/v1/",
    #            "parent_node": "projects" 
    #        }

    #ids_to_fetch = [35600]

    #respo = HanlderInstance.runAPI("get", "projects/{}/financials", params, ids_to_fetch)
