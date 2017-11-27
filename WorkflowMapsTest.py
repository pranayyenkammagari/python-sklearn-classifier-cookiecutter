from collections import OrderedDict
from DependencyTestResult import DependencyTestResult
from SqlDbConnection import SqlDbConnection

import ConfigMapper
from datetime import datetime as dt
import XMLDataCrawler


def parse_xml(xml_path):
    '''Parse XML files in path for <workflowMaps> nodes'''

    requestedMaps = {}

    for content in XMLDataCrawler.getXMLData(xml_path, 'workflowMaps', 'workflowMap'):
        name = str(content['@name'])
        modifiedDate = content.get('@modifiedSince', None)
        if modifiedDate:
            modifiedDate = dt.strptime(modifiedDate, '%Y-%m-%d')

        requestedMaps[name] = modifiedDate

    return requestedMaps


def get_loaded_maps(sqlServerConfigs):
    
    connectionString = sqlServerConfigs['connection_string'].format_map(sqlServerConfigs)

    query = """SELECT [FYI_PROCNAME]
                     ,[FYI_DATE_CREATED]
                     FROM [{database}].[FYIADM].[FYI_WFPROCESS] main_q WITH (NOLOCK)
                     ORDER BY [FYI_DATE_CREATED] DESC
                     """.format_map(sqlServerConfigs)
    
    # TODO:  Remove / change to Debug
    #print('cs: {}'.format(connectionString))
    
    db_data = None
    with SqlDbConnection(connectionString) as connection:
        db_data = connection.select(query)
        
    loadedWorkflowMaps = {}
    
    if db_data and len(db_data) > 0:
        for name, modified in db_data:
            #print('{}: {}'.format(name, modified))
            loadedWorkflowMaps[name] = dt.strptime(modified, '%Y-%m-%d')
    
    return loadedWorkflowMaps


def compare_results(requested, loaded):
    failed = {}
    
    for name, modifiedSince in requested.items():
        if name not in loaded.keys():
            failed[name] = modifiedSince
            continue
        
        # If specific version/date is requested
        if modifiedSince:
            modified = loaded.get(name)
            if modifiedSince > modified:
                failed[name] = modifiedSince

    return failed


def getOutputText(failedWorkflowMaps: dict, region: str):
    lines = []
    
    for name, modifiedDate in failedWorkflowMaps.items():
                
        if modifiedDate is None:
            message = 'Requested workflow map \'{}\' not loaded in emVision region {}'
            lines.append(message.format(
                name,
                region))
        else:
            message = 'Requested workflow map \'{0}\' not loaded or modified since {1} in emVision region {2}'
            lines.append(message.format(
                name,
                modifiedDate.strftime('%Y-%m-%d'),
                region))
    
    txt = '\n'.join(lines)

    return txt


def process(allConfigs, path):
    '''Process all requested table permissions and compare with what's been granted for the application in a particular DB2 region'''

    testType = 'Workflow Maps'

    print('-- {}...'.format(testType))

    result = DependencyTestResult()
    result.Name = testType

    sqlServerConfigs = ConfigMapper.getConfigDictionary(allConfigs, 'SQLServer_emVision')

    requested = parse_xml(path)
    result.Count = len(requested)

    loaded = get_loaded_maps(sqlServerConfigs)
    result.FailureData = compare_results(requested, loaded)

    if len(result.FailureData) == 0:
        result.Passed = True

    result.OutputText = getOutputText(result.FailureData, sqlServerConfigs.get('database', ''))

    return result