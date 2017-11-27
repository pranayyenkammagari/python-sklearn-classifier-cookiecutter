import sys
import os

from collections import OrderedDict
from DependencyTestResult import DependencyTestResult
from SqlDbConnection import SqlDbConnection

import ConfigMapper
import ListUtilities
import xmltodict
import XMLDataCrawler


def parse_xml(xml_path):
    '''Parse XML files in path for <documentTemplate> nodes'''

    requestedDocs = []
    for content in XMLDataCrawler.getXMLData(xml_path, 'documentTemplates', 'document'):
        for key in content.keys():
            if key == '@dataQueryRequired' and content['@dataQueryRequired'] == 'true':
                requestedDocs.append(content['@docCode'])
    return requestedDocs


def get_loaded_data_queries(db2Configs):
    connection_string = db2Configs['connection_string'].format_map(db2Configs)

    # TODO:  Remove / change to Debug
    #print('cs: {}'.format(connection_string))

    results = None

    with SqlDbConnection(connection_string) as sql:
        results = sql.select('SELECT DOC_ID from dba.CORT_LETTER_QUERY_seq')

    givenDocs = []

    for doc_code in results:
        doc_id=doc_code[0]
        givenDocs.append(doc_id)

    return givenDocs


def compare_docs(allRequested, allLoaded):
    '''Compare requested documents with what's loaded'''

    failedDocs = []

    for reqDocCode in allRequested:
        if reqDocCode not in allLoaded:
            failedDocs.append(reqDocCode)
                
    return failedDocs


def getOutputText(failedStructure: list):
    lines = []

    for failed in failedStructure:
        lines.append("Requested Letter Data Query doesn't exist for DocCode {}".format(failed))

    txt = '\n'.join(lines)

    return txt


def process(allConfigs, path, db2Region):

    testType = 'Letter Data Queries'

    print('-- {}...'.format(testType))
    
    result = DependencyTestResult()
    result.Name = testType

    db2Configs = ConfigMapper.getConfigDictionary(allConfigs, 'DB2', db2Region)

    requestedDocs = parse_xml(path)
    result.Count = len(requestedDocs)

    loadedDocs = get_loaded_data_queries(db2Configs)
    result.FailureData = compare_docs(requestedDocs, loadedDocs)

    if len(result.FailureData) == 0:
        result.Passed = True

    result.OutputText = getOutputText(result.FailureData)

    return result

