from collections import OrderedDict
from DependencyTestResult import DependencyTestResult
from SqlDbConnection import SqlDbConnection
from TableStructure import TableStructure

import ConfigMapper
import ListUtilities
import xmltodict
import XMLDataCrawler



def parse_xml(xml_path):
    '''Parse XML files in path for <tableStructure> nodes'''

    requestedStructure = []

    for content in XMLDataCrawler.getXMLData(xml_path, 'tableStructure', 'table'):
        a = TableStructure()
        a.Schema = str(content['@schema'])
        a.Table = str(content['@name'])
        if 'column' in content.keys():
            for column in ListUtilities.ensureIsList(content['column']):
                a.Columns.append(column)

        requestedStructure.append(a)

    return requestedStructure


def get_db_structure(db2Configs):
    
    connection_string = db2Configs['connection_string'].format_map(db2Configs)
    
    # TODO:  Convert to debug
    #print('cs: {}'.format(connection_string))

    # Get all columns & convert to dictionary for in-memory lookup
    c_results = None
    with SqlDbConnection(connection_string) as sql:
        c_results = sql.select('SELECT TBCreator, TBName, Name from sysibm.syscolumns where TBCreator IN (\'DBA\', \'OPERS\') WITH UR;')

    columns = {}
    for schema, table, column in c_results:
        schema = schema.strip()
        table = table.strip()
        column = column.strip()

        st = '{}.{}'.format(schema, table)
        
        if st not in columns.keys():
            columns[st] = []

        columns[st].append(column)

    # Get list of all relevant tables and assign found columns from above
    givenStructure = []

    t_results = None
    with SqlDbConnection(connection_string) as sql:
        t_results = sql.select('SELECT Creator, Name from sysibm.systables WHERE Creator IN (\'DBA\', \'OPERS\');')

    for schema, table in t_results:
        schema = schema.strip()
        table = table.strip()
        st = '{}.{}'.format(schema, table)

        a = TableStructure()
        a.Table = table
        a.Schema = schema
        a.Columns = columns.get(st, [])
        givenStructure.append(a)
        

    return givenStructure


def compare_results(req_structure, given_structure):
    '''Compare requested table/column structure with what's available'''
    
    # Re-Shape objects for easier comparison
    reqStructDict = {}
    for struct in req_structure:
        reqStructDict['{}.{}'.format(struct.Schema, struct.Table)] = struct

    givenStructDict = {}
    for struct in given_structure:
        givenStructDict['{}.{}'.format(struct.Schema, struct.Table)] = struct


    failedStructure = []

    for table in reqStructDict.keys():
        if table in givenStructDict.keys():

            missingColumns = False
            for column in ListUtilities.ensureIsList(reqStructDict[table].Columns):
                if column not in givenStructDict[table].Columns:
                    missingColumns = True

            # Fail if any columns are missing
            if missingColumns is True:
                failedStructure.append(reqStructDict[table])
        
        # Fail if table is missing
        else:
            failedStructure.append(reqStructDict[table])


    return failedStructure


def getOutputText(failedStructure: list, db2Region: str):
    lines = []

    for failed in failedStructure:
        
        lines.append('Requested structure {}.{} ({}):  Not everything in place in DB2 Region {}'.format( 
            failed.Schema,
            failed.Table,
            ','.join(failed.Columns),
            db2Region))

    txt = '\n'.join(lines)

    return txt


def process(allConfigs, path, db2Region):
    '''Process all requested table structure and compare with 
    what's in place in a particular DB2 region'''

    print('-- Table Structure...')

    result = DependencyTestResult()
    result.Name = 'Table Structure'

    db2Configs = ConfigMapper.getConfigDictionary(allConfigs, 'DB2', db2Region)

    requestedStructure = parse_xml(path)
    result.Count = len(requestedStructure)

    givenStructure = get_db_structure(db2Configs)
    result.FailureData = compare_results(requestedStructure, givenStructure)

    if len(result.FailureData) == 0:
        result.Passed = True

    result.OutputText = getOutputText(result.FailureData, db2Region)

    return result