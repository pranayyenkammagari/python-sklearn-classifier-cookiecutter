from collections import OrderedDict
from DependencyTestResult import DependencyTestResult
from TablePermissions import TablePermissions
from SqlDbConnection import SqlDbConnection

import ConfigMapper
import fnmatch
import XMLDataCrawler


def parse_xml(xml_path):
    '''Parse XML files in path for <tablePermissions> nodes'''

    requestedPermissions = []

    for content in XMLDataCrawler.getXMLData(xml_path, 'tablePermissions', 'table'):
        a = TablePermissions()
        a.Schema = str(content['@schema'])
        a.Table = str(content['@name'])
        if 'SELECT' in content['permission']:
            a.Read = True
        if 'INSERT' in content['permission']:
            a.Create = True
        if 'UPDATE' in content['permission']:
            a.Update = True
        if 'DELETE' in content['permission']:
            a.Delete = True
        requestedPermissions.append(a)

    return requestedPermissions


def get_db_permissions(sqlServerConfigs, db2Region, userId):
        
        query = """SELECT mem.GPMEM_MEMBER_ID 'USER_ID',mem.GPBD_NAME 'GROUP_NAME',
                        access.GRBD_NAME 'PERMISSIONS'
                        FROM [IT_SEC_RacfUnload].dbo.getGroupMemberShipvw mem
                        INNER JOIN [IT_SEC_RacfUnload].[dbo].[getTableAccessVw] access ON
                        mem.GPBD_NAME=access.GRACC_AUTH_ID
                        WHERE mem.GPMEM_MEMBER_ID ='{}' and (access.GRBD_NAME like '%{}%' or access.GRBD_NAME like '&%')""".format(userId, db2Region)
        
        
        connectionString = sqlServerConfigs['connection_string'].format_map(sqlServerConfigs)
        
        # TODO:  Remove / change to Debug
        #print('cs: {}'.format(connectionString))
        
        db_data = None
        with SqlDbConnection(connectionString) as connection:
            db_data = connection.select(query)
        
        givenPermissions = []
        db_permissions = []

        if db_data and len(db_data) > 0:
            for item in db_data:
                db_permissions.append(item[2])
        if db_permissions:
            for each_perm in db_permissions:
                db_split_data = each_perm.split('.')
                a = TablePermissions()
                a.Schema = db_split_data[1]
                a.Table = db_split_data[2]
                if db_split_data[3] == 'SELECT':
                    a.Read = True
                if db_split_data[3] == 'INSERT':
                    a.Create = True
                if db_split_data[3] == 'UPDATE':
                    a.Update = True
                if db_split_data[3] == 'DELETE':
                    a.Delete = True
                if db_split_data[3] == 'IUD':
                    a.Create = True
                    a.Delete = True
                    a.Update = True
                givenPermissions.append(a)

        return givenPermissions


def compare_results(req_perm_list, given_perm_list):
    failures = []
    failure_results = []

    for req in req_perm_list:
        aggGivenRead = False
        aggGivenCreate = False
        aggGivenDelete = False
        aggGivenUpdate = False

        for given in given_perm_list:
            if fnmatch.fnmatch(req.Schema, given.Schema):
                if fnmatch.fnmatch(req.Table, given.Table):
                    if given.Read is True:
                        aggGivenRead = True
                    if given.Update is True:
                        aggGivenUpdate = True
                    if given.Create is True:
                        aggGivenCreate = True
                    if given.Delete is True:
                        aggGivenDelete = True

        if req.Read is True:
            if aggGivenRead is False:
                failures.append(req)
        if req.Update is True:
            if aggGivenUpdate is False:
                failures.append(req)
        if req.Create is True:
            if aggGivenCreate is False:
                failures.append(req)
        if req.Delete is True:
            if aggGivenDelete is False:
                failures.append(req)

    return failures


def getOutputText(failedPermissions: list, db2Region: str):
    lines = []
    
    for failed in failedPermissions:
        
        lines.append('Requested permissions ({}) not available for {}.{} in DB2 Region {}'.format(
            ', '.join([x.upper() for x in failed.flaggedPermissions()]), 
            failed.Schema, 
            failed.Table, 
            db2Region))
    
    txt = '\n'.join(lines)

    return txt


def process(allConfigs, path, db2Region, userId):
    '''Process all requested table permissions and compare with what's been granted for the application in a particular DB2 region'''

    print('-- Table Permissions...')

    result = DependencyTestResult()
    result.Name = 'Table Permissions'

    sqlServerConfigs = ConfigMapper.getConfigDictionary(allConfigs, 'SQLServer_RACFUnload')

    requestedPerm = parse_xml(path)
    result.Count = len(requestedPerm)

    givenPerm = get_db_permissions(sqlServerConfigs, db2Region, userId)
    result.FailureData = compare_results(requestedPerm, givenPerm)

    if len(result.FailureData) == 0:
        result.Passed = True

    result.OutputText = getOutputText(result.FailureData, db2Region)

    return result