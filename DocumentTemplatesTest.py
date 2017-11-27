from collections import OrderedDict
from DependencyTestResult import DependencyTestResult
from SqlDbConnection import SqlDbConnection
from packaging import version as pkgVersion

import ConfigMapper
import ListUtilities
import xmltodict
import XMLDataCrawler
import SmartCommAPI
import ThunderheadNowAPI
import FormattedText


def parse_xml(xml_path):
    '''Parse XML files in path for <documentTemplate> nodes'''

    requestedTemplates = {}

    for content in XMLDataCrawler.getXMLData(xml_path, 'documentTemplates', 'document'):
        
        docCode = str(content['@docCode'])
        version = content.get('@version', None)
        
        requestedTemplates[docCode] = {}

        if version is not None:
            requestedTemplates[docCode]['version'] = version

    return requestedTemplates


def get_thunderheadNow_templates(thunderheadConfigs:dict, versionedDocCodes:list):
    
    documents = ThunderheadNowAPI.searchForDocuments(thunderheadConfigs)
    
    loadedTemplates = {}

    for doc in documents:
        docCode = doc.name
        id = doc.id

        loadedTemplates[docCode] = {}
        loadedTemplates[docCode]['id'] = id
        
        if docCode in versionedDocCodes:
            maxVersion = pkgVersion.parse('0.0.0')

            versionResults = ThunderheadNowAPI.getVersions(thunderheadConfigs, id)
            for version in versionResults:
                thisVersion = pkgVersion.parse('{}.{}.{}'.format(
                    version['versionMajor'],
                    version['versionMinor'],
                    version['versionRevision']))
                if thisVersion > maxVersion:
                    maxVersion = thisVersion
            
            loadedTemplates[docCode]['version'] = maxVersion.base_version

    return loadedTemplates


def get_smartCommunications_templates(smartCommConfigs:dict, versionedDocCodes:list):
    
    searchResults = SmartCommAPI.searchByType(smartCommConfigs, 'application/x-thunderhead-ddv')
    
    loadedTemplates = {}

    for doc in searchResults:
        docCode = doc['itemName']
        id = doc['itemId']

        loadedTemplates[docCode] = {}
        loadedTemplates[docCode]['id'] = id
                
        if docCode in versionedDocCodes:
            maxVersion = pkgVersion.parse('0.0.0')

            versionResults = SmartCommAPI.getVersions(smartCommConfigs, id)
            for version in versionResults:
                thisVersion = pkgVersion.parse('{}.{}.{}'.format(
                    version['versionMajor'],
                    version['versionMinor'],
                    version['versionRevision']))
                if thisVersion > maxVersion:
                    maxVersion = thisVersion

            loadedTemplates[docCode]['version'] = maxVersion.base_version

    return loadedTemplates


def compare_results(req_templates, loaded_templates):
    '''Compare requested document templates with what's been loaded'''

    failedTemplates = {}

    for reqDocCode in req_templates.keys():
        try:
            if reqDocCode in loaded_templates.keys():
                minVersion = pkgVersion.parse(req_templates[reqDocCode].get('version', '0.0.0'))
                loadedVersion = pkgVersion.parse(loaded_templates[reqDocCode].get('version', '0.0.0'))

                # Fail if version minimum not met
                if loadedVersion < minVersion:
                    failedTemplates[reqDocCode] = req_templates[reqDocCode]
        
            # Fail if docCode is missing
            else:
                failedTemplates[reqDocCode] = req_templates[reqDocCode]
        except Exception as ex:
            print('Error on DocCode: {}'.format(reqDocCode))
            raise ex

    return failedTemplates


def getOutputText(failedTemplates: dict):
    lines = []

    for docCode in failedTemplates.keys():
        version = failedTemplates[docCode].get('version', '(Any)')
        
        lines.append('Requested template {} v{}:  Not loaded in ThunderheadNow or SmartCommunications'.format( 
            docCode,
            version))

    txt = '\n'.join(lines)

    return txt


def process(allConfigs, path):
    '''Process all requested letter/document templates and compare with 
    what's been loaded in both ThunderheadNow & SmartCommunications'''

    print('-- Document Templates...')

    result = DependencyTestResult()
    result.Name = 'Document Templates'

    thConfigs = ConfigMapper.getConfigDictionary(allConfigs, 'API_ThunderheadNow')
    scConfigs = ConfigMapper.getConfigDictionary(allConfigs, 'API_SmartCommunications')

    requestedTemplates = parse_xml(path)
    result.Count = len(requestedTemplates)
    
    versionedDocCodes = []
    for docCode, version in requestedTemplates.items():
        if version is not None:
            versionedDocCodes.append(docCode)

    loadedTemplates = get_thunderheadNow_templates(thConfigs, versionedDocCodes)

    loadedTemplates.update(get_smartCommunications_templates(scConfigs, versionedDocCodes))

    result.FailureData = compare_results(requestedTemplates, loadedTemplates)

    if len(result.FailureData) == 0:
        result.Passed = True

    result.OutputText = getOutputText(result.FailureData)

    return result