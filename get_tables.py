
'''
This script should do the following:
    1. Fetch RFB servers for `external_tables` list, for each SPED-type.
    2. Parse XML in the response to find each table URL.
    3. Download each table as a CSV file.
    4. Insert them into db.
'''
import xmltodict


with open('external-tables-spedpiscofins.xml', 'rb') as f:
    doc = xmltodict.parse(f)

root = doc['RetornoVersoesTabelasExternas']
base_url = root['urlDownloadArquivo']
table_reference = xmltodict.parse(root['metadadosXml'])

packages = table_reference['sistema']['tabelas']['pacotes']['pacote']

for pkg in packages:
    pkg_name = pkg['@cod']
    pkg_desc = pkg['@desc']

    print('{:<40} {}'.format(pkg_name, pkg_desc))
    for table in pkg['tabelas']['tabela']:
        attrs = {k.strip('@'):v for (k,v) in table.items() if k.startswith('@')}

        print('\t', attrs) 

