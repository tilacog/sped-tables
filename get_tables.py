
'''
This script should do the following:
    1. Fetch RFB servers for `external_tables` list, for each SPED-type.
    2. Parse XML in the response to find each table URL.
    3. Download each table as a CSV file.
    4. Insert them into db.
'''
from concurrent import futures
import io
import requests
import xmltodict


# ---[ Definitions ]---

base_listing_url = ("http://www.sped.fazenda.gov.br"
                    "/spedtabelas/WsConsulta/WsConsulta.asmx")

post_request_xml = 'base-post.xml'

sped_names = [
    'SpedFiscal',
    'SpedPisCofins',
    'SpedContabil',
    'SpedEcf',
    # more?
]


def fetch_table_listing(sped_name):
    '''sends a request and returns it's response body'''
    with open(post_request_xml) as f:
        xml_data = f.read().format(sped_name=sped_name)

    headers = {'Content-Type': 'text/xml'}
    response = requests.post(base_listing_url, data=xml_data, headers=headers)

    return response.text


def parse_table_listing(listing_xml):
    '''parses xml data and yields pieces of data'''
    doc = xmltodict.parse(listing_xml.read())

    root = doc['soap:Envelope']['soap:Body']\
              ['consultarVersoesTabelasExternasResponse']\
              ['consultarVersoesTabelasExternasResult']
    base_file_url = root['urlDownloadArquivo']

    # This node contains an embedded xml document
    table_reference = xmltodict.parse(root['metadadosXml'])

    packages = table_reference['sistema']['tabelas']['pacotes']['pacote']

    for pi, pkg in enumerate(packages):
        # Some packages have no tables
        if not pkg['tabelas']:
            continue

        # Some items are returned as objects instead of lists.
        # This step puts those cases under a list.
        table_items = pkg['tabelas']['tabela']
        if not isinstance(table_items, list):
            table_items = [table_items]

        for table in table_items:

            # prepare info for requests.get
            payload = {'idTabela': table['@id'], 'versao': table['@versao']}
            local_filename = '{}-{}-{}-{}.csv'.format(
                table['@tipo'],
                pkg['@cod'],
                table['@id'],
                table['@versao'],
            )

            yield (base_file_url, payload, local_filename)

# --------


def save_file(file_name, content):
    with open(file_name, 'w', encoding='utf-8') as f:
        f.write(content.decode('latin1'))


def get_file(base_url, payload):
    resp = requests.get(base_url, params=payload)
    return resp.content


def download_one(datapoint):
    # cant use starmap, so pass a tuple and unpack here
    base_url, payload, local_filename = datapoint

    csv_file = get_file(base_url, payload)
    save_file(local_filename, csv_file)


def download_many(file_list):
    workers = 10
    with futures.ThreadPoolExecutor(workers) as executor:
        res = executor.map(download_one, file_list)

    return len(list(res))


if __name__ == '__main__':
    from tqdm import tqdm

    for sped_name in tqdm(sped_names):

        table_listing = io.StringIO(fetch_table_listing(sped_name))
        data = list(parse_table_listing(table_listing))

        # append sped_name on all local file names
        data = [(i, j, sped_name + '-' + k) for (i, j, k) in data]

        download_many(data)
