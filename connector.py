"""
GLOBAL VARS
"""
USERNAME = "user-public-notificacoes"
PASSWRD = "Za4qNXdyQNSa9YaA"
AUTH = (USERNAME, PASSWRD)

## SIZE = 0 não retornar os hits dos documentos, mas retorna o aggregation completo.
QUERY_SIZE = 0

ESTADOS = [
  'sp', 'pr', 'sc', 'rs',
  'ms', 'ro', 'ac', 'am',
  'rr', 'pa', 'ap', 'to',
  'ma', 'rn', 'pb', 'pe',
  'al', 'se', 'ba', 'mg',
  'rj', 'mt', 'go', 'df',
  'pi', 'ce', 'es',
]

sort = [
  {"dataNotificacao": {"order": "asc"}}
]

BASE_URL_PAIS = "https://elasticsearch-saps.saude.gov.br/desc-notificacoes-esusve-"

FAIXAS_ETARIAS = [
  [0, 4], [5, 9], [10, 14],
  [15, 19], [20, 29], [30, 39],
  [40, 49], [50, 59], [60, 69],
  [70, 79], [80, 999]
]

aggs = {
  "group_by_date":{
    "date_histogram": {
      "field": "dataNotificacao",
      "interval": 'day'
    }
  }
}

headers = {"Content-Type": "application/json"}

import requests
from requests.auth import HTTPBasicAuth
import json
from typing import List
import pandas as pd
import numpy as np

class ESUSNotificaConnector(object):

    estados: List = []
    municipio: str = ""
    querys: dict = {}

    def __init__(self):
        self.__prepare_queries()
        pass

    def __prepare_queries(self):
        pcr_positivo_ranges = {}
        for f in FAIXAS_ETARIAS:
            pcr_positivo_ranges["pcr-positivo-{}a{}".format(f[0], f[1])] = {}
            pcr_positivo_ranges["pcr-positivo-{}a{}".format(f[0], f[1])]['query_string'] = {
                "query": f"_exists_:resultadoTeste AND (Positivo) AND tipoTeste:(RT-PCR) AND idade:(>= {f[0]}) AND idade:(<= {f[1]}) AND dataNotificacao:[2020-01-01T00:00:00.000Z TO *]",
                "default_field": "resultadoTeste"
            }

        self.querys = {
            "pcr-positivo": {
                "query_string":{
                "query": "_exists_:resultadoTeste AND (Positivo) AND tipoTeste:(RT-PCR) AND dataNotificacao:[2020-01-01T00:00:00.000Z TO *]",
                "default_field": "resultadoTeste"
                }
            },
            "pcr-negativo": {
                "query_string": {
                "query": "_exists_:resultadoTeste AND (Negativo) AND tipoTeste:(RT-PCR) AND dataNotificacao:[2020-01-01T00:00:00.000Z TO *]",
                "default_field": "resultadoTeste"
                }
            }
        }

        for k in pcr_positivo_ranges:
            self.querys[k] = pcr_positivo_ranges[k]

        self.querys["obitos"] = {
            "query_string": {
                    "query": "_exists_:resultadoTeste AND (Positivo) AND tipoTeste:(RT-PCR) AND evolucaoCaso:(Óbito) AND dataNotificacao:[2020-01-01T00:00:00.000Z TO *]",
                    "default_field": "resultadoTeste"
            }
        }
        
        pass

    def load_data(self, estados):
        FIRST_TIME = True
        PAIS_FLAG = False
        ESTADOS_FLAG = False
        if "Brasil" in estados:
            estados = ESTADOS
            PAIS_FLAG = True
        elif len(estados) > 1:
            ESTADOS_FLAG = True
        for estado in estados:
            print(f"== Acessando os dados do estado: [{estado}] ==")
            URL = BASE_URL_PAIS + f"{estado}/_search?pretty"
            dataframes = []
            for query_id in self.querys:
                query = {
                    "query": self.querys[query_id],
                    "sort": sort,
                    "aggs": aggs,
                    "size": QUERY_SIZE
                }
                response = requests.request("GET", URL, headers = headers, auth=(USERNAME, PASSWRD), data = json.dumps(query))
                json_resp = json.loads(response.text)
                rows = []
                try:
                    for el in json_resp['aggregations']['group_by_date']['buckets']:
                        rows.append({"Data": el['key_as_string'], query_id: el['doc_count']})
                except Exception as e:
                    raise Exception("Query possivelmente inválida! Verifique se os dados parametrizados estão corretos. Razão: " + str(e))

                df_q = pd.DataFrame(rows)
                if len(df_q.index) != 0:
                    df_q.replace(np.nan, 0, inplace=True)
                    df_q["Data"] = (pd.to_datetime(df_q["Data"], format="%Y-%m-%d")).dt.date
                    dataframes.append(df_q)
                else:
                    print(f"\tQuery {query_id} sem retorno. Não será incluída no relatório final!")
            if FIRST_TIME:
                df = dataframes[0]
                for d in dataframes[1:]:
                    df = pd.merge(left=df, right=d, left_on=['Data'], right_on=['Data'], how='left')
                FIRST_TIME = False
            else:
                df_estado = dataframes[0]
                for d in dataframes[1:]:
                    df_estado = pd.merge(left=df_estado, right=d, left_on=['Data'], right_on=['Data'], how='left')
                df = pd.concat([df, df_estado], axis=0)
                
        print(f'Estado [{estado}] processado.\n')

        if PAIS_FLAG or ESTADOS_FLAG:
            df = df.groupby(by=['Data'], as_index=False).agg('sum')
            df.sort_values(by='Data', inplace=True)
        return df

