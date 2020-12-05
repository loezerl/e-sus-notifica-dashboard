import datetime
import pandas as pd
import numpy as np
import streamlit as st
from connector import ESUSNotificaConnector, ESTADOS

REGIOES = {
  "norte": ['ac', 'ap', 'am', 'pa', 'ro', 'rr', 'to'],
  "nordeste": ['al', 'ba', 'ce', 'ma', 'pb', 'pe', 'pi', 'rn', 'se'],
  "centro-oeste": ['go', 'mt', 'ms', 'df'],
  "sudeste": ['es', 'mg', 'rj', 'sp',],
  "sul": ['pr', 'rs', 'sc'],
}

FAIXA_ETARIA = {
    "0 a 4 anos": [False, 'pcr-positivo-0a4'],
    "5 a 9 anos": [False, 'pcr-positivo-5a9'],
    "10 a 14 anos": [False, 'pcr-positivo-10a14'],
    "15 a 19 anos": [False, 'pcr-positivo-15a19'],
    "20 a 29 anos": [False, 'pcr-positivo-20a29'],
    "30 a 39 anos": [False, 'pcr-positivo-30a39'],
    "40 a 49 anos": [False, 'pcr-positivo-40a49'],
    "50 a 59 anos": [False, 'pcr-positivo-50a59'],
    "60 a 69 anos": [False, 'pcr-positivo-60a69'],
    "70 a 79 anos": [False, 'pcr-positivo-70a79'],
    "Maior que 80 anos": [False, 'pcr-positivo-80a999']
}

esus = ESUSNotificaConnector()

st.title("e-SUS Notifica Dashboard")
st.write("""
Este dashboard tem como objetivo facilitar a comunicação com a API do e-SUS Notifica
para extrair os dados relacionados à covid-19 e construir uma base temporal. As informações
calculadas por dia são:
- Quantidade de RT-PCR positivos por dia.
- Quantidade de RT-PCR negativos por dia.
- Quantidade de RT-PCR positivos nas faixas etárias: [0, 4], [5, 9], [10, 14], [15, 19],
[20, 29], [30, 39], [40, 49], [50, 59] [60, 69], [70, 79], [80, 999].

A partir desses dados é possível extrair informações como (pelo país, estado ou região):
- Quantidade de RT-PCR positivo distribuído percentualmente por faixa etária: A partir dessa informação
pode-se ilustrar ao longo de um período temporal as faixas etárias e seus respectivos testes positivos.
- Positividade: Percentual de testes positivos em razão da quantidade de testes realizados.

Caso queira extrair outras informações dos dados gerados, utilize o *script* original:
https://github.com/loezerl/e-sus-notifica-series

Autor: Lucas Loezer (https://github.com/loezerl/)
""")
st.header("Deseja filtrar o resultado por Região, Estados ou utilizar o país todo?")
choosed_filter = st.radio('Selecione o filtro', ['Região', 'Estado', 'Brasil'], index=2)
if choosed_filter == "Região":
    choosed_regiao = st.radio("Selecione a Região", [k for k in REGIOES])
    choosed_estado = REGIOES[choosed_regiao]
elif choosed_filter == "Estado":
    choosed_estado = st.multiselect('Selecione os Estados', ESTADOS)
else:
    choosed_estado = ['Brasil']

@st.cache
def load_data(choosed_estado):
    df = esus.load_data(choosed_estado)
    df.replace(np.nan, 0, inplace=True)
    df['Data'] = pd.to_datetime(df['Data'], format='%Y-%m-%d')
    return df

df = load_data(choosed_estado)
st.write("Dados extraídos!")
st.write(str(choosed_estado))
st.write(df.head(10))

st.subheader("Deseja filtrar por algum período específico?")

d3 = st.date_input("Período selecionado", [], min_value=datetime.datetime.strptime("2020-01-01", "%Y-%m-%d"))
if len(d3) > 0:
    df = df[df['Data'] >= str(d3[0])]
    df = df[df['Data'] <= str(d3[1])]
    st.write("Início: {} | Fim: {}".format(str(d3[0]), str(d3[1])))
    st.subheader("Deseja filtrar por alguma faixa etária?")
    for k in FAIXA_ETARIA:
        FAIXA_ETARIA[k][0] = st.checkbox(k)

    filtro_idades = []
    for k in FAIXA_ETARIA:
        if FAIXA_ETARIA[k][0]:
            filtro_idades.append(FAIXA_ETARIA[k][1])

    df['pcr'] = df['pcr-positivo'] + df['pcr-negativo']
    df = df[df['pcr-positivo'] != 0]
    ignore_columns = [
        'Data',
        'pcr-positivo',
        'pcr-negativo',
        'pcr',
        'obitos'
    ]
    df = df[ignore_columns + filtro_idades]
    st.write(df.head())
    drop_columns = []
    plot_columns = []
    ## Percentual de positividade por faixa etária
    for c in df.columns.values:
        if not(c in ignore_columns):
            df[c + "_percent"] = ((df[c]/df['pcr-positivo'])*100).round(3)
            drop_columns.append(c)
            plot_columns.append(c + "_percent")
    ## Percentual de positividade por teste realizado
    df["positividade"] = ((df['pcr-positivo'] / df['pcr']) * 100).round(3)
    df.set_index('Data', inplace=True)
    st.subheader("Gráfico de positivos (%) por faixa etária")
    st.area_chart(df[plot_columns])

    st.subheader("Gráfico de positividade (%)")
    st.line_chart(df['positividade'])


