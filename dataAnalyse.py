import pandas as pd
import datetime as dt
import string
import time

## Carregando dados 
df = pd.read_csv("apache.log", sep=" ", names=['host', 'delete', 'logname', 'user', 'time', 'request', 'response', 'bytes', 'url', 'browserLog', 'browser', 'networkClass' ])
df.drop('delete', axis=1, inplace=True)

## Pré processamento 
def networkClass(ip):
    ip2 = int(ip.split(".")[0])
    if ip2 >= 0 and ip2 <= 127:
        return "Classe A"
    if ip2 >= 128 and ip2 <= 191:
        return "Classe B"
    if ip2 >= 192 and ip2 <= 223:
        return "Classe C"
    if ip2 >= 224 and ip2 <= 239:
        return "Classe D"
    if ip2 >= 240 and ip2 <= 255:
        return "Classe E"

def sepDate(dt):
    dat = str(time.strptime(dt, "[%Y-%m-%dT%H:%M:%SZ]")[0]) + "-" + str(time.strptime(dt, "[%Y-%m-%dT%H:%M:%SZ]")[1]) + "-" + str(time.strptime(dt, "[%Y-%m-%dT%H:%M:%SZ]")[2])
    return dat

def analyseResponse(resp):
    if int(resp) >= 100 and int(resp) <= 199:
        return "Informational"
    if int(resp) >= 200 and int(resp) <= 299:
        return "Sucess"
    if int(resp) >= 300 and int(resp) <= 399:
        return "Redirection"
    if int(resp) >= 400 and int(resp) <= 499:
        return "Client Error"
    if int(resp) >= 500 and int(resp) <= 599:
        return "Server Error"

df['browser'] = df['browserLog'].apply(lambda br: br.split(" ")[0])
df['networkClass'] = df['host'].apply(networkClass)
df['hour'] = df['time'].apply(lambda d: time.strptime(d, "[%Y-%m-%dT%H:%M:%SZ]")[3])
df['minute'] = df['time'].apply(lambda d: time.strptime(d, "[%Y-%m-%dT%H:%M:%SZ]")[4])
df['second'] = df['time'].apply(lambda d: time.strptime(d, "[%Y-%m-%dT%H:%M:%SZ]")[5])
df['date'] = df['time'].apply(sepDate)
df['endpoint'] = df['request'].apply(lambda end: end.split("/")[1])
df['statusResponse'] = df['response'].apply(analyseResponse)

## Análise dos dados

# Dataframe para armazenar os resultados
dfResult = pd.DataFrame()

# Separando dados por data
dates = df['date'].value_counts().index.tolist()

for date in dates:
    # Separando subset da data
    subset_date = df[df['date'] == date]

    # Desafio 1: 5 logins que mais efetuaram requisições
    desafio_01 = subset_date['user'].value_counts().head(5).index.tolist()

    # Desafio 2: 10 browsers mais utilizados
    desafio_02 = subset_date['browser'].value_counts().head(10).index.tolist()

    # Desafio 3: os endereços de rede classe C com maior quantidade de requisições
    subset_class = subset_date[subset_date['networkClass'] == 'Classe C']
    desafio_03 = subset_class['host'].value_counts().head(5).index.tolist()

    # Desafio 4: A hora com mais acesso no dia
    desafio_04 = subset_date['hour'].value_counts().head(1).index.tolist()

    # Desafio 5: A hora com maior consumo de bytes
    groupHour = subset_date.groupby(['hour'])
    desafio_05 = groupHour['bytes'].sum().sort_values(ascending=False).index.tolist()[0]

    # Desafio 6: O endpoint com maior consumo de bytes
    groupEndpoint = subset_date.groupby(['endpoint'])
    desafio_06 = groupEndpoint['bytes'].sum().sort_values(ascending=False).index.tolist()[0]

    # Desafio 7: A quantidade de bytes por minuto
    # Obs: Aqui considerei o total de minutos entre o primeiro e o último log
    finalIndex = subset_date.shape[0]
    finalIndex = finalIndex - 1
    
    hour_ini = subset_date['hour'].iloc[0]
    minu_ini = subset_date['minute'].iloc[0]
    hour_fin = subset_date['hour'].iloc[finalIndex]
    minu_fin = subset_date['minute'].iloc[finalIndex]

    totalMinutes = 0
    if hour_ini < hour_fin:
        totalMinutes = (60 - minu_ini) + ((hour_fin - (hour_ini + 1)) * 60) + minu_fin
    else:
        totalMinutes = minu_fin - minu_ini

    desafio_07 = subset_date['bytes'].sum() / totalMinutes

    # Desafio 8: A quantidade de bytes por hora
    # Obs: Aqui considerei a média de do total de bytes do dia pelo total de horas
    totalHours = totalMinutes / 60
    desafio_08 = subset_date['bytes'].sum() / totalHours
    desafio_08

    # Desafio 9: A quantidade de usuários por minuto
    desafio_09 = subset_date.shape[0] / totalMinutes

    # Desafio 10: A quantidade de usuários por hora
    desafio_10 = subset_date.shape[0] / totalHours

    # Desafio 11: a quantidade de requisições que tiveram erros, agrupadas por erro
    subset_erro = df[df['statusResponse'] == 'Client Error']
    desafio_11 = subset_erro.shape[0]

    desafio_11_group = subset_erro.groupby('response').size().sort_values(ascending=False).to_dict()
    
    # Desafio 12: A quantidade de requisições que tiveram sucesso
    desafio_12 = subset_date[subset_date['statusResponse'] == "Sucess"].shape[0]

    # Desafio 13: A quantidade de requisições que foram redirecionadas
    desafio_13 = subset_date[subset_date['statusResponse'] == "Redirection"].shape[0]

    ## Gravando resultado
    obj = {'date:' : date,
            '05_login_mais' : [list(desafio_01)],
            '10_browser_mais': [list(desafio_02)],
            '05_classe_c': [list(desafio_03)],
            'hora_mais_ acesso': [desafio_04[0]],
            'hora_maior_bytes': [desafio_05],
            'endpoint_mais_bytes': [desafio_06],
            'bytes_minuto' : [desafio_07],
            'bytes_hora': [desafio_08],
            'usuarios_minuto': [desafio_09],
            'usuarios_hora': [desafio_10],
            'qtd_erro_cliente': [desafio_11],
            'qtd_erro_agrup': [desafio_11_group],
            'qtd_sucesso': [desafio_12],
            'qtd_redirecionadas': [desafio_13]
            }
    dfData = pd.DataFrame(data=obj, index=[0])
    
    dfResult = pd.concat([dfResult, dfData])


dfResult.to_csv('resultado.csv', index=False)