'''

Code author: @wallissoncarvalho
Github repository: github.com/wallissoncarvalho/HidroData
Under BSD 3-Clause "New" or "Revised" License

'''


import xml.etree.ElementTree as ET
import requests
import pandas as pd
import calendar    
import geopandas as gpd
from tqdm import tqdm

def inventario(params={'codEstDE': '', 'codEstATE': '', 'tpEst': '1', 'nmEst': '', 'nmRio': '', 'codSubBacia': '', 'codBacia': '',
          'nmMunicipio': '', 'nmEstado': '', 'sgResp': '', 'sgOper': '', 'telemetrica': ''}):
    '''
    Essa função busca as estações cadastradas no inventário do Hidroweb.
    Por padrão o código está buscando estações fluviométricas. Você pode buscar por estações pluviométrias alterando o argumento tpEst para 2.
    
    Se você quiser passar uma seleção específica crie um dicionário com os seguintes parâmetros:
        codEstDE: Código de 8 dígitos da estação - INICIAL (Ex.: 00047000)
        codEstATE: Código de 8 dígitos da estação - FINAL (Ex.: 90300000)
        tpEst: Tipo da estação (1-Flu ou 2-Plu)
        nmEst: Nome da Estação (Ex.: Barra Mansa)
        nmRio: Nome do Rio (Ex.: Rio Javari)
        codSubBacia: Código da Sub-Bacia hidrografica (Ex.: 10)
        codBacia: Código da Bacia hidrografica (Ex.: 1)
        nmMunicipio: Município (Ex.: Itaperuna)
        nmEstado: Estado (Ex.: Rio de Janeiro)
        sgResp: Sigla do Responsável pela estação (Ex.: ANA)
        sgOper: Sigla da Operadora da estação (Ex.: CPRM)
        telemetrica: (Ex: 1-SIM ou 0-NÃO)
    
    Dicionário com campos obritaróios, preencha os campos desejados:
    params = {'codEstDE': '', 'codEstATE': '', 'tpEst': '', 'nmEst': '', 'nmRio': '', 'codSubBacia': '', 'codBacia': '',
          'nmMunicipio': '', 'nmEstado': '', 'sgResp': '', 'sgOper': '', 'telemetrica': ''}  
    '''
    check_params = ['codEstDE', 'codEstATE', 'tpEst', 'nmEst', 'nmRio', 'codSubBacia',
                    'codBacia', 'nmMunicipio', 'nmEstado', 'sgResp', 'sgOper', 'telemetrica']
    if list(params.keys())!=check_params:
        print('O argumento params deve estar vazio ou conter os campos obrigatórios, use help(inventario) para mais informações.')
        return 
     
    response = requests.get('http://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroInventario', params)
    tree = ET.ElementTree(ET.fromstring(response.content))
    root = tree.getroot()
    if params['tpEst'] == '1':
        index=1
        stations = pd.DataFrame(columns=['Nome','Codigo', 'Tipo','AreaDrenagem','SubBacia', 'Municipio','Estado',
                                         'Responsavel', 'Latitude', 'Longitude'])
        for station in tqdm(root.iter('Table')):
            stations.at[index, 'Nome'] = station.find('Nome').text
            stations.at[index, 'Codigo'] = station.find('Codigo').text
            stations.at[index, 'Tipo'] = station.find('TipoEstacao').text
            stations.at[index, 'AreaDrenagem'] = station.find('AreaDrenagem').text
            stations.at[index, 'SubBacia'] = station.find('SubBaciaCodigo').text
            stations.at[index, 'Municipio'] = station.find('nmMunicipio').text
            stations.at[index, 'Estado'] = station.find('nmEstado').text
            stations.at[index, 'Responsavel'] = station.find('ResponsavelSigla').text
            stations.at[index, 'Latitude'] = float(station.find('Latitude').text)
            stations.at[index, 'Longitude'] = float(station.find('Longitude').text)
            index+=1
    elif params['tpEst'] == '2':
        index=1
        stations = pd.DataFrame(columns=['Nome','Codigo', 'Tipo','SubBacia', 'Municipio','Estado',
                                         'Responsavel', 'Latitude', 'Longitude'])
        for station in tqdm(root.iter('Table')):

            stations.at[index, 'Nome'] = station.find('Nome').text
            stations.at[index, 'Codigo'] = station.find('Codigo').text
            stations.at[index, 'Tipo'] = station.find('TipoEstacao').text
            stations.at[index, 'SubBacia'] = station.find('SubBaciaCodigo').text
            stations.at[index, 'Municipio'] = station.find('nmMunicipio').text
            stations.at[index, 'Estado'] = station.find('nmEstado').text
            stations.at[index, 'Responsavel'] = station.find('ResponsavelSigla').text
            stations.at[index, 'Latitude'] = float(station.find('Latitude').text)
            stations.at[index, 'Longitude'] = float(station.find('Longitude').text)
            index+=1
    else:
        print('Por favor selecione um tipo de estação, use help(inventario) para mais informações.')
        return
    stations = gpd.GeoDataFrame(stations, geometry = gpd.points_from_xy(stations.Longitude, stations.Latitude))
    return stations

def stations(list_stations, tipoDados):
    '''
       A partir de uma lista com o número das estações, essa função retorna a série de dados diárias em um dataframe
       tipoDados deve ser em formato string (e.g. '2')
    '''   
    params = {'codEstacao': '', 'dataInicio': '', 'dataFim': '', 'tipoDados': '', 'nivelConsistencia': ''}
    typesData = {'3': ['Vazao{:02}'], '2': ['Chuva{:02}'], '1': ['Cota{:02}']}
    params['tipoDados'] = tipoDados
    data_stations = []
    nodata_stations = []
    not_try = []
    
    for station in tqdm(list_stations):
        params['codEstacao'] = str(station)
        try:
            response = requests.get('http://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroSerieHistorica', params, timeout=50.0)
        except (requests.ConnectTimeout, requests.HTTPError, requests.ReadTimeout, requests.Timeout, requests.ConnectionError):
            not_try.append(station)
            continue
        
        tree = ET.ElementTree(ET.fromstring(response.content))
        root = tree.getroot()
        df=[]
        for month in root.iter('SerieHistorica'):
            codigo = month.find('EstacaoCodigo').text
            consist = int(month.find('NivelConsistencia').text)
            date = pd.to_datetime(month.find('DataHora').text,dayfirst=True)
            date = pd.Timestamp(date.year, date.month, 1, 0)
            last_day=calendar.monthrange(date.year,date.month)[1]
            month_dates = pd.date_range(date,periods=last_day, freq='D')  
            data = []
            list_consist = []
            for i in range(last_day):
                value = typesData[params['tipoDados']][0].format(i+1)
                try:
                    data.append(float(month.find(value).text))
                    list_consist.append(consist)
                except TypeError:
                    data.append(month.find(value).text)
                    list_consist.append(consist)
                except AttributeError:
                    data.append(None)
                    list_consist.append(consist)
            index_multi = list(zip(month_dates,list_consist))
            index_multi = pd.MultiIndex.from_tuples(index_multi,names=["Date","Consistence"])
            df.append(pd.DataFrame({f'{int(codigo):08}': data}, index=index_multi))
        if (len(df))>0:
            df = pd.concat(df)
            df = df.sort_index()
            drop_index = df.reset_index(level=1,drop=True).index.duplicated(keep='last')
            df = df[~drop_index]
            df = df.reset_index(level=1, drop=True)
            series = df[f'{int(codigo):08}']
            date_index = pd.date_range(series.index[0], series.index[-1], freq='D')
            series=series.reindex(date_index)
            data_stations.append(series)
        else:
            nodata_stations.append(station)
        
    data_stations = pd.concat(data_stations, axis=1)
    return data_stations, nodata_stations
