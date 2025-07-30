import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from polygon import RESTClient
from datetime import timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
import streamlit as st
import io
import os
import zipfile
import numpy as np
import requests
import pytz
from datetime import time
import datetime as dt

# Variables globales para almacenar datos1 y datos2
datos1 = None
datos2 = None

def open_close_30min(ticker, api_key, fecha_inicio, fecha_fin):
    client = RESTClient(api_key)
    local_tz = pytz.timezone('America/New_York')
    
    try:
        # --- LÍNEA MODIFICADA ---
        # Obtener datos agregados cada 30 minutos
        resp = client.get_aggs(ticker=ticker, multiplier=1, timespan="hour", 
                               from_=fecha_inicio, to=fecha_fin)
        
        datos = [{
            'fecha': pd.to_datetime(agg.timestamp, unit='ms').tz_localize('UTC').tz_convert(local_tz),
            'open': agg.open, 
            'high': agg.high, 
            'low': agg.low, 
            'close': agg.close, 
            'volume': agg.volume
        } for agg in resp]
        
        df_OC = pd.DataFrame(datos)
        
        if df_OC.empty:
            return pd.DataFrame()
            
        df_OC['fecha'] = df_OC['fecha'].dt.tz_localize(None)
        df_OC.set_index('fecha', inplace=True)
        df_OC.index = pd.to_datetime(df_OC.index)
        
        fecha_inicio = pd.to_datetime(fecha_inicio)
        fecha_fin = pd.to_datetime(fecha_fin)
        
        df_OC = df_OC[(df_OC.index >= fecha_inicio) & (df_OC.index <= fecha_fin)]
        
        return df_OC
    
    except Exception as e:
        print(f"Error al obtener datos para {ticker}: {str(e)}")
        return pd.DataFrame()

def open_close(ticker, api_key, fecha_inicio, fecha_fin):
    global datos1, datos2
    ticker = "SPY"
    client = RESTClient(api_key)
    local_tz = pytz.timezone('America/New_York')
    i = 1
    try:
        # Obtener datos agregados cada 15 minutos
        resp = client.get_aggs(ticker=ticker, multiplier=5, timespan="minute", #VOLVER A CAMBIAR A 15 MIN
                               from_=fecha_inicio, to=fecha_fin)
        #st.write(resp)
        datos = [{
            'fecha': pd.to_datetime(agg.timestamp, unit='ms').tz_localize('UTC').tz_convert(local_tz),
            'open': agg.open, 
            'high': agg.high, 
            'low': agg.low, 
            'close': agg.close, 
            'volume': agg.volume
        } for agg in resp]
        
   
        df_OC = pd.DataFrame(datos)
        # Convertir timestamps aware a naive eliminando la zona horaria
        df_OC['fecha'] = df_OC['fecha'].dt.tz_localize(None)
        
        # Establecer la columna 'fecha' como el índice del DataFrame
        df_OC.set_index('fecha', inplace=True)
        df_OC.index = pd.to_datetime(df_OC.index)
        
        # Asegurarse de que las fechas de inicio y fin son de tipo datetime
        fecha_inicio = pd.to_datetime(fecha_inicio)
        fecha_fin = pd.to_datetime(fecha_fin)
        
        # Crear variables dinámicas datos1 y datos2
        if datos:
            if i == 1:
                datos1 = pd.DataFrame(df_OC)
                i += 1
                #st.dataframe(datos1)
            elif i == 2:
                datos2 = pd.DataFrame(df_OC)
                i += 1
        
        # Filtrar el DataFrame por las fechas de inicio y fin
        df_OC = df_OC[(df_OC.index >= fecha_inicio) & (df_OC.index <= fecha_fin)]
        
        
        
        
        return df_OC
    
    except Exception as e:
        print(f"Error al obtener datos para {ticker}: {str(e)}")
        return pd.DataFrame()

# Mostrar los datos almacenados en datos1 y datos2
def mostrar_datos():
    global datos1, datos2
    datos_final = None
    if datos1 is not None:
        print("Datos1:")
        print(datos1)
        datos_final =  pd.concat([datos_final, datos1])
    else:
        print("No se han obtenido datos para datos1.")
    
    if datos2 is not None:
        print("Datos2:")
        print(datos2)
    else:
        print("No se han obtenido datos para datos2.")
        
    st.write("datos completos:")
    st.dataframe(datos_final)
    
    return datos_final

def get_spy_intraday_financial_modeling(fecha_inicio, fecha_fin):
    # Convertir fechas a datetime
    fecha_inicio = pd.to_datetime(fecha_inicio)
    fecha_fin = pd.to_datetime(fecha_fin)
    API_KEY = "dXm5M61pLypaHuujU7K4ULqol9IEWNp3"
 
    base_url = 'https://financialmodelingprep.com/api/v3/historical-chart/15min/SPY' #VOLVER A CAMBIAR A 15 MIN
    params = {
        'from': fecha_inicio,
        'to': fecha_fin,
        'apikey': API_KEY
    }
 
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        print('Failed to retrieve data')
        return None
    
    data = response.json()
    df_fm = pd.DataFrame(data)
 
    df_fm.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
    df_fm['date'] = pd.to_datetime(df_fm['date'])
    df_fm = df_fm.set_index('date')
    
    # Filtrar por rango de fechas
    df_fm = df_fm[(df_fm.index >= fecha_inicio) & (df_fm.index <= fecha_fin)]
    
    # Ordenar el DataFrame por fecha ascendente
    df_fm.sort_index(inplace=True)
    
    return df_fm
             
def get_open_and_close(ticker, api_av, fecha_inicio, fecha_fin):
    # Configuración de la URL y los parámetros para la API de Alpha Vantage
    url = "https://www.alphavantage.co/query"
    # Convertir fechas a datetime
    fecha_inicio = pd.to_datetime(fecha_inicio)
    fecha_fin = pd.to_datetime(fecha_fin)
    
    fecha_actual = fecha_inicio
    df_completo = pd.DataFrame()
    while fecha_actual <= fecha_fin:
        ultimo_dia_mes = min(fecha_actual + relativedelta(day=31), fecha_fin)
        if ultimo_dia_mes > fecha_fin:
            ultimo_dia_mes = fecha_fin
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": ticker,
            "interval": "5min", #VOLVER A PONER A 15 MIN
            "apikey": api_av,
            "outputsize": "full",
            "extended_hours": "false",
            "month": fecha_actual.strftime("%Y-%m")
        }
        
        # Realizar la solicitud a la API de Alpha Vantage
        response = requests.get(url, params=params)
        data = response.json()
        #st.write("Respuesta JSON completa:", data)
        
        # Imprimir la respuesta completa en formato JSON (solo para verificación)
        #print(data)
        
        # Verificar que la respuesta contiene los datos de series temporales
        if "Time Series (5min)" in data:
            time_series = data["Time Series (5min)"]
            df = pd.DataFrame.from_dict(time_series, orient='index')        
            df.rename(columns=lambda x: x[3:].strip(), inplace=True)
            
            df[['open', 'close']] = df[['open', 'close']].apply(pd.to_numeric)
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            
            #st.write("DataFrame completo antes de filtrar por fecha:", df)
            
            # Asegurarse de que las fechas de inicio y fin son de tipo datetime
            fecha_inicio = pd.to_datetime(fecha_inicio)
            fecha_fin = pd.to_datetime(fecha_fin)
            #fecha_inicio = pd.Timestamp(fecha_inicio)
            #fecha_fin = pd.Timestamp(fecha_fin)
            
            # Convertir a valores numéricos
            for col in df.columns:
                df[col] = pd.to_numeric(df[col])
            
            # Filtrar por rango de fechas
            df = df[(df.index >= fecha_actual) & (df.index <= ultimo_dia_mes)]
            
            #agregar al dataframe completo
            df_completo = pd.concat([df_completo, df])
            #st.dataframe(df_completo)
            
           #st.write("DataFrame filtrado por rango de fechas:", df)
            #st.write("Valores de Open y Close para el rango de fechas:", df_completo[['open', 'close']])
            return df_completo
            
        else:
            print("No se encontraron datos para el ticker proporcionado.")
            return pd.DataFrame()
        fecha_actual=ultimo_dia_mes + pd.Timedelta(days=1)
    
    
#api_av = "KCIUEY7RBRKTL8GI"
api_av = "A5FNH8G5HZAQHL2"

def listar_archivos_xlxs(directorio):
    archivos = [archivo for archivo in os.listdir(directorio) if archivo.endswith('.xlsx')]
    return archivos


def cargar_datos(filepath):
    data = pd.read_excel(filepath)
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date']).dt.tz_localize(None)
    else:
        return None
        
    data = data.set_index('date')
    return data

def verificar_opcion(client, ticker, start_date, end_date):
    try:
        resp = client.get_aggs(ticker=ticker, multiplier=1, timespan="day", from_=start_date.strftime('%Y-%m-%d'), to=end_date.strftime('%Y-%m-%d'))
        return len(resp) > 0
    except:
        return False
    
def verificar_opcion_15min(client, ticker, fecha_inicio, fecha_fin):
    try:
        resp = client.get_aggs(ticker=ticker, multiplier=15, timespan="minute", from_=fecha_inicio.strftime('%Y-%m-%d'), to=fecha_fin.strftime('%Y-%m-%d'))
        return len(resp) > 0
    except:
        return False
    
def obtener_historico(ticker_opcion, api_key, fecha_inicio, fecha_fin):
    client = RESTClient(api_key)
    resp = client.get_aggs(ticker=ticker_opcion, multiplier=1, timespan="day", from_=fecha_inicio.strftime('%Y-%m-%d'), to=fecha_fin.strftime('%Y-%m-%d'))
    datos = [{'fecha': pd.to_datetime(agg.timestamp, unit='ms'), 'open': agg.open, 'close': agg.close} for agg in resp]
    df = pd.DataFrame(datos)
    df.set_index('fecha', inplace=True)
    df.index = df.index.date
    return df

def obtener_historico_30min_start_time(ticker_opcion, api_key, fecha_inicio, fecha_fin):
    client = RESTClient(api_key)
    local_tz = pytz.timezone('America/New_York') # Importante para la zona horaria de NY

    try:
        # 1. Parámetros cambiados para obtener datos cada 30 minutos
        resp = client.get_aggs(ticker=ticker_opcion, multiplier=1, timespan="hour", 
                               from_=fecha_inicio.strftime('%Y-%m-%d'), to=fecha_fin.strftime('%Y-%m-%d'))
        
        # 2. Se incluyen todos los datos (high, low, volume) que son útiles para intradía
        datos = [{
            'fecha': pd.to_datetime(agg.timestamp, unit='ms').tz_localize('UTC').tz_convert(local_tz),
            'open': agg.open,
            'high': agg.high,
            'low': agg.low,
            'close': agg.close,
            'volume': agg.volume
        } for agg in resp]
        
        df = pd.DataFrame(datos)
        
        if df.empty:
            return pd.DataFrame()

        # 3. Se procesa la fecha y se establece como índice (conservando la hora)
        df['fecha'] = df['fecha'].dt.tz_localize(None)
        df.set_index('fecha', inplace=True)
        
        return df

    except Exception as e:
        print(f"Error al obtener datos para {ticker_opcion}: {str(e)}")
        return pd.DataFrame()

def obtener_historico_30min(ticker_opcion, api_key, fecha_inicio, fecha_fin):
    client = RESTClient(api_key)
    local_tz = pytz.timezone('America/New_York') # Importante para la zona horaria de NY

    try:
        # 1. Parámetros cambiados para obtener datos cada 30 minutos
        resp = client.get_aggs(ticker=ticker_opcion, multiplier=1, timespan="minute", 
                               from_=fecha_inicio.strftime('%Y-%m-%d'), to=fecha_fin.strftime('%Y-%m-%d'))
        
        # 2. Se incluyen todos los datos (high, low, volume) que son útiles para intradía
        datos = [{
            'fecha': pd.to_datetime(agg.timestamp, unit='ms').tz_localize('UTC').tz_convert(local_tz),
            'open': agg.open,
            'high': agg.high,
            'low': agg.low,
            'close': agg.close,
            'volume': agg.volume
        } for agg in resp]
        
        df = pd.DataFrame(datos)
        
        if df.empty:
            return pd.DataFrame()

        # 3. Se procesa la fecha y se establece como índice (conservando la hora)
        df['fecha'] = df['fecha'].dt.tz_localize(None)
        df.set_index('fecha', inplace=True)
        
        return df

    except Exception as e:
        print(f"Error al obtener datos para {ticker_opcion}: {str(e)}")
        return pd.DataFrame()

def obtener_historico_15min(ticker_opcion, api_key, fecha_inicio, fecha_fin):
    #fecha_fin = fecha_fin
    #st.write(type(fecha_inicio))
    # Agregar 1 día
    #fecha_fin = fecha_inicio + timedelta(days=1)
    #st.write("fecha fin en historico 15min")
    #st.write(fecha_fin)
    #fecha_inicio.strftime('%Y-%m-%d')
    #api_av = "KCIUEY7RBRKTL8GI"
    #st.write(fecha_inicio)
    client = RESTClient(api_key)
    local_tz = pytz.timezone('America/New_York')
    try:
        # Obtener datos agregados cada 15 minutos
        resp = client.get_aggs(ticker=ticker_opcion, multiplier=5, timespan="minute", #CAMBIAR A 15 MIN
                               from_=fecha_inicio, to=fecha_fin)
        #st.write(resp)
        datos = [{
            'fecha': pd.to_datetime(agg.timestamp, unit='ms').tz_localize('UTC').tz_convert(local_tz),
            'open': agg.open, 
            'high': agg.high, 
            'low': agg.low, 
            'close': agg.close, 
            'volume': agg.volume
        } for agg in resp]
        
        #st.write(fecha_inicio)
        #st.write(fecha_inicio.strftime('%Y-%m-%d'))
        # Procesar la respuesta para crear el DataFrame
        #datos = [{'fecha': pd.to_datetime(agg.timestamp, unit='ms'), 'open': agg.open, 'high': agg.high, 
                  #'low': agg.low, 'close': agg.close, 'volume': agg.volume} for agg in resp]
        df = pd.DataFrame(datos)
        # Convertir timestamps aware a naive eliminando la zona horaria
        df['fecha'] = df['fecha'].dt.tz_localize(None)
        #Mostrar dataframe df, se mjuestra dos veces
        #st.dataframe(df)
        
        
        # Establecer la columna 'fecha' como el índice del DataFrame
        df.set_index('fecha', inplace=True)
        df.index = pd.to_datetime(df.index)
        
        # Asegurarse de que las fechas de inicio y fin son de tipo datetime
        #fecha_inicio = local_tz.localize(pd.to_datetime(fecha_inicio))
        #fecha_fin = local_tz.localize(pd.to_datetime(fecha_fin))
        fecha_inicio = pd.to_datetime(fecha_inicio)
        fecha_fin = pd.to_datetime(fecha_fin)
        
        # Filtrar el DataFrame por las fechas de inicio y fin
        df = df[(df.index >= fecha_inicio) & (df.index <= fecha_fin)]
        #st.dataframe(df)
        
        return df
    
    except Exception as e:
        print(f"Error al obtener datos para {ticker_opcion}: {str(e)}")
        return pd.DataFrame()

def obtener_historico_15min_pol(ticker_opcion, api_key, fecha_inicio, fecha_fin):
    #fecha_inicio.strftime('%Y-%m-%d')
    #api_av = "KCIUEY7RBRKTL8GI"
    #st.write(fecha_inicio)
    ticker_opcion = "SPY"
    client = RESTClient(api_key)
    local_tz = pytz.timezone('America/New_York')
    try:
        # Obtener datos agregados cada 15 minutos
        resp = client.get_aggs(ticker=ticker_opcion, multiplier=15, timespan="minute", 
                               from_=fecha_inicio, to=fecha_fin)
        #st.write(resp)
        datos = [{
            'fecha': pd.to_datetime(agg.timestamp, unit='ms').tz_localize('UTC').tz_convert(local_tz),
            'open': agg.open, 
            'high': agg.high, 
            'low': agg.low, 
            'close': agg.close, 
            'volume': agg.volume
        } for agg in resp]
        
        #st.write(fecha_inicio)
        #st.write(fecha_inicio.strftime('%Y-%m-%d'))
        # Procesar la respuesta para crear el DataFrame
        #datos = [{'fecha': pd.to_datetime(agg.timestamp, unit='ms'), 'open': agg.open, 'high': agg.high, 
                  #'low': agg.low, 'close': agg.close, 'volume': agg.volume} for agg in resp]
        df = pd.DataFrame(datos)
        # Convertir timestamps aware a naive eliminando la zona horaria
        df['fecha'] = df['fecha'].dt.tz_localize(None)
        #Mostrar dataframe df, se mjuestra dos veces
        #st.dataframe(df)
        
        
        # Establecer la columna 'fecha' como el índice del DataFrame
        df.set_index('fecha', inplace=True)
        df.index = pd.to_datetime(df.index)
        
        # Asegurarse de que las fechas de inicio y fin son de tipo datetime
        #fecha_inicio = local_tz.localize(pd.to_datetime(fecha_inicio))
        #fecha_fin = local_tz.localize(pd.to_datetime(fecha_fin))
        fecha_inicio = pd.to_datetime(fecha_inicio)
        fecha_fin = pd.to_datetime(fecha_fin)
        
        # Filtrar el DataFrame por las fechas de inicio y fin
        df = df[(df.index >= fecha_inicio) & (df.index <= fecha_fin)]
        #st.dataframe(df)
        
        return df
    
    except Exception as e:
        print(f"Error al obtener datos para {ticker_opcion}: {str(e)}")
        return pd.DataFrame()


def obtener_historico_15minn(ticker_opcion, api_key, fecha_inicio, fecha_fin):
    client = RESTClient(api_key)
    resp = client.get_aggs(ticker=ticker_opcion, multiplier=15, timespan="minute", from_=fecha_inicio.strftime('%Y-%m-%d'), to=fecha_fin.strftime('%Y-%m-%d'))
    base_url = "https://www.alphavantage.co/query"
    function = "TIME_SERIES_INTRADAY"
    interval = "15min"
    
    params = {
        "function": function,
        "symbol": ticker_opcion,
        "interval": interval,
        "apikey": api_key,
        "outputsize": "full",
        "extended_hours": "false"
    }
    
    try:
        response = requests.get(base_url, params=params)
        data = response.json()
        #st.write("Respuesta JSON completa:", data)  # También se muestra en Streamlit
        
        if "Time Series (15min)" not in data:
            print(f"No se recibieron datos para {ticker_opcion}")
            return pd.DataFrame()
        
        time_series = data["Time Series (15min)"]
        
        #st.dataframe(df_option)  # Mostrar el DataFrame en la interfaz
        
        
        df = pd.DataFrame.from_dict(time_series, orient='index')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        
        
        # Renombrar columnas
        df.columns = ['open', 'high', 'low', 'close', 'volume']
        
        # Convertir a valores numéricos
        for col in df.columns:
            df[col] = pd.to_numeric(df[col])
        
        # Filtrar por rango de fechas
        df = df[(df.index >= fecha_inicio) & (df.index <= fecha_fin)]
        
        if not df.empty:
            print(f"Datos recibidos para {ticker_opcion}:")
            print(f"Número de registros: {len(df)}")
            print(f"Primer registro: {df.iloc[0]}")
            print(f"Último registro: {df.iloc[-1]}")
        else:
            print(f"No hay datos en el rango de fechas especificado para {ticker_opcion}")
        
        return df
    
    except Exception as e:
       print(f"Error al obtener datos para {ticker_opcion}: {str(e)}")
       return pd.DataFrame()
    
def encontrar_opcion_cercana(client, base_date, option_price, column_name, option_days, option_offset, ticker):
    min_days = option_days - option_offset #23
    max_days = option_days + option_offset #37
    best_date = None
    for offset in range(min_days, max_days + 1):
        option_date = (base_date + timedelta(days=offset)).strftime('%y%m%d')
        option_type = 'C' if column_name == 1 else 'P'
        option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
        if verificar_opcion(client, option_name, base_date, base_date + timedelta(days=1)):
            best_date = option_date
            break
    return best_date

def encontrar_opcion_cercana_15min(client, base_date, option_price, column_name,option_days, option_offset, ticker):
    min_days = option_days - option_offset #23
    max_days = option_days + option_offset #37
    best_date = None
    for offset in range(min_days, max_days + 1):
        for hour_offset in range(0, 24 * 60, 5):  # Iterar cada 15 minutos   CAMBIAR A 15 MIN
            option_date = (base_date + timedelta(days=offset, minutes=hour_offset)).strftime('%y%m%d')       
            option_type = 'C' if column_name == 1 else 'P'
            option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
            #st.write("Dentro de la función 15min")
            #st.write(option_date)
            #st.write(option_name)
            if verificar_opcion(client, option_name, base_date, base_date + timedelta(days=1)):
                best_date = option_date
                break
    return best_date

def desfasar_strike(actual_strike, column_name, method = None, offset = 5):
    strike = actual_strike
    if method == "OTM":
        if column_name == 1:
            strike = int(actual_strike + offset)
        else:
            strike = int(actual_strike - offset)
 
    return strike

def encontrar_strike_cercano(client, base_date, option_price, column_name, option_days, option_offset, ticker, method, offset, it_max = 10):
    
    actual_option_price = desfasar_strike(option_price, column_name, method, offset)
    for i in range(it_max):
        best_date = encontrar_opcion_cercana(client, base_date, actual_option_price, column_name, option_days, option_offset, ticker)
        if best_date is not None:
            break
        if column_name == 1:
            actual_option_price += ((i + 1)*((-1)**i))
        else:
            actual_option_price -= ((i + 1)*((-1)**(i + 1)))

    return best_date, actual_option_price

option_hours = 1  # Buscar opciones cercanas en un rango de 1 hora
option_offset_minutes = 30  # Margen de 30 minutos en ambos sentidos
              
def realizar_backtest(data_filepath, api_key, ticker, balance_inicial, pct_allocation, fixed_amount, allocation_type, fecha_inicio, fecha_fin, option_days=30, option_offset=0, trade_type='Open to Close', periodo='Diario', column_name='toggle_false', method = "ATM", offset = 5, esce1=False):
    data = cargar_datos(data_filepath)
    balance = balance_inicial
    balance_posiciones = balance
    resultados = []
    client = RESTClient(api_key)
    
    
    # Variables para rastrear posiciones abiertas
    posiciones_abiertas = []  # Lista para almacenar las posiciones abiertas
    posicion_actual_abierta = False
    posicion_anterior_abierta = False
    trade_result_anterior = 0
    option_name_anterior = None
    etf_open_price_anterior = 0
    etf_open_price_anterior = 0
    tipo_posicion = None
    precio_entrada_anterior = 0
    precio_salida_anterior = 0
    precio_usar_apertura_anterior = 0
    precio_usar_cierre_anterior = 0
    fecha_entrada = None
    num_contratos_anterior = 0
    option_name = ''
    señal_anterior = None  # Para comparar señales entre días
    
    
    if periodo == 'Diario':
        fecha_inicio = fecha_inicio.date()
        fecha_fin = fecha_fin.date()
    else:
        fecha_inicio = pd.Timestamp(fecha_inicio)
        fecha_fin = pd.Timestamp(fecha_fin)

    if "Trades_M1" in data_filepath:
        nombre_de_la_columna = 'start_time'
        # Se crea la columna una sola vez, antes de recorrer
        data[f'siguiente_{nombre_de_la_columna}'] = data[nombre_de_la_columna].shift(-1)
        #hora_especifica = dt.time(23, 0, 0)
        #fecha_inicio = dt.datetime.combine(fecha_inicio.date(), hora_especifica)
        #fecha_fin = dt.datetime.combine(fecha_fin.date(), hora_especifica)
    if "Trades_H1_Best1" in data_filepath:
        nombre_de_la_columna = 'start_time'
        # Se crea la columna una sola vez, antes de recorrer
        data[f'siguiente_{nombre_de_la_columna}'] = data[nombre_de_la_columna].shift(-1)

    if "Trades_H1_Best2" in data_filepath:
        nombre_de_la_columna = 'start_time'
        # Se crea la columna una sola vez, antes de recorrer
        data[f'siguiente_{nombre_de_la_columna}'] = data[nombre_de_la_columna].shift(-1)
    
    if "Trades_H1_Best3" in data_filepath:
        nombre_de_la_columna = 'start_time'
        # Se crea la columna una sola vez, antes de recorrer
        data[f'siguiente_{nombre_de_la_columna}'] = data[nombre_de_la_columna].shift(-1)

        

    for date, row in data.iterrows():
        
        if periodo == 'Diario':
            date = date.date()
        else:
            date = pd.Timestamp(date)
            
        if date < fecha_inicio or date > fecha_fin:
            continue
        if row[column_name] not in [0, 1]:
            continue
        
        #if "Trades_H1" or "Trades_H1_Best1" or "Trades_H1_Best2" or "Trades_H1_Best3" in data_filepath:
        if "Trades_M1" in data_filepath:
            #st.write("--------------------------------------------------------------------------------")
        
            colombia_tz = 'America/Bogota'
            ny_tz = 'America/New_York'

            
            señal_actual = row[column_name]
            
            
                
            #2. Extraer tiempos de entrada y salida del archivo
            start_time = pd.to_datetime(row['start_time'])
            start_time = start_time.tz_localize(colombia_tz).tz_convert(ny_tz)
            start_time = start_time.tz_localize(None)

            next_start_time = row[f'siguiente_{nombre_de_la_columna}']
            next_start_time = next_start_time.tz_localize(colombia_tz).tz_convert(ny_tz)
            next_start_time = next_start_time.tz_localize(None)
            
            end_time = pd.to_datetime(row['end_time'])
            end_time = end_time.tz_localize(colombia_tz).tz_convert(ny_tz)
            end_time = end_time.tz_localize(None)
            
            precio_usar_apertura_excel = row['start_price']
            precio_usar_cierre_excel = row['end_price']
            option_price = round(row['start_price'])
            
            #st.write(f"Descargando historial intradía del SPY para la fecha {start_time}...")
            # Llama a tu función existente para obtener los datos del ETF
            spy_intraday_historial = open_close_30min("SPY", api_key, fecha_inicio, fecha_fin)
            #st.write(spy_intraday_historial)
            
            
            #st.write("Si está tomando el archivo")
            #st.write(start_time)
            #st.write(next_start_time)
            #st.write(end_time)
            #st.write(precio_usar_apertura_excel)
            #st.write(precio_usar_cierre_excel)
            #st.write(option_price)
            #st.write(señal_actual)
            
            if señal_actual in [0,1]:
                                       
                #Esto sería lo nuevo
                #if spy_intraday_historial.empty: #Esto a lo mejor ya no se necesita
                    #continue
                
                if trade_type == 'Close to Close':
                    precio_usar_apertura = 'close'
                    precio_usar_cierre = 'close'
                    index = 1
                    #option_price = round(spy_intraday_historial['Close'].iloc[0]) #cambiar a 'close'
                    
                elif trade_type == 'Close to Open':
                    precio_usar_apertura = 'close'
                    precio_usar_cierre = 'open'
                    index = 1                   
                    #option_price = round(spy_intraday_historial['Close'].iloc[0]) #cambiar a 'close'
                    
                else: #Open to Close
                    precio_usar_apertura = 'open'
                    precio_usar_cierre = 'close'
                    index = 0
                    #option_price = round(spy_intraday_historial['open'].iloc[0]) #Se basa en la apertura del día actual
                 
                option_date, actual_option_price = encontrar_strike_cercano(client, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                option_price = actual_option_price
                #st.write("option date")
                #st.write(option_date)
                if option_date:
                    option_type = 'C' if row[column_name] == 1 else 'P'
                    option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                    df_option_start_time = obtener_historico_30min_start_time(option_name, api_key, date, date + timedelta(days=option_days))
                    st.write("df_option_start_time:")
                    st.write(df_option_start_time)
                    df_option_end_time = obtener_historico_30min(option_name, api_key, date, date + timedelta(days=option_days))
                    st.write("df_option_end_time:")
                    st.write(df_option_end_time)
                    df_option_start_time = df_option_start_time.loc[start_time:]
                    st.write("df_option_start_time recortado a start_time:")
                    st.write(df_option_start_time)
                    df_option_end_time = df_option_end_time.loc[start_time:]
                    st.write("df_option_end_time recortado a start_time:")
                    st.write(df_option_end_time)
                    
                    if not df_option_start_time.empty:
                        st.write("entra porque el df_option_start_time no está vacío")
                        st.write(df_option_end_time.index)
                        df_option_end_time = df_option_end_time.loc[end_time:]
                        st.write("data frame empezando desde end_time o cercano:")
                        st.write(df_option_end_time)
                        #if not end_time in df_option.index:
                            #hacer end_time el siguiente registro del dataframe df_option.index 
                        #if end_time in df_option_end_time.index:
                        if not df_option_end_time.empty:
                            #st.write("entra acá porque end_time si está en df_option.index")
                            df_option_cierre = df_option_end_time.loc[end_time]
                            st.write("df_option recortado al cierre: a revisar")
                            st.write(df_option_cierre)
                            posicion_actual_abierta = True
                            option_open_price = df_option_start_time[precio_usar_apertura].iloc[0]##PENDIENTE DE REVISAR
                            #st.write("Precio de entrada para la opción día actual:")
                            #st.write(option_open_price)
                            option_close_price = df_option_start_time[precio_usar_cierre].iloc[index]
                            #st.write("Precio de salida opción día actual:")
                            #st.write(option_close_price)
                            option_close_price_cierre = df_option_cierre[precio_usar_cierre]
                            #st.write("Precio de salida opción día de cierre:")
                            #st.write(option_close_price_cierre)
                            max_contract_value = option_open_price * 100
                            #st.write(max_contract_value)
                            
                            if allocation_type == 'Porcentaje de asignación':
                                #st.write("Entra en este allocation_type")
                                if next_start_time < end_time:
                                    num_contratos = int((balance_posiciones * pct_allocation) / max_contract_value)
                                    #st.write(balance_posiciones)
                                    #st.write(num_contratos)
                                else: #next_start_time > end_time:
                                    num_contratos = int((balance * pct_allocation) / max_contract_value)
                                    #st.write(balance)
                                    #st.write(pct_allocation)
                                    #st.write(max_contract_value)
                                    #st.write(num_contratos)
                            else: #allocation_type == 'Monto fijo de inversión':
                                if balance < max_contract_value:
                                    #st.error("No hay suficiente dinero para abrir más posiciones. La ejecución del tester ha terminado.")
                                    return pd.DataFrame(resultados), balance
                                else: #balance >= max_contract_value
                                    num_contratos = int(fixed_amount / max_contract_value)
                            
                            #st.write("Numero de contratos día actual:")
                            #st.write(num_contratos)
                            #st.write("Option Type actual:")
                            #st.write(option_type)
                            cost_trade = max_contract_value * num_contratos
                            #st.write("Costo de la operación:")
                            #st.write(cost_trade)
                            
                            if next_start_time < end_time:
                                #st.write("Balance con posiciones abiertas:")
                                balance_posiciones -= cost_trade
                                #st.write(balance_posiciones)
                                trade_result = (df_option_cierre[precio_usar_cierre] - option_open_price) * 100 * num_contratos
                                balance += trade_result
                            else: #next_start_time > end_time:
                                #trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                                trade_result = (df_option_cierre[precio_usar_cierre] - option_open_price) * 100 * num_contratos
                                #st.write("Este es el precio de cierre de la opción para ese día:")
                                #st.write(df_option[precio_usar_cierre].iloc[index])
                                balance += trade_result
                                balance_posiciones = balance
                             
                            
                            #st.write("trade result actual positivo:")
                            #st.write(trade_result)
                            
                            # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                            #etf_data = yf.download("SPY", start="2022-01-01", end=date + pd.Timedelta(days=1), multi_level_index=False, auto_adjust=False)
                            #st.write("etf_data")
                            #st.write(etf_data)
                            #etf_data = etf_data.drop(etf_data.index[-1])
                            #etf_data.columns = etf_data.columns.str.lower()
                            #etf_data.index.name = 'date'
                            #etf_open_price = etf_data['open'].iloc[0] if not etf_data.empty else None
                            #st.write("Precio de entrada día actual:")
                            #st.write(etf_open_price)
                            #etf_close_price = etf_data['close'].iloc[0] if not etf_data.empty else None
                            #st.write("Precio salida día actual:")
                            #st.write(etf_close_price)
                            
                            
                            resultados.append({
                                'Fecha': start_time, 
                                'Tipo': 'Call' if row[column_name] == 1 else 'Put',
                                #'Pred': row[column_name],
                                'toggle_false': row[column_name],
                                'toggle_true': row[column_name],
                                'Fecha Apertura': start_time,
                                'Fecha Cierre': end_time,
                                #'Fecha Apertura': df_option.index[0],
                                #'Fecha Cierre': df_option.index[index],
                                'Precio Entrada': option_open_price, 
                                'Precio Salida': df_option_start_time[precio_usar_cierre].iloc[index],
                                #'Precio Salida Utilizado': df_option[precio_usar_cierre].iloc[index],
                                'Precio Salida Utilizado': df_option_cierre[precio_usar_cierre],
                                'Resultado': trade_result,
                                'Contratos': num_contratos,
                                'Opcion': option_name,
                                'Open': precio_usar_apertura_excel,
                                'Close': precio_usar_cierre_excel,
                                'Costo Posiciones': cost_trade,
                                'Balance Posiciones': balance_posiciones
                                #'Open Posición Abierta': etf_open_price,
                                #'Close Posición Abierta': etf_close_price
                            })
                            posicion_actual_abierta = False
                            print(trade_result)
                        else:
                            st.write("No entró al end_time en df_option.index")
        
        else: #El archivo no es Trades_H1
            if periodo == 'Diario':
                señal_actual = row[column_name]
                
                if 'probability' in data.columns:
                    #st.write("Si tiene la columna de modelos seleccionados")
                    ensamble = True
                else:
                    #st.write("El archivo no tiene la columna de modelos seleccionados")
                    ensamble = False
                    
                if 'Selecteed_Models' in data.columns:
                    #st.write("Si tiene la columna de modelos seleccionados")
                    ensamble = True
                else:
                    #st.write("El archivo no tiene la columna de modelos seleccionados")
                    ensamble = False
                
                #if ensamble and row['Selected_Models'] == "[]":
                if ensamble and row['probability'] == 0:
                    continue
                else: #Si tiene modelos
                    
                    # Nueva estrategia cuando el checkbox está seleccionado y el periodo es 'Diario'
                    if esce1:
                        if señal_actual in [0, 1]:
                            if posicion_anterior_abierta:  #posicion_anterior_abierta = True
                                st.write("Hay posiciones abiertas...")
                                st.write("date actual:")
                                st.write(date)
                                #Abrimos una nueva posición del día actual
                                data_for_date = yf.download("SPY", start="2022-01-01", end=date + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                                data_for_date = data_for_date.drop(data_for_date.index[-1])
                                #data_for_date.columns = data_for_date.columns.str.lower()
                                data_for_date.index.name = 'date'
                                st.write("data_for_date")
                                st.write(data_for_date)
                                print(data_for_date.columns)
                                if data_for_date.empty:
                                    continue
                                if trade_type == 'Close to Close':
                                    st.write("Es close to close")
                                    precio_usar_apertura = 'close'
                                    precio_usar_cierre = 'close'
                                    index = 1
                                    option_price = round(data_for_date['Close'].iloc[0])
                                    
                                elif trade_type == 'Close to Open':
                                    precio_usar_apertura = 'close'
                                    precio_usar_cierre = 'open'
                                    index = 1                   
                                    option_price = round(data_for_date['Close'].iloc[0])
                                    
                                else: #Open to Close
                                    precio_usar_apertura = 'open'
                                    precio_usar_cierre = 'close'
                                    index = 0
                                    option_price = round(data_for_date['Open'].iloc[0]) #Se basa en la apertura del día actual
                                    #st.write(option_price)
                                #option_date = encontrar_opcion_cercana(client, date, option_price, row[column_name], option_days, option_offset, ticker)
                                option_date, actual_option_price = encontrar_strike_cercano(client, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                                option_price = actual_option_price
                                if option_date:
                                    option_type = 'C' if row[column_name] == 1 else 'P'
                                    st.write("option type para posición abierta:")
                                    st.write(option_type)
                                    option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                                df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
                                df_option_anterior = obtener_historico(option_name_anterior, api_key, date, date + timedelta(days=option_days))
                                option_open_price_opnd = df_option_anterior[precio_usar_apertura_anterior].iloc[0]
                                option_close_price_opnd = df_option_anterior[precio_usar_cierre_anterior].iloc[0]
                                st.write("Precio de entrada posición abierta siguiente día:")
                                st.write(option_open_price_opnd)
                                st.write("Precio de entrada posición abierta siguiente día:")
                                st.write(option_close_price_opnd)
                                
                                
                                    
                                
                                if señal_actual == señal_anterior: #Tenemos posibilidad de recuperar ganancia
                                    st.write("Señales iguales")
                                    st.write("Manteniendo señal hasta el final del día...")
                                    #st.write("Fecha día anterior")
                                    #st.write(fecha_entrada)
                                    #st.write("trade result día anterior")
                                    #st.write(trade_result_anterior)
                                    #st.write("option name día anterior")
                                    #st.write(option_name_anterior)
                                    
                                    #data_for_date_anterior = yf.download(ticker, start=fecha_inicio, end=fecha_fin + pd.DateOffset(days=1)) fecha_entrada
                                    data_for_date_anterior = yf.download("SPY", start="2022-01-01", end=fecha_entrada + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                                    data_for_date_anterior = data_for_date_anterior.drop(data_for_date_anterior.index[-1])
                                    data_for_date_anterior.columns = data_for_date_anterior.columns.str.lower()
                                    data_for_date_anterior.index.name = 'date'
                                    print(data_for_date_anterior.columns)
                                    if not data_for_date_anterior.empty:
                                        etf_open_price_anterior = data_for_date_anterior['Open'].iloc[0] if not data_for_date_anterior.empty else None
                                        etf_close_price_anterior = data_for_date_anterior['Close'].iloc[0] if not data_for_date_anterior.empty else None
                                        st.write("Precio del open de ayer")
                                        st.write(etf_open_price_anterior)
                                        st.write("Precio del close de ayer")
                                        st.write(etf_close_price_anterior)
                                    
                                    if not data_for_date.empty:
                                        etf_close_price = data_for_date['Close'].iloc[0] if not data_for_date.empty else None
                                        etf_open_price = data_for_date['Open'].iloc[0] if not data_for_date.empty else None
                                        st.write("Precio del open de hoy")
                                        st.write(etf_open_price)
                                        st.write("Precio del close de hoy")
                                        st.write(etf_close_price)
                                        
                                        
                                    if not df_option.empty:
                                        option_close_price = df_option[precio_usar_cierre].iloc[index]
                                    trade_result_anterior = (option_close_price_opnd - precio_entrada_anterior) * 100 * num_contratos_anterior
                                    st.write("Nuevo trade result anterior calculado:")
                                    st.write(trade_result_anterior)
                                    
                                    
                                    
                                    #df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
                                    #trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                                    
                                    balance += trade_result_anterior
                                    
                                    resultados.append({
                                        'Fecha': date, 
                                        'Tipo': 'Call' if señal_actual == 1 else 'Put',
                                        'toggle_false': row[column_name],
                                        'toggle_true': row[column_name],
                                        'Fecha Apertura': fecha_entrada,
                                        'Fecha Cierre': date,
                                        'Precio Entrada': precio_entrada_anterior, 
                                        'Precio Salida': precio_salida_anterior, 
                                        'Precio Salida Utilizado': option_close_price_opnd,
                                        'Resultado': trade_result_anterior,
                                        'Contratos': num_contratos_anterior,
                                        'Opcion': option_name_anterior,
                                        'Open': etf_open_price_anterior,
                                        'Close': etf_close_price,
                                        'Open Posición Abierta': etf_open_price_anterior,
                                        'Close Posición Abierta': etf_close_price_anterior
                                    })
                                    
                                    # La posición anterior ya está cerrada
                                    posicion_anterior_abierta = False
                                    tipo_posicion = None
                                    option_name_anterior = None
                                    num_contratos_anterior = 0   
                                    etf_open_price_anterior = 0 
                                    fecha_entrada = None
                                    #trade_result_anterior = 0
                                    
                                else: #señal_actual != señal_anterior  Estaríamos incrementando la pérdida -- Se cierra posición de inmediato--
                                    st.write("Señales no iguales")
                                    st.write("Cerrando posición...")
                                    st.write(date)
                                    #st.write("Fecha día anterior")
                                    #st.write(fecha_entrada)
                                    #st.write("trade result día anterior")
                                    #st.write(trade_result_anterior)
                                    #st.write("option name día anterior")
                                    #st.write(option_name_anterior)
                                    #st.write("Precio usar cierre anterior:")
                                    #st.write(precio_usar_cierre_anterior)
                                    
                                    #data_for_date_anterior = yf.download(ticker, start=fecha_inicio, end=fecha_fin + pd.DateOffset(days=1)) fecha_entrada
                                    data_for_date_anterior = yf.download("SPY", start="2022-01-01", end=fecha_entrada + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                                    data_for_date_anterior = data_for_date_anterior.drop(data_for_date_anterior.index[-1])
                                    data_for_date_anterior.columns = data_for_date_anterior.columns.str.lower()
                                    data_for_date_anterior.index.name = 'date'
                                    
                                    
                                    if not data_for_date_anterior.empty:
                                        etf_open_price_anterior = data_for_date_anterior['Open'].iloc[0] if not data_for_date_anterior.empty else None
                                        etf_close_price_anterior = data_for_date_anterior['Close'].iloc[0] if not data_for_date_anterior.empty else None
                                        st.write("Precio del open de ayer")
                                        st.write(etf_open_price_anterior)
                                        st.write("Precio del close de ayer")
                                        st.write(etf_close_price_anterior)
                                    
                                    if not data_for_date.empty:
                                        etf_open_price = data_for_date['Open'].iloc[0] if not data_for_date.empty else None
                                        etf_close_price = data_for_date['Close'].iloc[0] if not data_for_date.empty else None
                                        st.write("Precio del open de hoy")
                                        st.write(etf_open_price)
                                        st.write("Precio del close de hoy")
                                        st.write(etf_close_price)
                                    
                                    
                                    if not df_option.empty:
                                        option_open_price = df_option[precio_usar_apertura].iloc[0]
                                        option_close_price = df_option[precio_usar_cierre].iloc[index]
                                        
                                    trade_result_anterior = (option_open_price_opnd - precio_entrada_anterior) * 100 * num_contratos_anterior
                                    st.write("Nuevo trade result anterior calculado:")
                                    st.write(trade_result_anterior)
                                    
                                    
                                    balance += trade_result_anterior
                                    
                                    resultados.append({
                                        'Fecha': date, 
                                        'Tipo': 'Call' if señal_actual == 1 else 'Put',
                                        'toggle_false': row[column_name],
                                        'toggle_true': row[column_name],
                                        'Fecha Apertura': fecha_entrada,
                                        'Fecha Cierre': date,
                                        'Precio Entrada': precio_entrada_anterior, 
                                        'Precio Salida': precio_salida_anterior, 
                                        'Precio Salida Utilizado': option_open_price_opnd,
                                        'Resultado': trade_result_anterior,
                                        'Contratos': num_contratos_anterior,
                                        'Opcion': option_name_anterior,
                                        'Open': etf_open_price_anterior,
                                        'Close': etf_open_price,
                                        'Open Posición Abierta': etf_open_price_anterior,
                                        'Close Posición Abierta': etf_close_price_anterior
                                    })
                                    
                                    # La posición anterior ya está cerrada
                                    posicion_anterior_abierta = False
                                    tipo_posicion = None
                                    option_name_anterior = None
                                    num_contratos_anterior = 0
                                    etf_open_price_anterior = 0
                                    trade_result_anterior = 0
                                    fecha_entrada = None
               
                                    
                                option_date = encontrar_opcion_cercana(client, date, option_price, row[column_name], option_days, option_offset, ticker)
                                
                                if not df_option.empty:
                                    posicion_actual_abierta = True
                                    option_open_price = df_option[precio_usar_apertura].iloc[0]
                                    option_close_price = df_option[precio_usar_cierre].iloc[index]
                                    max_contract_value = option_open_price * 100
                                    if allocation_type == 'Porcentaje de asignación':
                                        num_contratos = int((balance * pct_allocation) / max_contract_value)
                                    else: #allocation_type == 'Monto fijo de inversión':
                                        if balance < max_contract_value:
                                            st.error("No hay suficiente dinero para abrir más posiciones. La ejecución del tester ha terminado.")
                                            return pd.DataFrame(resultados), balance
                                        else: #balance >= max_contract_value
                                            num_contratos = int(fixed_amount / max_contract_value)
                                    trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                
                                    if trade_result > 0:
                                        balance += trade_result
                                        # Registrar el resultado de la nueva operación
                                        etf_data = yf.download("SPY", start="2022-01-01", end=date + pd.Timedelta(days=1), multi_level_index=False, auto_adjust=False)
                                        etf_data = etf_data.drop(etf_data.index[-1])
                                        etf_data.columns = etf_data.columns.str.lower()
                                        etf_data.index.name = 'date'
                                        print(etf_data.columns)
                                        etf_open_price = etf_data['Open'].iloc[0] if not etf_data.empty else None
                                        etf_close_price = etf_data['Close'].iloc[0] if not etf_data.empty else None
                            
                                        resultados.append({
                                            'Fecha': date, 
                                            'Tipo': 'Call' if señal_actual == 1 else 'Put',
                                            'toggle_false': row[column_name],
                                            'toggle_true': row[column_name],
                                            #'Fecha Apertura': df_option.index[0],
                                            #'Fecha Cierre': df_option.index[index],
                                            'Fecha Apertura': date,
                                            'Fecha Cierre': date,
                                            'Precio Entrada': option_open_price, 
                                            'Precio Salida': df_option[precio_usar_cierre].iloc[index],
                                            'Precio Salida Utilizado': df_option[precio_usar_cierre].iloc[index],
                                            'Resultado': trade_result,
                                            'Contratos': num_contratos,
                                            'Opcion': option_name,
                                            'Open': etf_open_price,
                                            'Close': etf_close_price,
                                            'Open Posición Abierta': etf_open_price,
                                            'Close Posición Abierta': etf_close_price
                                        })
                                        posicion_actual_abierta = False
                                    else:  # Si la operación no es rentable, dejamos la posición abierta
                                        #Dejamos la posición anterior abierta
                                        posicion_anterior_abierta = True
                                        tipo_posicion = 'Call' if señal_actual == 1 else 'Put'
                                        num_contratos_anterior = num_contratos
                                        option_name_anterior = option_name
                                        precio_entrada_anterior = option_open_price
                                        precio_salida_anterior = option_close_price
                                        trade_result_anterior = trade_result
                                        fecha_entrada = date
                                        #precio_usar_cierre_anterior = option_close_price
                                        precio_usar_cierre_anterior = precio_usar_cierre
                                        precio_usar_apertura_anterior = precio_usar_apertura
                                        #st.write(fecha_entrada)
                                        #st.write(precio_entrada_anterior)
                                        #st.write(num_contratos_anterior)
                                        #st.write(trade_result_anterior)
                                        
                                        #st.write(option_name_anterior)
                                        # No registramos el resultado aún
                                        # Guardamos la señal actual para la siguiente iteración
                                        señal_anterior = señal_actual
                                        
                            else: #posicion_anterior_abierta = False
                                st.write("No hay posiciones abiertas para la fecha de:")
                                st.write(date)
                                #Abrimos una nueva posición
                                data_for_date = yf.download("SPY", start="2022-01-01", end=date + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                                data_for_date = data_for_date.drop(data_for_date.index[-1])
                                data_for_date.columns = data_for_date.columns.str.lower()
                                data_for_date.index.name = 'date'
                                print(data_for_date.columns)
                                
                                if data_for_date.empty:
                                    continue
                                if trade_type == 'Close to Close':
                                    precio_usar_apertura = 'close'
                                    precio_usar_cierre = 'close'
                                    index = 1
                                    option_price = round(data_for_date['Close'].iloc[0])
                                    
                                elif trade_type == 'Close to Open':
                                    precio_usar_apertura = 'close'
                                    precio_usar_cierre = 'open'
                                    index = 1                   
                                    option_price = round(data_for_date['Close'].iloc[0])
                                    
                                else: #Open to Close
                                    precio_usar_apertura = 'open'
                                    precio_usar_cierre = 'close'
                                    index = 0
                                    option_price = round(data_for_date['Open'].iloc[0]) #Se basa en la apertura del día actual
                                    #st.write(option_price)
                                #option_date = encontrar_opcion_cercana(client, date, option_price, row[column_name], option_days, option_offset, ticker)
                                option_date, actual_option_price = encontrar_strike_cercano(client, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                                option_price = actual_option_price
                                if option_date:
                                    option_type = 'C' if row[column_name] == 1 else 'P'
                                    option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                                    df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
                                    if not df_option.empty:
                                        posicion_actual_abierta = True
                                        option_open_price = df_option[precio_usar_apertura].iloc[0]
                                        st.write("Precio de entrada para la opción día actual:")
                                        st.write(option_open_price)
                                        option_close_price = df_option[precio_usar_cierre].iloc[index]
                                        st.write("Precio de salida opción día actual:")
                                        st.write(option_close_price)
                                        max_contract_value = option_open_price * 100
                                        
                                        if allocation_type == 'Porcentaje de asignación':
                                            num_contratos = int((balance * pct_allocation) / max_contract_value)
                                        else: #allocation_type == 'Monto fijo de inversión':
                                            if balance < max_contract_value:
                                                st.error("No hay suficiente dinero para abrir más posiciones. La ejecución del tester ha terminado.")
                                                return pd.DataFrame(resultados), balance
                                            else: #balance >= max_contract_value
                                                num_contratos = int(fixed_amount / max_contract_value)
                                        
                                        st.write("Numero de contratos día actual:")
                                        st.write(num_contratos)
                                        st.write("Option Type actual:")
                                        st.write(option_type)
                                        trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                                        if trade_result >= 0:
                                            balance += trade_result
                                            st.write("trade result actual positivo:")
                                            st.write(trade_result)
                                            # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                                            etf_data = yf.download("SPY", start="2022-01-01", end=date + pd.Timedelta(days=1), multi_level_index=False, auto_adjust=False)
                                            etf_data = etf_data.drop(etf_data.index[-1])
                                            etf_data.columns = etf_data.columns.str.lower()
                                            etf_data.index.name = 'date'
                                            etf_open_price = etf_data['Open'].iloc[0] if not etf_data.empty else None
                                            st.write("Precio de entrada día actual:")
                                            st.write(etf_open_price)
                                            etf_close_price = etf_data['Close'].iloc[0] if not etf_data.empty else None
                                            st.write("Precio salida día actual:")
                                            st.write(etf_close_price)
                                            
                                            
                                            resultados.append({
                                                'Fecha': date, 
                                                'Tipo': 'Call' if row[column_name] == 1 else 'Put',
                                                #'Pred': row[column_name],
                                                'toggle_false': row[column_name],
                                                'toggle_true': row[column_name],
                                                'Fecha Apertura': date,
                                                'Fecha Cierre': date,
                                                #'Fecha Apertura': df_option.index[0],
                                                #'Fecha Cierre': df_option.index[index],
                                                'Precio Entrada': option_open_price, 
                                                'Precio Salida': df_option[precio_usar_cierre].iloc[index],
                                                'Precio Salida Utilizado': df_option[precio_usar_cierre].iloc[index],
                                                'Resultado': trade_result,
                                                'Contratos': num_contratos,
                                                'Opcion': option_name,
                                                'Open': etf_open_price,
                                                'Close': etf_close_price,
                                                'Open Posición Abierta': etf_open_price,
                                                'Close Posición Abierta': etf_close_price
                                            })
                                            posicion_actual_abierta = False
                                            print(trade_result)
                                            
                                        else: #trade_result < 0
                                            #Dejamos la posición anterior abierta
                                            posicion_anterior_abierta = True
                                            tipo_posicion = 'Call' if señal_actual == 1 else 'Put'
                                            num_contratos_anterior = num_contratos
                                            option_name_anterior = option_name
                                            precio_entrada_anterior = option_open_price
                                            precio_salida_anterior = option_close_price
                                            trade_result_anterior = trade_result
                                            st.write("trade result negativo que se convertirá en mi anterior:")
                                            st.write(trade_result_anterior)
                                            precio_usar_cierre_anterior = precio_usar_cierre
                                            precio_usar_apertura_anterior = precio_usar_apertura
                                            #option_open_price_opnd = option_open_price
                                            fecha_entrada = date
                                            #st.write(fecha_entrada)
                                            #st.write(precio_entrada_anterior)
                                            #st.write(num_contratos_anterior)
                                            #st.write(trade_result_anterior)
                                            #st.write(option_name_anterior)
                                            #st.write(precio_usar_cierre_anterior)
                                            # No registramos el resultado aún
                                            # Guardamos la señal actual para la siguiente iteración
                                            señal_anterior = señal_actual
                        else:
                            continue
                        
                    else: #esce1 = False  
                        data_for_date = yf.download("SPY", start=date, end=date + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                        #st.write("datos con data_for_date (yahoo finance)")
                        #st.write(data_for_date)
                        #data_for_date2 = get_open_and_close(ticker, api_av, fecha_inicio, fecha_fin)
                        #st.write("datos con get_open_and_close (alpha vantage")
                        #st.write(data_for_date2)
                        #df_option = obtener_historico_15min(option_name, api_key, date, date + timedelta(days=option_days))
                        #st.write("datos con obtener_historico_15min")
                        #st.write(df_option)
                        #data_for_date_fm = get_spy_intraday_financial_modeling(fecha_inicio, fecha_fin)
                        #st.write("datos con get_spy_intraday_financial_modeling")
                        #st.write(data_for_date_fm)
                        #data_for_date3 = open_close(ticker, api_key, fecha_inicio, fecha_fin)
                        
                        #data_for_date = data_for_date.drop(data_for_date.index[-1])
                        #data_for_date.columns = data_for_date.columns.str.lower()
                        data_for_date.index.name = 'date'
                        #data_for_date3.index.name = 'date'
                        #datee = pd.to_datetime(date)
                        #datee = pd.to_datetime(datee)
                        # Reemplazar la hora, minuto y segundo
                        #datee = datee.replace(hour=9, minute=35, second=0)
                        #st.write("datee")
                        #st.write(datee)
                        #st.write("date.index")
                        #st.write(data_for_date3.index[64])
                        #data_for_date3 = data_for_date3[data_for_date3.index >= datee]
                        #st.write("datos con data_for_date3")
                        #st.write(data_for_date3)
                        #st.write("datos eliminando ultimo index")
                        #st.write(data_for_date)
                        #print(data_for_date.columns)
                        if data_for_date.empty:
                            continue
                        #if data_for_date3.empty:
                            #continue
                        if trade_type == 'Close to Close':
                            #st.write("Es close to close")
                            precio_usar_apertura = 'close'
                            precio_usar_cierre = 'close'
                            index = 1
                            option_price = round(data_for_date['Close'].iloc[0])
                            #st.write("option price 1:")
                            #st.write(option_price)
                            #option_price_5min = round(data_for_date3['close'].iloc[0])
                            
                        elif trade_type == 'Close to Open':
                            precio_usar_apertura = 'close'
                            precio_usar_cierre = 'open'
                            index = 1                   
                            option_price = round(data_for_date['Close'].iloc[0])
                            #option_price_5min = round(data_for_date3['close'].iloc[0])
                            
                            
                        else: #Open to Close
                            precio_usar_apertura = 'open'
                            precio_usar_cierre = 'close'
                            index = 0
                            option_price = round(data_for_date['Open'].iloc[0]) #Se basa en la apertura del día actual
                            #option_price_5min = round(data_for_date3['open'].iloc[0])
                            #st.write(option_price)
                        #option_date = encontrar_opcion_cercana(client, date, option_price, row[column_name], option_days, option_offset, ticker)
                        option_date, actual_option_price = encontrar_strike_cercano(client, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                        option_price = actual_option_price
                        #st.write("Option_date:")
                        #st.write(option_date)
                        #st.write("option price 2:")
                        #st.write(option_price)
                        if option_date:
                            option_type = 'C' if row[column_name] == 1 else 'P'
                            option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                            df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
                            #st.write("df_option:")
                            #st.write(df_option)
                            if not df_option.empty:
                                option_open_price = df_option[precio_usar_apertura].iloc[0]
                                option_close_price = df_option[precio_usar_cierre].iloc[index]
                                #st.write("option open price:")
                                #st.write(option_open_price)
                                #st.write("option close price:")
                                #st.write(option_close_price)
                                max_contract_value = option_open_price * 100
                                #st.write("max_contract_value")
                                #st.write(max_contract_value)
                                if allocation_type == 'Porcentaje de asignación':
                                    num_contratos = int((balance * pct_allocation) / max_contract_value)
                                    #st.write("Número de contratos:")
                                    #st.write(num_contratos)
                                else: #allocation_type == 'Monto fijo de inversión':
                                    if balance < max_contract_value:
                                        st.error("No hay suficiente dinero para abrir más posiciones. La ejecución del tester ha terminado.")
                                        return pd.DataFrame(resultados), balance
                                    else: #balance >= max_contract_value
                                        num_contratos = int(fixed_amount / max_contract_value)
                                trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                                #st.write("trade result:")
                                #st.write(trade_result)
                                balance += trade_result
                                #st.write("Balance:")
                                #st.write(balance)
                                
                                # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                                etf_data = yf.download("SPY", start=date, end=date + pd.Timedelta(days=1), multi_level_index=False, auto_adjust=False)
                                #st.write("datos sin eliminar ultimo index-etf")
                                #st.write(etf_data)
                                #etf_data = etf_data.drop(etf_data.index[-1])
                                #etf_data.columns = etf_data.columns.str.lower()
                                etf_data.index.name = 'date'
                                #st.write("datos eliminando ultimo index")
                                #st.write(data_for_date)
                                etf_open_price = etf_data['Open'].iloc[0] if not etf_data.empty else None
                                etf_close_price = etf_data['Close'].iloc[0] if not etf_data.empty else None
                                
                                resultados.append({
                                    'Fecha': date, 
                                    'Tipo': 'Call' if row[column_name] == 1 else 'Put',
                                    #'Pred': row[column_name],
                                    'toggle_false': row[column_name],
                                    'toggle_true': row[column_name],
                                    'Fecha Apertura': df_option.index[0],
                                    'Fecha Cierre': df_option.index[index],
                                    'Precio Entrada': option_open_price, 
                                    'Precio Salida': df_option[precio_usar_cierre].iloc[index], 
                                    'Resultado': trade_result,
                                    'Contratos': num_contratos,
                                    'Opcion': option_name,
                                    #'Open': df_option[['open']]
                                    'Open': etf_open_price,
                                    'Close': etf_close_price,
                                    #'Open2': etf_open_price3,
                                    #'Close2': etf_close_price3
                                })
                                print(trade_result)
                    
            else: #periodo == '15 minutos'
                data_for_date = yf.download("SPY", start="2022-01-01", end=date + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                data_for_date = data_for_date.drop(data_for_date.index[-1])
                data_for_date.columns = data_for_date.columns.str.lower()
                data_for_date.index.name = 'date'
                print(data_for_date.columns)
                #st.write("Fecha date:",date)
                #st.write("Fecha inicio:",fecha_inicio)
                #st.write("Fecha fin:",fecha_fin)
                data_for_date2 = get_open_and_close(ticker, api_av, fecha_inicio, fecha_fin)
                data_for_date3 = open_close(ticker, api_key, fecha_inicio, fecha_fin)
                data_for_date4 = mostrar_datos()
                data_for_date_fm = get_spy_intraday_financial_modeling(fecha_inicio, fecha_fin)
                #st.write(start)
                #st.write(data_for_date)
                #st.write ("dataframe fm")
                #st.write(data_for_date_fm)
                #st.write ("función open_close (Polygon)")
                #st.write(data_for_date3)
                #st.write ("función mostrar datos globales")
                #st.write(data_for_date4)
                if data_for_date.empty:
                    continue
                if data_for_date2.empty:
                    continue
    
                if trade_type == 'Close to Close':
                    precio_usar_apertura = 'close'
                    precio_usar_cierre = 'close'
                    index = 1
                    #option_price = round(data_for_date2.loc[date]['close'])
                    option_price2= round(data_for_date2.loc[date]['open'])
                    option_price= round(data_for_date4.loc[date]['open'])
                elif trade_type == 'Close to Open':
                    precio_usar_apertura = 'close'
                    precio_usar_cierre = 'open'
                    index = 1               
                    #option_price = round(data_for_date2.loc[date]['close'])
                    option_price2= round(data_for_date2.loc[date]['open'])
                    option_price= round(data_for_date4.loc[date]['open'])
                else: #Open to Close
                    precio_usar_apertura = 'open'
                    precio_usar_cierre = 'close'
                    index = 0                
                    #option_price2 = round(data_for_date['Open'].iloc[0])
                    option_price2= round(data_for_date2.loc[date]['open'])
                    option_price= round(data_for_date4.loc[date]['open'])
                    #st.write(option_price)
            
                #option_date = encontrar_opcion_cercana_15min(client, date, option_price, row[column_name], option_days, option_offset, ticker)
                option_date, actual_option_price = encontrar_strike_cercano(client, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                option_price = actual_option_price
                if option_date:
                    option_type = 'C' if row[column_name] == 1 else 'P'
                    option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                    #option_name2 = f'O:{ticker}{option_date2}{option_type}00{option_price2}000'
                    #st.write(option_name)
                    #st.write("option date2 - Diario")
                    #st.write(option_date2)
                    #st.write("option date - 15 min")
                    #st.write(option_date)
                #st.write(date)
                #st.write(timedelta(days=option_days))
                #st.write(date + timedelta(days=option_days))
                df_option = obtener_historico_15min(option_name, api_key, date, date + timedelta(days=option_days))
                #df_option2 = obtener_historico_15min_pol(option_name, api_key, date, date + timedelta(days=option_days))
                df = get_open_and_close(ticker, api_av, fecha_inicio, fecha_fin)
                df_glo = mostrar_datos()
                #df_option2 = obtener_historico_15min_pol(ticker, api_key, date, date + timedelta(days=option_days))
                #vo = verificar_opcion_15min(client, ticker, date, date + timedelta(days=option_days))
                #vo = verificar_opcion_15min(client, ticker, fecha_inicio, fecha_fin)
                #df_option2 = obtener_historico_15min_pol(option_name, api_key, date, date + timedelta(days=option_days))
                #df2 = obtener_historico_15min_pol(option_name, api_key, date, date + timedelta(days=option_days))
                #st.write("df_option:")
                #st.dataframe(df_option)
                #st.write("función get_open_and_close:")
                #st.dataframe(df)
                #st.write("df_option2:")
                #st.dataframe(df_option2)
                #st.write("verificar opción:")
                #st.write(vo)
                #st.write("Respuesta JSON completa:", data)  # También se muestra en Streamlit
                if not df_option.empty:   
                    #st.write("Entró por acá")
                    option_open_price = df_option['open'].iloc[0]
                    #st.write(open_hour)
                    #st.write(close_hour)
                    #st.write(option_open_price)
                    #st.write(df_option[precio_usar_cierre].iloc[index])
                    #st.write(df_option.iloc[0])
                    #st.write(df_option.iloc[-1])
                    #st.write(df_option)
                    
                    #st.write(df_option[precio_usar_cierre].iloc[index])
                    option_close_price = df_option['close'].iloc[-1]  # Último cierre del día
                    #option_open_price = df.at[date, 'open']
                    #option_close_price = df.at[date, 'close']
    
                
                #df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))    
                
                if not df_option.empty:
                    #option_open_price = df_option[precio_usar_apertura].iloc[0]
                    max_contract_value = option_open_price * 100
                    if allocation_type == 'Porcentaje de asignación':
                        num_contratos = int((balance * pct_allocation) / max_contract_value)
                    else: #allocation_type == 'Monto fijo de inversión':
                        if balance < max_contract_value:
                            st.error("No hay suficiente dinero para abrir más posiciones. La ejecución del tester ha terminado.")
                            return pd.DataFrame(resultados), balance
                        else: #balance >= max_contract_value
                            num_contratos = int(fixed_amount / max_contract_value)
                    trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                    balance += trade_result
    
                    # Obtener el símbolo del ETF del índice (por ejemplo, 'SPY' para el índice S&P 500)
                    #etf_symbol = 'SPY'  # Reemplaza 'SPY' con el símbolo correcto de tu ETF de índice
                    
                    # Usar la nueva función de Alpha Vantage para obtener los datos del ETF
                    #etf_open_price, etf_close_price = get_alpha_vantage_data(ticker, date)
           
                    # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                    etf_data = yf.download("SPY", start="2022-01-01", end=date + pd.Timedelta(days=1), multi_level_index=False, auto_adjust=False)
                    etf_data = etf_data.drop(etf_data.index[-1])
                    etf_data.columns = etf_data.columns.str.lower()
                    etf_data.index.name = 'date'
                    etf_open_price = etf_data['Open'].iloc[0] if not etf_data.empty else None
                    etf_close_price = etf_data['Close'].iloc[0] if not etf_data.empty else None
                    if periodo == '15 minutos':
                        etf_open_price = df.at[date, 'open']
                        etf_close_price = df.at[date, 'close']
                        if not data_for_date_fm.empty:
                        #if not df_option2.empty:
                            #etf_open_price = df_option2.at[date, 'open']
                            #etf_close_price = df_option2.at[date, 'close']
                            etf_open_price= data_for_date_fm.at[date, 'open']
                            etf_close_price= data_for_date_fm.at[date, 'close']
                            etf_open_price2= data_for_date2.at[date, 'open']
                            etf_close_price2= data_for_date2.at[date, 'close']
                            etf_open_price3= data_for_date4.at[date, 'open']
                            etf_close_price3= data_for_date4.at[date, 'close']
                        else:
                            etf_open_price = df.at[date, 'open']
                            etf_close_price = df.at[date, 'close']
                        #etf_open_price = df_option2.at[date, 'open']
                        #st.write(df_option2.at[date, 'open'])
                        #st.write(df.at[date, 'open'])
    
                    resultados.append({
                        'Fecha': date, 
                        'Tipo': 'Call' if row[column_name] == 1 else 'Put',
                        #'Pred': row[column_name],
                        'toggle_false': row[column_name],
                        'toggle_true': row[column_name],
                        'Fecha Apertura': df_option.index[0],
                        'Fecha Cierre': df_option.index[index],
                        'Precio Entrada': option_open_price, 
                        'Precio Salida': df_option[precio_usar_cierre].iloc[index], 
                        'Resultado': trade_result,
                        'Contratos': num_contratos,
                        'Opcion': option_name,
                        #'Open': df_option[['open']]
                        'Open': etf_open_price,
                        'Close': etf_close_price,
                        #'Open2': etf_open_price3,
                        #'Close2': etf_close_price3
                    })
                    print(trade_result)

    resultados_df = pd.DataFrame(resultados)
    if not resultados_df.empty and 'Resultado' in resultados_df.columns:
        graficar_resultados(resultados_df, balance, balance_inicial)
        resultados_df.to_excel('resultados_trades_1.xlsx')
    else:
        st.error("No se encontraron resultados válidos para el periodo especificado.")
    return resultados_df, balance

def graficar_resultados(df, final_balance, balance_inicial):
    if df.empty or 'Resultado' not in df.columns:
        st.error("No se pueden graficar resultados porque el DataFrame está vacío o falta la columna 'Resultado'.")
        return
    
    plt.figure(figsize=(14, 7))
    df['Ganancia acumulada'] = df['Resultado'].cumsum() + balance_inicial
    ax = df.set_index('Fecha')['Ganancia acumulada'].plot(kind='line', marker='o', linestyle='-', color='b')
    ax.set_title(f'Resultados del Backtesting de Opciones - Balance final: ${final_balance:,.2f}')
    ax.set_xlabel('Fecha')
    ax.set_ylabel('Ganancia/Pérdida Acumulada')
    plt.xticks(rotation=45)
    
    # Ajuste para mostrar correctamente fechas y horas en el eje x
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))  # Coloca marcas de horas en el eje x
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)


    ax.axhline(y=balance_inicial, color='r', linestyle='-', label='Balance Inicial')
    
    # Crear un segundo eje Y (eje derecho) para el precio de cierre
    ax2 = ax.twinx()
    ax2.set_ylim(300, 700)  # Configurar límites del eje Y derecho
    ax2.plot(df['Fecha'], df['Close'], color='orange', linestyle='-', label='Precio del S&P (Close)')
    ax2.set_ylabel('Precio del S&P (Close)', color='black')
    ax2.tick_params(axis='y', labelcolor='black')
    
    #plt.legend()
    # Leyendas de ambos ejes
    ax.legend(loc='upper left')
    ax2.legend(loc='upper right')
    plt.grid(True, which='both', linestyle='-', linewidth=0.5)
    plt.tight_layout()
    plt.savefig('resultados_backtesting.png')
    plt.show()

def main():

    st.title("Backtesting ARKAD")

    
    tooltip_style = """
    <style>
    .tooltip {
        position: relative;
        display: inline-block;
        cursor: pointer;
        color: #3498db;
        float: left:
        margin-left: 5px;
        vertical_align: middle;
    }

    .tooltip .tooltiptext {
        visibility: hidden;
        width: 220px;
        background-color: black;
        color: #fff;
        text-align: left;  /Alineación de texto/
        border-radius: 6px;
        padding: 10px;
        position: absolute;
        z-index: 1;
        top: -5px;
        right: 105%;
        opacity: 0;
        transition: opacity 0.3s;
    }

    .tooltip:hover .tooltiptext {
        visibility: visible;
        opacity: 1;
    }
    </style>
    """

    # Insertamos el estilo en la aplicación Streamlit
    st.markdown(tooltip_style, unsafe_allow_html=True)

    
    # Directorio donde se encuentran los archivos .xlsx
    directorio_datos = '.'
    archivos_disponibles = [archivo for archivo in os.listdir(directorio_datos) if archivo.endswith('.xlsx')]
    
    #Extraer información del nombre del archivo seleccionado
    def extract_file_info(filename):
        #Valores por defecto
        default_values = ("Operación desconocida", "Información desconocida", "Responsable desconocido", 
                      "Fecha desconocida", "Fecha desconocida", "Versión desconocida")
        parts = filename.split('_')
        if len(parts) < 6:  # Verifica que haya suficientes partes en el nombre del archivo
            return default_values
    
        try:
            operation = {'CC': 'Close to Close', 'OC': 'Open to Close', 'CO': 'Close to Open'}.get(parts[0], 'Operación desconocida')
            info ={'Proba': 'Probabilidades', 'Pred': 'Predicciones'}.get(parts[1], 'Información desconocida')
            responsible = {'Valen': 'Valentina', 'Santi': 'Santiago', 'Andres': 'Andrés'}.get(parts[2], 'Responsable desconocido')
            start_date = parts[3][2:4] + '/' + parts[3][4:6] #+ '/20' + parts[2][0:2]
            end_date = parts[4][2:4] + '/' + parts[4][4:6] #+ '/20' + parts[3][0:2]
            version = parts[5].split('.')[0]
        
            return operation, info, responsible, start_date, end_date, version
        except IndexError:
            return default_values
        

    #placeholder para el ícono de información
    info_placeholder = st.empty()
    
    #Toogle
    toggle_activated = st.toggle("Se opera si se supera el Threshold")
    column_name = 'toggle_true' if toggle_activated else 'toggle_false'
    
    # Opción de selección del archivo .xlsx
    data_filepath = st.selectbox("*Seleccionar archivo de datos históricos:*", archivos_disponibles)
    
    if data_filepath:
       operation, info, responsible, start_date, end_date, version = extract_file_info(data_filepath)
       data = cargar_datos(data_filepath)
       
       #if data['threshold'] is not None:
           #st.write(f"*Threshold óptimo: {data['threshold']}*")
       #else:
           #st.write("*Threshold óptimo:* No se pudo encontrar el valor del threshold en el archivo.")
       # Actualizar el tooltip
       if operation.startswith("Información desconocida"):
           tooltip_text = f"<div class='tooltip'>&#9432; <span class='tooltiptext'>{operation}</span></div>"
       else:
           tooltip_text = f"""
           <div class="tooltip">
                &#9432;  <!-- Ícono de información -->
                <span class="tooltiptext">
                Tipo de operación: {operation}<br>
                {info}<br>
                Responsable del algoritmo: {responsible}<br>
                Rango de fechas: {start_date}<br>
                {end_date}<br>
                Versión: {version}
                </span>
           </div>
            """
       info_placeholder.markdown(tooltip_text, unsafe_allow_html=True)
        
    # Option Days input
    option_days_input = st.number_input("*Option Days:* (Número de días de vencimiento de la opción que se está buscando durante el backtesting)", min_value=0, max_value=90, value=30, step=1)
    
    # Option Offset input
    option_offset_input = st.number_input("*Option Offset:* (Rango de días de margen alrededor del número de días objetivo dentro del cual se buscará la opción más cercana)", min_value=0, max_value=90, value=7, step=1)
    
    # Additional inputs for the backtest function
    balance_inicial = st.number_input("*Balance inicial*", min_value=0, value=100000, step= 1000)
    # Agregar opción para elegir el tipo de asignación
    allocation_type = st.radio("Seleccionar tipo de asignación de capital:", ('Porcentaje de asignación', 'Monto fijo de inversión'))
    if allocation_type == 'Porcentaje de asignación':
        pct_allocation = st.number_input("*Porcentaje de Asignación de Capital:*", min_value=0.001, max_value=0.6, value=0.05)
        fixed_amount = None
    else:
        fixed_amount = st.number_input("*Monto fijo de inversión:*", min_value=0.0, max_value=float(balance_inicial), value=1000.0, step=1000.0)
        pct_allocation = None
        
    periodo = st.radio("*Seleccionar periodo de datos*", ('Diario','15 minutos'))
    
    # Checkbox "Escenario 1" con ícono de información y texto condicional
    if periodo == 'Diario':
        # Checkbox con tooltip usando el diseño flex
        col1, col2 = st.columns([1, 1])
        with col1:
            esce1 = st.checkbox("Aplicar estrategia para manejo de pérdida de ganancias")
        with col2:
            st.markdown("""
            <div class="tooltip" style="display: inline;">
                &#9432;
                <span class="tooltiptext">1. Si la opción es rentable, se cierra normalmente al final del día.
                                         2. Si hay pérdidas, mantenemos la posición y al día siguiente: 
                                             a. Si la señal sigue igual, dejamos la posición abierta hasta el final del día. 
                                             b. Si la señal cambia, cerramos la posición inmediatamente.</span>
            </div>
            """, unsafe_allow_html=True)
    else:
        esce1 = False
        
    fecha_inicio = st.date_input("*Fecha de inicio del periodo de backtest:*", min_value=datetime(2020, 1, 1))
    fecha_fin = st.date_input("*Fecha de finalización del periodo de backtest:*", max_value=datetime.today())
    #if periodo == '15 minutos':
        #open_hour = st.time_input("*Seleccionar Hora de Apertura:*", value=datetime.strptime("09:30", "%H:%M").time())
        #close_hour = st.time_input("*Seleccionar Hora de Cierre:*", value=datetime.strptime("16:00", "%H:%M").time())
        
    method = st.radio("*Seleccionar Strikes a Considerar*", ('ATM','OTM'))
    
    if method == "OTM":   
        offset = st.number_input("*Seleccionar cantidad de strikes a desplazarse*", min_value=0, value=5, step=1)
    else:
        offset = 0
    
    trade_type = st.radio('*Tipo de Operación*', ('Open to Close', 'Close to Close', 'Close to Open'))
    
    
        
    if st.button("Run Backtest"):
        resultados_df, final_balance = realizar_backtest(data_filepath, 'rlD0rjy9q_pT4Pv2UBzYlXl6SY5Wj7UT', "SPY", balance_inicial, pct_allocation, fixed_amount, 
        allocation_type, pd.Timestamp(fecha_inicio), pd.Timestamp(fecha_fin), option_days_input, option_offset_input, trade_type, periodo, column_name, method, offset, esce1)
        st.success("Backtest ejecutado correctamente!")

        # Guardar resultados en el estado de la sesión
        st.session_state['resultados_df'] = resultados_df
        st.session_state['final_balance'] = final_balance
        st.session_state['balance_inicial'] = balance_inicial
        
        
        # Provide download links for the generated files
        st.write("### Descargar Resultados")
        
        # Resultados DataFrame to Excel
        excel_buffer = io.BytesIO()
        resultados_df.to_excel(excel_buffer, index=False)
        st.download_button(label="Descargar Resultados Excel", data=excel_buffer, file_name="resultados_trades_1.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        
        # Display and download the plot
        st.write("### Gráfico")
        fig, ax = plt.subplots(figsize=(14, 7))
        resultados_df['Ganancia acumulada'] = resultados_df['Resultado'].cumsum() + balance_inicial
        ax = resultados_df.set_index('Fecha')['Ganancia acumulada'].plot(kind='line', marker='o', linestyle='-', color='b', ax=ax)
        ax.set_title(f'Resultados del Backtesting de Opciones - Balance final: ${final_balance:,.2f}')
        ax.set_xlabel('Fecha')
        ax.set_ylabel('Ganancia/Pérdida Acumulada')
        plt.xticks(rotation=45)
        ax.axhline(y=balance_inicial, color='r', linestyle='-', label='Balance Inicial')
        
        # Crear un segundo eje Y (eje derecho) para el precio de cierre
        ax2 = ax.twinx()
        ax2.set_ylim(300, 700)  # Configurar límites del eje Y derecho
        ax2.plot(resultados_df['Fecha'], resultados_df['Close'], color='orange', linestyle='-', label='Precio del S&P (Close)')
        ax2.set_ylabel('Precio del S&P (Close)', color='black')
        ax2.tick_params(axis='y', labelcolor='black')
        
        #plt.legend()
        # Leyendas de ambos ejes
        ax.legend(loc='upper left')
        ax2.legend(loc='upper right')
        plt.grid(True, which='both', linestyle='-', linewidth=0.5)
        plt.tight_layout()
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png')
        st.image(img_buffer)
        st.download_button(label="Descargar Gráfico", data=img_buffer, file_name="resultados_backtesting.png", mime="image/png")

        
            
        datos = pd.read_excel(r"resultados_trades_1.xlsx")
        datos = datos[(datos['Fecha'] >= pd.Timestamp(fecha_inicio)) & (datos['Fecha'] <= pd.Timestamp(fecha_fin))]
        if trade_type == 'Close to Close':
            datos['Direction'] = (datos['Close'].shift(-1) > datos['Close']).astype(int)
        elif trade_type == 'Close to Open':
            datos['Direction'] = (datos['Close'] < datos['Open'].shift(-1)).astype(int)
        elif trade_type == 'Open to Close':
            datos['Direction'] = (datos['Open'] < datos['Close']).astype(int)
        else:
            datos['Direction'] = 0

            
        
            
        datos = datos.reset_index(drop=True)
        datos['acierto'] = np.where(
            datos['Direction'] == datos[column_name], 1, 0)
        # desempeño de modelo en entrenamiento
        datos['asertividad'] = datos['acierto'].sum()/len(datos['acierto'])
        datos['cumsum'] = datos['acierto'].cumsum()
        # desempeño portafolio acumulado importante si definimos un inicio
        datos['accu'] = datos['cumsum']/(datos.index + 1)
        
        # Muestra el DataFrame actualizado
        if trade_type == 'Open to Close':
            datos['open_to_close_pct'] = datos['Close']/datos['Open'] - 1
    
            # Calcula la ganancia
            datos['Ganancia'] = datos.apply(lambda row: abs(
                row['open_to_close_pct']) if row['acierto'] else -abs(row['open_to_close_pct']), axis=1)
            
        elif trade_type == 'Close to Close':
            datos['close_to_close_pct'] = datos['Close'].shift(-1) / datos['Close'] - 1
            # Calcula la ganancia
            datos['Ganancia'] = datos.apply(lambda row: abs(
                row['close_to_close_pct']) if row['acierto'] else -abs(row['close_to_close_pct']), axis=1)
        else:
            datos['close_to_open_pct'] = datos['Open'].shift(-1) / datos['Close'] - 1 
            # Calcula la ganancia
            datos['Ganancia'] = datos.apply(lambda row: abs(
                row['close_to_open_pct']) if row['acierto'] else -abs(row['close_to_open_pct']), axis=1)

        # Calcula la ganancia acumulada
        datos['Ganancia_Acumulada'] = datos['Ganancia'].cumsum()

        matrix=np.zeros((2,2)) # form an empty matric of 2x2
        for i in range(len(datos[column_name])): #the confusion matrix is for 2 classes: 1,0
                #1=positive, 0=negative
            if int(datos[column_name][i])==1 and int(datos['Direction'][i])==1: 
                matrix[0,0]+=1 #True Positives
            elif int(datos[column_name][i])==1 and int(datos['Direction'][i])==0:
                   matrix[0,1]+=1 #False Positives
            elif int(datos[column_name][i])==0 and int(datos['Direction'][i])==1:
                  matrix[1,0]+=1 #False Negatives
            elif int(datos[column_name][i])==0 and int(datos['Direction'][i])==0:
                matrix[1,1]+=1 #True Negatives
            
                    
        # Calculate F1-score
        tp, fp, fn, tn = matrix.ravel()
        datos['tp'] = tp
        datos['tn'] = tn
        datos['fp'] = fp
        datos['fn'] = fn
        precision = tp / (tp + fp)
        datos['precision'] = precision
        recall = tp / (tp + fn)
        datos['recall'] = recall
        f1_score = 2 * (precision * recall) / (precision + recall)
        datos['f1_score'] = f1_score

        
        datos.to_excel(excel_buffer, index=False)
                
        # Crear archivo zip con ambos archivos
        with zipfile.ZipFile("resultados.zip", "w") as zf:
            zf.writestr("resultados_trades_1.xlsx", excel_buffer.getvalue())
            zf.writestr("resultados_backtesting.png", img_buffer.getvalue())
            zf.writestr("datos.xlsx", excel_buffer.getvalue())

        '''
        Comprimir ambos archivos y descargarlos en un archivo ZIP:
        '''
        
        with open("resultados.zip", "rb") as f:
            st.download_button(
                label="Descargar Resultados ZIP",
                data=f,
                file_name="resultados.zip",
                mime="application/zip"
            )

if __name__ == "__main__":
    main()


    
    
#API KEY que se tenía
#tXoXD_m9y_wE2kLEILzsSERW3djux3an
#KCIUEY7RBRKTL8GI
