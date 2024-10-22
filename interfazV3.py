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

# Variables globales para almacenar datos1 y datos2
datos1 = None
datos2 = None

def open_close(ticker, api_key, fecha_inicio, fecha_fin):
    global datos1, datos2
    ticker = "SPY"
    client = RESTClient(api_key)
    local_tz = pytz.timezone('America/New_York')
    i = 1
    try:
        # Obtener datos agregados cada 15 minutos
        resp = client.get_aggs(ticker=ticker, multiplier=15, timespan="minute", 
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
 
    base_url = 'https://financialmodelingprep.com/api/v3/historical-chart/15min/SPY'
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
            "interval": "15min",
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
        if "Time Series (15min)" in data:
            time_series = data["Time Series (15min)"]
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
    
    
api_av = "KCIUEY7RBRKTL8GI"

def listar_archivos_xlxs(directorio):
    archivos = [archivo for archivo in os.listdir(directorio) if archivo.endswith('.xlsx')]
    return archivos


def cargar_datos(filepath):
    data = pd.read_excel(filepath)
    if 'date' in data.columns:
        data['date'] = pd.to_datetime(data['date'])
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

def obtener_historico_15min(ticker_opcion, api_key, fecha_inicio, fecha_fin):
    #fecha_inicio.strftime('%Y-%m-%d')
    #api_av = "KCIUEY7RBRKTL8GI"
    #st.write(fecha_inicio)
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
        for hour_offset in range(0, 24 * 60, 15):  # Iterar cada 15 minutos
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

option_hours = 1  # Buscar opciones cercanas en un rango de 1 hora
option_offset_minutes = 30  # Margen de 30 minutos en ambos sentidos
              
def realizar_backtest(data_filepath, api_key, ticker, balance_inicial, pct_allocation, fecha_inicio, fecha_fin, option_days=30, option_offset=0, trade_type='Close to Close', periodo='Diario', column_name='toggle_false', esce1=False):
    data = cargar_datos(data_filepath)
    balance = balance_inicial
    resultados = []
    client = RESTClient(api_key)
    
    # Variables para rastrear posiciones abiertas
    posicion_abierta = False
    tipo_posicion = None
    precio_entrada = 0
    fecha_entrada = None
    num_contratos = 0
    option_name = ''
    señal_anterior = None  # Para comparar señales entre días
    
    if periodo == 'Diario':
        fecha_inicio = fecha_inicio.date()
        fecha_fin = fecha_fin.date()
    else:
        fecha_inicio = pd.Timestamp(fecha_inicio)
        fecha_fin = pd.Timestamp(fecha_fin)

    for date, row in data.iterrows():
        if periodo == 'Diario':
            date = date.date()
        else:
            date = pd.Timestamp(date)
            
        if date < fecha_inicio or date > fecha_fin:
            continue
        if row[column_name] not in [0, 1]:
            continue
        
        
        if periodo == 'Diario':
            señal_actual = row[column_name]
            # Nueva estrategia cuando el checkbox está seleccionado y el periodo es 'Diario'
            if esce1:
                # No hay posición abierta, evaluamos si abrimos una nueva
                if not posicion_abierta:
                    # Obtener los datos necesarios para abrir la posición
                    st.write("No hay posiciones abiertas...")
                    if señal_actual in [0, 1]:
                        st.write(señal_anterior)
                        st.write(señal_actual)
                        # (Código existente para obtener option_price, option_date, option_name, df_option, etc.)
                        data_for_date = yf.download(ticker, start=date - pd.DateOffset(days=1), end=date + pd.DateOffset(days=1))
                        st.dataframe(data_for_date)
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
                            st.write("option price:")
                            st.write(option_price)
                    
                        option_date = encontrar_opcion_cercana(client, date, option_price, row[column_name], option_days, option_offset, ticker)
                        if option_date:
                            option_type = 'C' if row[column_name] == 1 else 'P'
                            option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                            df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
                            if not df_option.empty:
                                option_open_price = df_option[precio_usar_apertura].iloc[0]
                                option_close_price = df_option[precio_usar_cierre].iloc[index]
                                max_contract_value = option_open_price * 100
                                num_contratos = int((balance * pct_allocation) / max_contract_value)
                                trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                                balance += trade_result
                                
                                # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                                etf_data = yf.download(ticker, start=date, end=date + pd.Timedelta(days=1))
                                etf_open_price = etf_data['Open'].iloc[0] if not etf_data.empty else None
                                etf_close_price = etf_data['Close'].iloc[0] if not etf_data.empty else None
                                
                                st.write("Ganancias:")
                                st.write(trade_result)
                                
                                if trade_result < 0:
                                    # Abrimos la posición
                                    posicion_abierta = True
                                    tipo_posicion = 'Call' if señal_actual == 1 else 'Put'
                                    precio_entrada = option_open_price
                                    fecha_entrada = date
                                    # No registramos el resultado aún
                                    # Guardamos la señal actual para la siguiente iteración
                                    señal_anterior = señal_actual
                                    posicion_abierta=True
                                else:
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
                                    
                            else:
                                continue
                        else:
                            continue
                    else:
                        continue
                else:
                    # Hay una posición abierta, evaluamos si mantenerla o cerrarla
                    st.write("Hay una posicion abierta")
                    # Obtener el precio de cierre de la opción al final del día actual
                    df_option = obtener_historico(option_name, api_key, fecha_entrada, date)
                    if not df_option.empty:
                        option_close_price = df_option['close'].iloc[-1]
                        # Calculamos el resultado actual de la posición
                        trade_result_temporal = (option_close_price - precio_entrada) * 100 * num_contratos
                        if trade_result_temporal < 0:
                            # La posición está en pérdida
                           if señal_actual == señal_anterior:
                               # La señal es la misma, mantenemos la posición abierta
                               pass  # No hacemos nada, mantenemos la posición
                           else:
                                # La señal ha cambiado, cerramos la posición inmediatamente al precio de apertura del día
                                # Obtener el precio de apertura de la opción en la fecha actual
                                df_option_actual = obtener_historico(option_name, api_key, date, date)
                                if not df_option_actual.empty:
                                    option_close_price = df_option_actual['open'].iloc[0]
                                    trade_result = (option_close_price - precio_entrada) * 100 * num_contratos
                                    balance += trade_result
                                
                                    # Registramos el resultado
                                    resultados.append({
                                        'Fecha Entrada': fecha_entrada,
                                        'Fecha Salida': date,
                                        'Fecha': date,
                                        'Tipo': tipo_posicion,
                                        'toggle_false': row[column_name],
                                        'toggle_true': row[column_name],
                                        'Precio Entrada': precio_entrada,
                                        'Precio Salida': option_close_price,
                                        'Resultado': trade_result,
                                        'Contratos': num_contratos,
                                        'Opcion': option_name,
                                        'Balance': balance
                                    })
                                    # Resetear variables de posición
                                    posicion_abierta = False
                                    tipo_posicion = None
                                    precio_entrada = 0
                                    fecha_entrada = None
                                    num_contratos = 0
                                    option_name = '' 
                                else:
                                    # No se pudieron obtener datos de la opción, manejamos este caso
                                    print(f"No se pudieron obtener datos de apertura de la opción {option_name} para la fecha {date}")
                                    # Decidimos cerrar la posición por seguridad
                                    posicion_abierta = False
                                    tipo_posicion = None
                                    precio_entrada = 0
                                    fecha_entrada = None
                                    num_contratos = 0
                                    option_name = ''
                        else:
                            # La posición está en ganancia, la cerramos al final del día
                            trade_result = trade_result_temporal
                            balance += trade_result
                            # Registramos el resultado
                            resultados.append({
                                'Fecha Entrada': fecha_entrada,
                                'Fecha Salida': date,
                                'Fecha': date,
                                'Tipo': tipo_posicion,
                                'toggle_false': row[column_name],
                                'toggle_true': row[column_name],
                                'Precio Entrada': precio_entrada,
                                'Precio Salida': option_close_price,
                                'Resultado': trade_result,
                                'Contratos': num_contratos,
                                'Opcion': option_name,
                                'Balance': balance
                            })
                            # Resetear variables de posición
                            posicion_abierta = False
                            tipo_posicion = None
                            precio_entrada = 0
                            fecha_entrada = None
                            num_contratos = 0
                            option_name = '' 
                    else:
                        # No se pudieron obtener datos de la opción, manejamos este caso
                        print(f"No se pudieron obtener datos de la opción {option_name} para la fecha {date}")
                        # Decidimos cerrar la posición por seguridad
                        posicion_abierta = False
                        tipo_posicion = None
                        precio_entrada = 0
                        fecha_entrada = None
                        num_contratos = 0
                        option_name = ''

            #if periodo == 'Diario':
            else: #esce1 = False           
                data_for_date = yf.download(ticker, start=date, end=date + pd.DateOffset(days=1))
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
                option_date = encontrar_opcion_cercana(client, date, option_price, row[column_name], option_days, option_offset, ticker)
                if option_date:
                    option_type = 'C' if row[column_name] == 1 else 'P'
                    option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                    df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
                    if not df_option.empty:
                        option_open_price = df_option[precio_usar_apertura].iloc[0]
                        option_close_price = df_option[precio_usar_cierre].iloc[index]
                        max_contract_value = option_open_price * 100
                        num_contratos = int((balance * pct_allocation) / max_contract_value)
                        trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                        balance += trade_result
                        
                        # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                        etf_data = yf.download(ticker, start=date, end=date + pd.Timedelta(days=1))
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
            data_for_date = yf.download(ticker, start=date, end=date + pd.DateOffset(days=1))
            #st.write("Fecha date:",date)
            #st.write("Fecha inicio:",fecha_inicio)
            #st.write("Fecha fin:",fecha_fin)
            data_for_date2 = get_open_and_close(ticker, api_av, fecha_inicio, fecha_fin)
            data_for_date3 = open_close(ticker, api_key, fecha_inicio, fecha_fin)
            data_for_date4 = mostrar_datos()
            data_for_date_fm = get_spy_intraday_financial_modeling(fecha_inicio, fecha_fin)
            #st.write(start)
            #st.write(data_for_date)
            st.write ("dataframe fm")
            st.write(data_for_date_fm)
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
        
            option_date = encontrar_opcion_cercana_15min(client, date, option_price, row[column_name], option_days, option_offset, ticker)
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
            st.write("df_option:")
            st.dataframe(df_option)
            st.write("función get_open_and_close:")
            st.dataframe(df)
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
                num_contratos = int((balance * pct_allocation) / max_contract_value)
                trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                balance += trade_result

                # Obtener el símbolo del ETF del índice (por ejemplo, 'SPY' para el índice S&P 500)
                #etf_symbol = 'SPY'  # Reemplaza 'SPY' con el símbolo correcto de tu ETF de índice
                
                # Usar la nueva función de Alpha Vantage para obtener los datos del ETF
                #etf_open_price, etf_close_price = get_alpha_vantage_data(ticker, date)
       
                # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                etf_data = yf.download(ticker, start=date, end=date + pd.Timedelta(days=1))
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

    plt.legend()
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
    option_days_input = st.number_input("*Option Days:* (Número de días de vencimiento de la opción que se está buscando durante el backtesting)", min_value=0, max_value=90, value=20, step=1)
    
    # Option Offset input
    option_offset_input = st.number_input("*Option Offset:* (Rango de días de margen alrededor del número de días objetivo dentro del cual se buscará la opción más cercana)", min_value=0, max_value=90, value=5, step=1)
    
    # Additional inputs for the backtest function
    balance_inicial = st.number_input("*Balance iniciall*", min_value=0, value=100000, step= 1000)
    pct_allocation = st.number_input("*Porcentaje de Asignación de Capital:*", min_value=0.001, max_value=0.6, value=0.05)
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
                <span class="tooltiptext">Opción actualmente en construcción...</span>
            </div>
            """, unsafe_allow_html=True)
    else:
        esce1 = False
        
    fecha_inicio = st.date_input("*Fecha de inicio del periodo de backtest:*", min_value=datetime(2020, 1, 1))
    fecha_fin = st.date_input("*Fecha de finalización del periodo de backtest:*", max_value=datetime.today())
    if periodo == '15 minutos':
        open_hour = st.time_input("*Seleccionar Hora de Apertura:*", value=datetime.strptime("09:30", "%H:%M").time())
        close_hour = st.time_input("*Seleccionar Hora de Cierre:*", value=datetime.strptime("16:00", "%H:%M").time())
    trade_type = st.radio('*Tipo de Operación*', ('Close to Close', 'Open to Close', 'Close to Open'))
    
    
        
    if st.button("Run Backtest"):
        resultados_df, final_balance = realizar_backtest(data_filepath, 'tXoXD_m9y_wE2kLEILzsSERW3djux3an', "SPY", balance_inicial, pct_allocation, pd.Timestamp(fecha_inicio), pd.Timestamp(fecha_fin), option_days_input, option_offset_input, trade_type, periodo, column_name, esce1)
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
        plt.legend()
        plt.grid(True, which='both', linestyle='-', linewidth=0.5)
        plt.tight_layout()
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png')
        st.image(img_buffer)
        st.download_button(label="Descargar Gráfico", data=img_buffer, file_name="resultados_backtesting.png", mime="image/png")

        
            
        datos = pd.read_excel(r"resultados_trades_1.xlsx")
        datos = datos[(datos['Fecha'] >= pd.Timestamp(fecha_inicio)) & (datos['Fecha'] <= pd.Timestamp(fecha_fin))]
        if trade_type == 'Close to Close':
            datos['Direction'] = (datos['Close'] > datos['Close'].shift(1)).astype(int)
        elif trade_type == 'Close to Open':
            datos['Direction'] = (datos['Close'].shift(1) < datos['Open']).astype(int)
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
        datos['open_to_close_pct'] = datos['Close']/datos['Open'] - 1

        # Calcula la ganancia
        datos['Ganancia'] = datos.apply(lambda row: abs(
            row['open_to_close_pct']) if row['acierto'] else -abs(row['open_to_close_pct']), axis=1)

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
