import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from polygon import RESTClient
from datetime import timedelta, date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from datetime import time
import streamlit as st
import time
import io
import os
import zipfile
import numpy as np
import requests
import pytz
import datetime as dt

df_opciones_cache = None
spy_daily_cache = None
#API_URL = "http://127.0.0.1:8000"  # Modo Local (Cuando corre en la misma VPS)
API_URL = "http://93.127.133.246:8000"  # Modo Remoto (en PC diferente a la VPS)

def obtener_todos_option_names_api():
    """
    Consulta todos los OptionName únicos llamando a la API.
    """
    global df_opciones_cache
    
    if df_opciones_cache is not None:
        return df_opciones_cache

    try:
        response = requests.get(f"{API_URL}/tickers")
        if response.status_code == 200:
            tickers = response.json()
            df_opciones_cache = pd.DataFrame(tickers, columns=["ticker"])
            return df_opciones_cache
        else:
            print(f"Error API: {response.status_code}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error de conexión: {e}")
        return pd.DataFrame()

def obtener_precios_api(option_name: str, start_time: pd.Timestamp, end_time: pd.Timestamp) -> tuple:
    """
    Obtiene precios de una opción vía API.
    """
    # Convertir a nanosegundos para la API
    start_ns = int(start_time.value)
    end_ns = int(end_time.value)
    
    try:
        params = {"start_ns": start_ns, "end_ns": end_ns}
        response = requests.get(f"{API_URL}/options/{option_name}/history", params=params)
        
        if response.status_code != 200:
            return pd.DataFrame(), False
            
        data = response.json()
        if not data["data"]:
            return pd.DataFrame(), False
            
        df = pd.DataFrame(data["data"], columns=data["columns"])
        
        # Procesamiento para compatibilidad con tu código original
        # window_start viene en nanosegundos, lo convertimos a datetime
        df['Date'] = pd.to_datetime(df['window_start'], unit='ns')
        
        # Renombrar columnas para que coincidan con lo que espera tu tester
        df = df.rename(columns={
            'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'
        })
        
        df.set_index('Date', inplace=True)
        df.index.name = None
        df.columns = [col.lower() for col in df.columns]
        
        return df, True

    except Exception as e:
        print(f"Error en obtener_precios_api: {e}")
        return pd.DataFrame(), False

def obtener_precios_spy_final_api(date: pd.Timestamp) -> tuple:
    """
    Busca Open/Close de SPY para una fecha exacta vía API.
    """
    date_str = date.strftime('%Y-%m-%d')
    
    try:
        # Pedimos el día específico
        params = {"start_date": date_str, "end_date": (date + timedelta(days=1)).strftime('%Y-%m-%d')}
        response = requests.get(f"{API_URL}/spy/history", params=params)
        
        if response.status_code == 200:
            data = response.json()
            if data["data"]:
                # Buscamos coincidencia exacta de timestamp si es necesario, 
                # o tomamos el primer registro del día (SPYhistorical suele ser diario)
                row = data["data"][0]
                # Asumiendo columnas: Date, Open, High, Low, Close, Volume
                # Indices: 1=Open, 4=Close
                return row[1], row[4]
        
        return None, None
    except Exception as e:
        print(f"Error API SPY: {e}")
        return None, None

def obtener_historico_api(ticker_opcion, fecha_inicio, fecha_fin):
    """
    Obtiene datos y los agrupa por día (Open del inicio, Close del final).
    """
    # --- INTENTO 1: Endpoint Optimizado (Caché Diario) ---
    try:
        start_str = pd.to_datetime(fecha_inicio).strftime('%Y-%m-%d')
        end_str = pd.to_datetime(fecha_fin).strftime('%Y-%m-%d')
        
        # Llamada al nuevo endpoint daily_optimized
        response = requests.get(
            f"{API_URL}/options/{ticker_opcion}/daily_optimized",
            params={"start_date": start_str, "end_date": end_str}
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("data"):
                df = pd.DataFrame(data["data"], columns=data["columns"])
                df['fecha'] = pd.to_datetime(df['date']).dt.date
                df.set_index('fecha', inplace=True)
                return df[['open', 'close']]
    except Exception as e:
        print(f"⚠️ Falló endpoint optimizado, usando fallback: {e}")

    # --- INTENTO 2: Fallback a Histórico Completo (Lógica Original) ---
    start_ts = pd.to_datetime(fecha_inicio)
    end_ts = pd.to_datetime(fecha_fin)
    
    start_ns = int(start_ts.timestamp() * 1e9)
    end_ns = int((end_ts + timedelta(days=1)).timestamp() * 1e9)
    
    try:
        params = {"start_ns": start_ns, "end_ns": end_ns}
        response = requests.get(f"{API_URL}/options/{ticker_opcion}/history", params=params)
        
        if response.status_code != 200:
            return pd.DataFrame()
            
        data = response.json()
        if not data["data"]:
            return pd.DataFrame()
            
        df = pd.DataFrame(data["data"], columns=data["columns"])
        df['fecha'] = pd.to_datetime(df['window_start'], unit='ns')
        df['date_group'] = df['fecha'].dt.date
        
        # Agrupación diaria (lógica original)
        df_daily = df.groupby('date_group').agg({
            'open': 'first',
            'close': 'last'
        })
        
        df_daily.index.name = 'fecha'
        return df_daily
        
    except Exception as e:
        print(f"Error obtener_historico_api: {e}")
        return pd.DataFrame()

def obtener_todos_option_names_sql():
    """
    Consulta todos los OptionName únicos de la base de datos y los almacena en caché.
    Ahora usa la API.
    """
    global df_opciones_cache
    
    # 1. Si el cache ya existe, retornarlo directamente (Singleton)
    if df_opciones_cache is not None:
        return df_opciones_cache

    return obtener_todos_option_names_api()

def obtener_precios_sql_2(option_name: str, start_time: pd.Timestamp, end_time: pd.Timestamp) -> pd.DataFrame:
    """Ahora usa la API."""
    return obtener_precios_api(option_name, start_time, end_time)


def open_close(ticker, api_key, fecha_inicio, fecha_fin): #Parcialmente no en uso
    global datos1, datos2
    ticker = "SPY"
    client = RESTClient(api_key)
    local_tz = pytz.timezone('America/New_York')
    i = 1
    try:
        # Obtener datos agregados cada 15 minutos
        resp = client.get_aggs(ticker=ticker, multiplier=1, timespan="minute", #VOLVER A CAMBIAR A 15 MIN
                               from_=fecha_inicio, to=fecha_fin)
        #print(resp)
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
        #print("Respuesta JSON completa:", data)
        
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
            
            #print("DataFrame completo antes de filtrar por fecha:", df)
            
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
            
           #print("DataFrame filtrado por rango de fechas:", df)
            #print("Valores de Open y Close para el rango de fechas:", df_completo[['open', 'close']])
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

# def verificar_opcion(client, ticker, start_date, end_date):
#     try:
#         resp = client.get_aggs(ticker=ticker, multiplier=1, timespan="day", from_=start_date.strftime('%Y-%m-%d'), to=end_date.strftime('%Y-%m-%d'))
#         return len(resp) > 0
#     except:
#         return False
    
#ASD
def verificar_opcion(client, ticker, start_date, end_date):
    # 1. Intentar verificar usando el caché de SQL (Optimización de cuello de botella)
    df_cache = obtener_todos_option_names_api()
    if df_cache is not None and not df_cache.empty and 'ticker' in df_cache.columns:
        return ticker in df_cache['ticker'].values

    try:
        resp = client.get_aggs(ticker=ticker, multiplier=1, timespan="day", from_=start_date.strftime('%Y-%m-%d'), to=end_date.strftime('%Y-%m-%d'))
        return len(resp) > 0
    except:
        return False
    
def verificar_opcion_15min(client, ticker, fecha_inicio, fecha_fin):
    # 1. Intentar verificar usando el caché de SQL
    df_cache = obtener_todos_option_names_api()
    if df_cache is not None and not df_cache.empty and 'ticker' in df_cache.columns:
        return ticker in df_cache['ticker'].values

    try:
        resp = client.get_aggs(ticker=ticker, multiplier=15, timespan="minute", from_=fecha_inicio.strftime('%Y-%m-%d'), to=fecha_fin.strftime('%Y-%m-%d'))
        return len(resp) > 0
    except:
        return False
    
def obtener_historico(ticker_opcion, api_key, fecha_inicio, fecha_fin):
    """
    Reemplazo de obtener_historico usando SQL.
    Agrega datos de minutos (window_start en ns) a velas diarias (Open/Close).
    """
    return obtener_historico_api(ticker_opcion, fecha_inicio, fecha_fin)

  
def obtener_historico_30min_start_time(ticker_opcion, api_key, fecha_inicio, fecha_fin):
    """
    Reemplazo de obtener_historico_30min_start_time usando SQL (Tabla SPYoptions).
    Retorna datos intradía (minuto a minuto) procesados igual que la función original.
    """
    df, success = obtener_precios_api(ticker_opcion, pd.to_datetime(fecha_inicio), pd.to_datetime(fecha_fin))
    if success:
        return df
    else:
        return pd.DataFrame()
    
#def encontrar_opcion_cercana(client, base_date, option_price, column_name, option_days, option_offset, ticker):
    #min_days = option_days - option_offset #23
    #max_days = option_days + option_offset #37
    #best_date = None
    #for offset in range(min_days, max_days + 1):
        #option_date = (base_date + timedelta(days=offset)).strftime('%y%m%d')
        #option_type = 'C' if column_name == 1 else 'P'
        #option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
        #if verificar_opcion(client, option_name, base_date, base_date + timedelta(days=1)):
            #best_date = option_date
            #break
    #return best_date

def encontrar_opcion_cercana(client, base_date, option_price, column_name, option_days, option_offset, ticker):
    # Iteramos 'i' desde 0 hasta el límite del offset (ej: 0, 1, 2... 5)
    for i in range(option_offset + 1):
        # Definimos los intentos para este nivel de 'i'.
        # Si i=0, solo probamos el objetivo (ej: 30).
        # Si i>0, probamos primero ARRIBA (+i) y luego ABAJO (-i).
        desplazamientos = [i] if i == 0 else [i, -i]
        
        for k in desplazamientos:
            # Calculamos los días objetivo (Ej: 30 + 1 = 31)
            dias_a_probar = option_days + k
            
            # Construimos la fecha usando el desplazamiento calculado
            option_date = (base_date + timedelta(days=dias_a_probar)).strftime('%y%m%d')
            option_type = 'C' if column_name == 1 else 'P'
            
            # Construimos el nombre del contrato
            option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
            #print(option_name,"  ",base_date)
            
            # Verificamos si existe
            if verificar_opcion(client, option_name, base_date, base_date + timedelta(days=1)):
                #Si existe, retornamos esta fecha inmediatamente (la más cercana a la meta)
                #input("Waiting 1")
                return option_date
                
    return None

# NUEVA VERSIÓN: Ahora devuelve el DataFrame encontrado para evitar re-consultar
def encontrar_opcion_cercana_optimizada(client, api_key, base_date, option_price, column_name, option_days, option_offset, ticker):
    for i in range(option_offset + 1):
        desplazamientos = [i] if i == 0 else [i, -i]
        for k in desplazamientos:
            dias_a_probar = option_days + k
            option_date = (base_date + timedelta(days=dias_a_probar)).strftime('%y%m%d')
            option_type = 'C' if column_name == 1 else 'P'
            option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
            
            # FUSIÓN: Intentamos traer los datos directamente. 
            # Si obtener_historico devuelve un DF con datos, ya validamos que existe y tenemos la info.
            #print(option_name,"  ",base_date)
            df = obtener_historico_api(option_name, base_date, base_date + timedelta(days=option_days))
            #print(df)
            
            if not df.empty:
                
                # Verificar que la fecha de trade (base_date) esté presente en los datos
                target_date = base_date.date() if hasattr(base_date, 'date') else base_date
                
                # Validación robusta: Manejar tanto índice de Fechas como de Timestamps
                fechas_disponibles = df.index.date if isinstance(df.index, pd.DatetimeIndex) else df.index
                
                if target_date in fechas_disponibles:
                    # input("Waiting 2")
                    return option_date, df # Retornamos la fecha Y el DataFrame
                
    return None, pd.DataFrame()

def encontrar_opcion_cercana_15min(client, base_date, option_price, column_name,option_days, option_offset, ticker):
    min_days = option_days - option_offset #23
    max_days = option_days + option_offset #37
    best_date = None
    for offset in range(min_days, max_days + 1):
        for hour_offset in range(0, 24 * 60, 5):  # Iterar cada 15 minutos   CAMBIAR A 15 MIN
            option_date = (base_date + timedelta(days=offset, minutes=hour_offset)).strftime('%y%m%d')       
            option_type = 'C' if column_name == 1 else 'P'
            option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
            #print("Dentro de la función 15min")
            #print(option_date)
            #print(option_name)
            if verificar_opcion(client, option_name, base_date, base_date + timedelta(days=1)):
                best_date = option_date
                print("K2 ",best_date)
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

# NUEVA VERSIÓN: Entrega el DF al proceso principal
def encontrar_strike_cercano_optimizado(client, api_key, base_date, option_price, column_name, option_days, option_offset, ticker, method, offset, it_max = 10):
    actual_option_price = desfasar_strike(option_price, column_name, method, offset)
    for i in range(it_max):
        # Llamamos a la versión optimizada
        #best_date, df_option = encontrar_opcion_cercana(client,base_date, actual_option_price, column_name, option_days, option_offset, ticker)
        best_date, df_option = encontrar_opcion_cercana_optimizada(client, api_key, base_date, actual_option_price, column_name, option_days, option_offset, ticker)
        
        if best_date is not None:
            return best_date, actual_option_price, df_option
            
        if column_name == 1:
            actual_option_price += ((i + 1)*((-1)**i))
        else:
            actual_option_price -= ((i + 1)*((-1)**(i + 1)))

    return None, actual_option_price, pd.DataFrame()
              
def obtener_datos_spy_diario_api(fecha_inicio, fecha_fin):
    """
    Obtiene datos diarios del SPY vía API.
    """
    global spy_daily_cache
    
    start_str = pd.to_datetime(fecha_inicio).strftime('%Y-%m-%d')
    end_str = pd.to_datetime(fecha_fin).strftime('%Y-%m-%d')
    
    # (Aquí podrías agregar lógica de caché local si quisieras)
    
    try:
        params = {"start_date": start_str, "end_date": end_str}
        response = requests.get(f"{API_URL}/spy/history", params=params)
        
        if response.status_code != 200:
            return pd.DataFrame()
            
        data = response.json()
        if not data["data"]:
            return pd.DataFrame()
            
        df = pd.DataFrame(data["data"], columns=data["columns"])
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        return df
        
    except Exception as e:
        print(f"Error SPY diario API: {e}")
        return pd.DataFrame()

def obtener_datos_spy_diario_sql(fecha_inicio, fecha_fin):
    """Ahora usa la API."""
    return obtener_datos_spy_diario_api(fecha_inicio, fecha_fin)

def realizar_backtest(data_filepath, api_key, ticker, balance_inicial, pct_allocation, fixed_amount, allocation_type, fecha_inicio, fecha_fin, option_days=30, option_offset=0, trade_type='Open to Close', periodo='Diario', column_name='toggle_false', method = "ATM", offset = 5, esce1=False, contratos_especificos=False):
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
        
    if "start_time" and "end_time" in data.columns:
        
        #Se establece la conexión a SQL Server para consultar el precio del subyacente
        # establecer_conexion_sql() # Ya no es necesario
        
        print("⏳ Procesando tiempos de entrada y salida (Optimizado)...")
        nombre_de_la_columna = 'start_time'
        
        # --- OPTIMIZACIÓN VECTORIZADA (Reemplaza el bucle lento) ---
        # Aseguramos que sean datetime para la comparación numérica
        data['start_time'] = pd.to_datetime(data['start_time'])
        data['end_time'] = pd.to_datetime(data['end_time'])
        
        start_times_values = data['start_time'].values
        end_times_values = data['end_time'].values
        
        # searchsorted encuentra el índice donde start_time >= end_time de forma binaria (muy rápido)
        # Asume que el archivo está ordenado cronológicamente por start_time
        indices_siguientes = np.searchsorted(start_times_values, end_times_values, side='left')
        
        # Mapear índices a valores, manejando el caso donde no hay siguiente (índice fuera de rango)
        n = len(data)
        siguientes_times = [start_times_values[i] if i < n else pd.NaT for i in indices_siguientes]
        
        data['siguiente_start_time'] = siguientes_times
        print("✅ Tiempos procesados.")

    # data_for_ROI = yf.download("SPY", start=fecha_inicio, end=fecha_fin + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)     
    # Reemplazo SQL para ROI
    data_for_ROI = obtener_datos_spy_diario_api(fecha_inicio, fecha_fin + pd.DateOffset(days=1))
    
    if not data_for_ROI.empty:
        ROI_SPY = ((data_for_ROI['Close'].iloc[-1] - data_for_ROI['Open'].iloc[0]) / data_for_ROI['Open'].iloc[0]) * 100
    else:
        print("⚠️ No se pudieron obtener datos para ROI (DataFrame vacío). Se establece ROI = 0.")
        ROI_SPY = 0
        
    print(f"🚀 Iniciando bucle de backtest sobre {len(data)} registros...")
    for date, row in data.iterrows():
        
        if periodo == 'Diario':
            # Intentamos obtener solo la fecha, pero manejamos la excepción si date ya es NaT
            try:
                date = date.date()
            except AttributeError:
                # Si esto falla (porque el índice ya era NaT), saltamos la iteración
                continue
        else:
            date = pd.Timestamp(date)
        
        print(f"📅 Procesando fecha: {date}", end='\r') # end='\r' sobrescribe la línea para no llenar la consola

        # 🟢 INSERTAR AQUÍ LA SOLUCIÓN AL TypeError (NaT)
        if pd.isnull(date): # Verifica si la variable 'date' es nula/inválida
            continue        # Si es nula, salta esta fila inmediatamente     
        
        if date < fecha_inicio or date > fecha_fin:
            continue
        
        if row[column_name] not in [0, 1]:
            continue
        
        if "start_time" and "end_time" in data.columns:
        
            colombia_tz = 'America/Bogota'
            ny_tz = 'America/New_York'
         
            señal_actual = row[column_name]
                
            #2. Extraer tiempos de entrada y salida del archivo
            start_time = pd.to_datetime(row['start_time'])
            #start_time = start_time.tz_localize(colombia_tz).tz_convert(ny_tz)
            start_time = start_time.tz_localize(ny_tz)
            start_time = start_time.tz_localize(None)
            
            #start_time = start_time.round('s')

            next_start_time = pd.to_datetime(row['siguiente_start_time'])
            # Verificar que existe un siguiente válido
            if pd.notna(next_start_time):
                next_start_time = next_start_time.tz_localize(ny_tz)
                next_start_time = next_start_time.tz_localize(None)
            
            end_time = pd.to_datetime(row['end_time'])
            #end_time = end_time.tz_localize(colombia_tz).tz_convert(ny_tz)
            end_time = end_time.tz_localize(ny_tz)
            end_time = end_time.tz_localize(None)
            
            # 'start_time' ya es el Timestamp exacto de la fila del Excel (con fecha y hora)
            spy_open, spy_close = obtener_precios_spy_final_api(start_time)
            print("start time:")
            print(start_time)
            print("end time:")
            print(end_time)
            print("spy open:")
            print(spy_open)
            print("spy close:")
            print(spy_close)
            if spy_open is not None and spy_close is not None:
                ## Usamos los precios obtenidos de SQL para ese Timestamp exacto
                precio_usar_apertura_excel = spy_open
                precio_usar_cierre_excel = spy_close
                # option_price usa el precio de apertura para encontrar el strike
                option_price = round(spy_open)
            else:
                print("No se encontraron datos de open o close del subyacente SPY en la BD")           
            
            # Llama a tu función existente para obtener los datos del ETF
            #spy_intraday_historial = open_close_30min("SPY", api_key, fecha_inicio, fecha_fin)
            #print(spy_intraday_historial)
            
            # ========== NUEVO: CERRAR POSICIONES QUE YA LLEGARON A SU END_TIME ==========
            posiciones_a_mantener = []
            
            for pos in posiciones_abiertas:
                # Si el start_time actual >= end_time de esta posición, CERRARLA
                if start_time >= pos['end_time']:
                    if contratos_especificos and "OptionName" in data.columns:
                        # Calcular ganancia/pérdida de ESTA posición específica
                        trade_result_pos = (pos['df_option_cierre'][pos['precio_usar_cierre']] 
                       - pos['option_open_price']) * 100 * pos['num_contratos']
                    else:                       
                        trade_result_pos = (pos['df_option_cierre'][pos['precio_usar_cierre']].iloc[pos['index']] 
                                           - pos['option_open_price']) * 100 * pos['num_contratos']
                    
                    # Actualizar balance con la ganancia de esta posición cerrada
                    balance += trade_result_pos
                    
                    # Devolver el costo de esta posición al balance_posiciones
                    balance_posiciones += pos['cost_trade']
                    
                    # ✅ NUEVO: Actualizar el resultado en la fila correspondiente
                    # Buscar la fila en resultados que corresponde a esta posición
                    for resultado in resultados:
                        if (resultado['Fecha Apertura'] == pos['start_time'] and 
                            resultado['Opcion'] == pos['option_name']):
                            resultado['Resultado'] = trade_result_pos  # Actualizar de 0 al valor real
                            break
                    
                    # Opcional: Log del cierre
                    #print(f"✅ Cerrada posición: {pos['option_name']}, Resultado: ${trade_result_pos:.2f}")
                    
                else:
                    # Esta posición sigue abierta, mantenerla
                    posiciones_a_mantener.append(pos)
            
            # Actualizar la lista de posiciones abiertas (sin las que se cerraron)
            posiciones_abiertas = posiciones_a_mantener
            
            # Actualizar balance_posiciones después de los cierres
            balance_posiciones = balance - sum([p['cost_trade'] for p in posiciones_abiertas])
            # ========== FIN DE CIERRE DE POSICIONES ==========
            
            if señal_actual in [0,1]:
          
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
                
                if contratos_especificos and "OptionName" in data.columns:
                    
                    # --- INICIALIZACIÓN DE VARIABLES PARA REGISTRO (PREVIENE NAMERROR) ---
                    bandera_encontrada = 'No'
                    precio_entrada = None
                    precio_salida = None
                    costo_posiciones = 0
                    num_contratos_final = 0
                    resultado_potencial = 0
                    
                    option_type = 'C' if row[column_name] == 1 else 'P'
                    option_name = row['OptionName']
                    # 🟢 CAMBIO 1: Capturar el resultado de la búsqueda (DataFrame y Bandera)
                    df_option_prices_db, encontrado_bd = obtener_precios_api(option_name, start_time, end_time)
                    print(f"Precios de la Opción '{option_name} obtenidos de SQL:")
                    st.dataframe(df_option_prices_db)
                    
                    # 🟢 CAMBIO 2: Lógica de Registro Condicional                   
                    if encontrado_bd:
                        # --- A. CASO ÉXITO (Datos Encontrados) ---
                        # Obtener los precios reales para el cálculo
                        option_open_price = df_option_prices_db[precio_usar_apertura].iloc[0]                                            
                        df_option_cierre = df_option_prices_db.iloc[-1]
                        print("df option cierre:")
                        print(df_option_cierre)
                        posicion_actual_abierta = True
                        option_open_price = df_option_prices_db[precio_usar_apertura].iloc[0]##PENDIENTE DE REVISAR
                        print("Precio de entrada para la opción día actual:")
                        print(option_open_price)
                        option_close_price = df_option_prices_db[precio_usar_cierre].iloc[-1] #Revisar si debería ser -1 y no index(0)
                        print("Precio de salida opción día actual:")
                        print(option_close_price)
                        #option_close_price_cierre = df_option_cierre[precio_usar_cierre].iloc[index]#A revisar también
                        #print("Precio de salida opción día de cierre:")
                        #print(option_close_price_cierre)
                        max_contract_value = option_open_price * 100
                        #print("max_contract_value")
                        #print(max_contract_value)
                            
                        # Calcular número de contratos basado en balance_posiciones
                        if allocation_type == 'Porcentaje de asignación':
                            num_contratos = int((balance_posiciones * pct_allocation) / max_contract_value)
                        else: #allocation_type == 'Monto fijo de inversión':
                            if balance_posiciones < max_contract_value:
                                continue
                                #return pd.DataFrame(resultados), balance
                            else:
                                num_contratos = int(fixed_amount / max_contract_value)
                        
                        #print("Numero de contratos día actual:")
                        #print(num_contratos)
                        #print("Option Type actual:")
                        #print(option_type)
                        cost_trade = max_contract_value * num_contratos
                        #print("Costo de la operación:")
                        #print(cost_trade)
                        # ✅ VALIDAR ANTES DE ABRIR
                        if cost_trade > balance_posiciones or num_contratos == 0:                                
                            continue
                        # Restar el costo de la nueva posición
                        balance_posiciones -= cost_trade
                        
                        # Agregar esta nueva posición a la lista de abiertas
                        posiciones_abiertas.append({
                            'num_contratos': num_contratos,
                            'option_open_price': option_open_price,
                            'option_name': option_name,
                            'df_option_cierre': df_option_cierre,
                            'precio_usar_cierre': precio_usar_cierre,
                            'index': index,
                            'cost_trade': cost_trade,
                            'end_time': end_time,  # IMPORTANTE: Guardar el end_time
                            'start_time': start_time  # Opcional, para debugging
                        })
                            
                        #print("trade result actual positivo:")
                        #print(trade_result)
                        
                        # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                        #etf_data = yf.download("SPY", start="2022-01-01", end=date + pd.Timedelta(days=1), multi_level_index=False, auto_adjust=False)
                        #print("etf_data")
                        #print(etf_data)
                        #etf_data = etf_data.drop(etf_data.index[-1])
                        #etf_data.columns = etf_data.columns.str.lower()
                        #etf_data.index.name = 'date'
                        #etf_open_price = etf_data['open'].iloc[0] if not etf_data.empty else None
                        #print("Precio de entrada día actual:")
                        #print(etf_open_price)
                        #etf_close_price = etf_data['close'].iloc[0] if not etf_data.empty else None
                        #print("Precio salida día actual:")
                        #print(etf_close_price)
                            
                        trade_result_display = (df_option_cierre[precio_usar_cierre] - option_open_price) * 100 * num_contratos
                        # Valores a registrar en resultados
                        bandera_encontrada = 'Sí'
                        precio_entrada = option_open_price
                        precio_salida = df_option_cierre[precio_usar_cierre]
                        costo_posiciones = cost_trade
                        num_contratos_final = num_contratos
                        resultado_potencial = trade_result_display
                            
                            
                    else:
                        # --- B. CASO FALLO (Datos No Encontrados en BD) ---
                        
                        # Si no se encontró, registramos el evento como fallido sin ejecutar el trade.
                        st.warning(f"Trade fallido: {option_name}. No se encontraron datos válidos.")
                        
                        
                    # 🟢 CAMBIO 3: REGISTRO ÚNICO PARA AMBOS CASOS
                    resultados.append({
                        'Fecha': start_time, 
                        'Tipo': 'Call' if row[column_name] == 1 else 'Put',
                        'toggle_false': row[column_name],
                        'toggle_true': row[column_name],
                        'Fecha Apertura': start_time,
                        'Fecha Cierre': end_time,
                        'Precio Entrada': precio_entrada, 
                        'Precio Salida Utilizado': precio_salida,
                        'Resultado': 0,  # Solo para mostrar
                        'Resultado Potencial': resultado_potencial,
                        'Contratos': num_contratos_final,
                        'Opcion': option_name,
                        'Opcion Encontrada BD': bandera_encontrada,
                        'ROI SPY': ROI_SPY,
                        'Open': precio_usar_apertura_excel,
                        'Close': precio_usar_cierre_excel,
                        'Costo Posiciones': costo_posiciones,
                        'Balance Posiciones': balance_posiciones
                    })
                    posicion_actual_abierta = False
                    #print(trade_result)
                            
                            
                else:
                    option_date, actual_option_price, _ = encontrar_strike_cercano_optimizado(client, api_key, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                    option_price = actual_option_price
                    
                    if option_date:
                        option_type = 'C' if row[column_name] == 1 else 'P'
                        option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                        
                        df_option_start_time, _ = obtener_precios_api(option_name, date, date + timedelta(days=option_days))
                        df_option_end_time, _ = obtener_precios_api(option_name, date, date + timedelta(days=option_days))
                        
                        df_option_start_time = df_option_start_time.loc[start_time:]
                        df_option_end_time = df_option_start_time.loc[start_time:]
                        
                        if not df_option_start_time.empty:
                            df_option_end_time = df_option_start_time.loc[end_time:]
                            
                            if not df_option_end_time.empty:
                                df_option_cierre = df_option_start_time.loc[end_time:]
                                posicion_actual_abierta = True
                                option_open_price = df_option_start_time[precio_usar_apertura].iloc[0]
                                option_close_price = df_option_start_time[precio_usar_cierre].iloc[index]
                                option_close_price_cierre = df_option_cierre[precio_usar_cierre].iloc[index]
                                max_contract_value = option_open_price * 100
                                
                                if allocation_type == 'Porcentaje de asignación':
                                    num_contratos = int((balance_posiciones * pct_allocation) / max_contract_value)
                                else: 
                                    if balance_posiciones < max_contract_value:
                                        continue
                                    else:
                                        num_contratos = int(fixed_amount / max_contract_value)
                                
                                cost_trade = max_contract_value * num_contratos
                                if cost_trade > balance_posiciones or num_contratos == 0:
                                    continue
                                balance_posiciones -= cost_trade
                                
                                posiciones_abiertas.append({
                                    'num_contratos': num_contratos,
                                    'option_open_price': option_open_price,
                                    'option_name': option_name,
                                    'df_option_cierre': df_option_cierre,
                                    'precio_usar_cierre': precio_usar_cierre,
                                    'index': index,
                                    'cost_trade': cost_trade,
                                    'end_time': end_time,
                                    'start_time': start_time
                                })
                                
                                trade_result_display = (df_option_cierre[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                                resultados.append({
                                    'Fecha': start_time, 
                                    'Tipo': 'Call' if row[column_name] == 1 else 'Put',
                                    'toggle_false': row[column_name],
                                    'toggle_true': row[column_name],
                                    'Fecha Apertura': start_time,
                                    'Fecha Cierre': end_time,
                                    'Precio Entrada': option_open_price, 
                                    'Precio Salida Utilizado': df_option_cierre[precio_usar_cierre].iloc[index],
                                    'Resultado': 0,
                                    'Resultado Potencial': trade_result_display,
                                    'Contratos': num_contratos,
                                    'Opcion': option_name,
                                    'ROI SPY': ROI_SPY,
                                    'Open': precio_usar_apertura_excel,
                                    'Close': precio_usar_cierre_excel,
                                    'Costo Posiciones': cost_trade,
                                    'Balance Posiciones': balance_posiciones
                                })
                                posicion_actual_abierta = False
                            else:
                                df_option_end_time = df_option_start_time.loc[start_time:]
                                df_option_end_time.index = pd.to_datetime(df_option_end_time.index)
                                try:
                                    df_localized = df_option_end_time.tz_localize('UTC')
                                except TypeError:
                                    df_localized = df_option_end_time
                                
                                df_ny_time = df_localized.tz_convert('America/New_York')
                                latest_timestamp_ny = df_ny_time.index.max()
                                
                                if latest_timestamp_ny.tzname() == 'EDT':
                                    HORA_DE_CORTE_NY = 15
                                else:
                                    HORA_DE_CORTE_NY = 14
                                
                                punto_de_inicio_ny = pd.Timestamp(
                                    f"{latest_timestamp_ny.date()} {HORA_DE_CORTE_NY}:00:00",
                                    tz='America/New_York')
                                
                                punto_de_inicio_ny = punto_de_inicio_ny.tz_localize(None)
                                df_recortado_final = df_option_end_time.loc[punto_de_inicio_ny:]
                                
                                if df_recortado_final.empty:
                                    df_recortado_final = df_option_end_time.loc[df_option_end_time.index[-1]:]
                                    df_option_cierre = df_option_start_time.loc[df_option_end_time.index[-1]:]
                                else:
                                    df_option_cierre = df_option_start_time.loc[punto_de_inicio_ny:]
                                
                                option_open_price = df_option_start_time[precio_usar_apertura].iloc[0]
                                option_close_price = df_option_start_time[precio_usar_cierre].iloc[index]
                                option_close_price_cierre = df_option_cierre[precio_usar_cierre].iloc[index]
                                max_contract_value = option_open_price * 100
                                
                                if allocation_type == 'Porcentaje de asignación':
                                    num_contratos = int((balance_posiciones * pct_allocation) / max_contract_value)
                                else:
                                    if balance_posiciones < max_contract_value:
                                        continue
                                    else:
                                        num_contratos = int(fixed_amount / max_contract_value)
                                
                                cost_trade = max_contract_value * num_contratos
                                if cost_trade > balance_posiciones or num_contratos == 0:
                                    continue

                                balance_posiciones -= cost_trade
                                
                                posiciones_abiertas.append({
                                    'num_contratos': num_contratos,
                                    'option_open_price': option_open_price,
                                    'option_name': option_name,
                                    'df_option_cierre': df_option_cierre,
                                    'precio_usar_cierre': precio_usar_cierre,
                                    'index': index,
                                    'cost_trade': cost_trade,
                                    'end_time': end_time,
                                    'start_time': start_time
                                })
                                
                                trade_result = (df_option_cierre[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                                
                                resultados.append({
                                    'Fecha': start_time, 
                                    'Tipo': 'Call' if row[column_name] == 1 else 'Put',
                                    'toggle_false': row[column_name],
                                    'toggle_true': row[column_name],
                                    'Fecha Apertura': start_time,
                                    'Fecha Cierre': end_time,
                                    'Precio Entrada': option_open_price, 
                                    'Precio Salida Utilizado': df_option_cierre[precio_usar_cierre].iloc[index],
                                    'Resultado': 0,
                                    'Resultado Potencial': trade_result,
                                    'Contratos': num_contratos,
                                    'Opcion': option_name,
                                    'ROI SPY': ROI_SPY,
                                    'Open': precio_usar_apertura_excel,
                                    'Close': precio_usar_cierre_excel,
                                    'Costo Posiciones': cost_trade,
                                    'Balance Posiciones': balance_posiciones
                                })
                    
                            
        
        else: #El archivo no contiene las columnas start_time y end_time
            if periodo == 'Diario':
                señal_actual = row[column_name]
                
                if 'probability' in data.columns:
                    #print("Si tiene la columna de modelos seleccionados")
                    ensamble = True
                else:
                    #print("El archivo no tiene la columna de modelos seleccionados")
                    ensamble = False
                    
                #if 'Selected_Models' in data.columns:
                    #print("Si tiene la columna de modelos seleccionados")
                    #ensamble = True
                #else:
                    #print("El archivo no tiene la columna de modelos seleccionados")
                    #ensamble = False
                
                #if ensamble and row['Selected_Models'] == "[]":
                if ensamble and row['probability'] == 0:
                    continue
                else: #Si tiene modelos
                    
                    # Nueva estrategia cuando el checkbox está seleccionado y el periodo es 'Diario'
                    if esce1:
                        if señal_actual in [0, 1]:
                            if posicion_anterior_abierta:  #posicion_anterior_abierta = True
                                #print("Hay posiciones abiertas...")
                                #print("date actual:")
                                #print(date)
                                #Abrimos una nueva posición del día actual
                                # data_for_date = yf.download("SPY", start="2022-01-01", end=date + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                                # Reemplazo API
                                data_for_date = obtener_datos_spy_diario_api("2022-01-01", date + pd.DateOffset(days=1))
                                data_for_date = data_for_date.drop(data_for_date.index[-1])
                                #data_for_date.columns = data_for_date.columns.str.lower()
                                data_for_date.index.name = 'date'
                                #print("data_for_date")
                                #print(data_for_date)
                                print(data_for_date.columns)
                                if data_for_date.empty:
                                    continue
                                if trade_type == 'Close to Close':
                                    #print("Es close to close")
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
                                    #print(option_price)
                                    
                                #--Se recibe df_option de una vez--    
                                #option_date = encontrar_opcion_cercana(client, date, option_price, row[column_name], option_days, option_offset, ticker)
                                #option_date, actual_option_price = encontrar_strike_cercano(client, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                                #option_price = actual_option_price
                                option_date, actual_option_price, df_option = encontrar_strike_cercano_optimizado(client, api_key, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                                option_price = actual_option_price
                                if option_date:
                                    option_type = 'C' if row[column_name] == 1 else 'P'
                                    #print("option type para posición abierta:")
                                    #print(option_type)
                                    option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                                    
                                #-- Se elimina la llamada reduntante a obtener_historico --   
                                #df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
                                df_option_anterior = obtener_historico_api(option_name_anterior, date, date + timedelta(days=option_days))
                                option_open_price_opnd = df_option_anterior[precio_usar_apertura_anterior].iloc[0]
                                option_close_price_opnd = df_option_anterior[precio_usar_cierre_anterior].iloc[0]
                                #print("Precio de entrada posición abierta siguiente día:")
                                #print(option_open_price_opnd)
                                #print("Precio de entrada posición abierta siguiente día:")
                                #print(option_close_price_opnd)
                                
                                
                                    
                                
                                if señal_actual == señal_anterior: #Tenemos posibilidad de recuperar ganancia
                                    #print("Señales iguales")
                                    #print("Manteniendo señal hasta el final del día...")
                                    #print("Fecha día anterior")
                                    #print(fecha_entrada)
                                    #print("trade result día anterior")
                                    #print(trade_result_anterior)
                                    #print("option name día anterior")
                                    #print(option_name_anterior)
                                    
                                    #data_for_date_anterior = yf.download(ticker, start=fecha_inicio, end=fecha_fin + pd.DateOffset(days=1)) fecha_entrada
                                    # data_for_date_anterior = yf.download("SPY", start="2022-01-01", end=fecha_entrada + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                                    # Reemplazo API
                                    data_for_date_anterior = obtener_datos_spy_diario_api("2022-01-01", fecha_entrada + pd.DateOffset(days=1))
                                    data_for_date_anterior = data_for_date_anterior.drop(data_for_date_anterior.index[-1])
                                    data_for_date_anterior.columns = data_for_date_anterior.columns.str.lower()
                                    data_for_date_anterior.index.name = 'date'
                                    print(data_for_date_anterior.columns)
                                    if not data_for_date_anterior.empty:
                                        etf_open_price_anterior = data_for_date_anterior['Open'].iloc[0] if not data_for_date_anterior.empty else None
                                        etf_close_price_anterior = data_for_date_anterior['Close'].iloc[0] if not data_for_date_anterior.empty else None
                                        #print("Precio del open de ayer")
                                        #print(etf_open_price_anterior)
                                        #print("Precio del close de ayer")
                                        #print(etf_close_price_anterior)
                                    
                                    if not data_for_date.empty:
                                        etf_close_price = data_for_date['Close'].iloc[0] if not data_for_date.empty else None
                                        etf_open_price = data_for_date['Open'].iloc[0] if not data_for_date.empty else None
                                        #print("Precio del open de hoy")
                                        #print(etf_open_price)
                                        #print("Precio del close de hoy")
                                        #print(etf_close_price)
                                        
                                        
                                    if not df_option.empty:
                                        option_close_price = df_option[precio_usar_cierre].iloc[index]
                                    trade_result_anterior = (option_close_price_opnd - precio_entrada_anterior) * 100 * num_contratos_anterior
                                    #print("Nuevo trade result anterior calculado:")
                                    #print(trade_result_anterior)
                                    
                                    
                                    
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
                                        'ROI SPY': ROI_SPY,
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
                                    #print("Señales no iguales")
                                    #print("Cerrando posición...")
                                    #print(date)
                                    #print("Fecha día anterior")
                                    #print(fecha_entrada)
                                    #print("trade result día anterior")
                                    #print(trade_result_anterior)
                                    #print("option name día anterior")
                                    #print(option_name_anterior)
                                    #print("Precio usar cierre anterior:")
                                    #print(precio_usar_cierre_anterior)
                                    
                                    #data_for_date_anterior = yf.download(ticker, start=fecha_inicio, end=fecha_fin + pd.DateOffset(days=1)) fecha_entrada
                                    # data_for_date_anterior = yf.download("SPY", start="2022-01-01", end=fecha_entrada + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                                    # Reemplazo API
                                    data_for_date_anterior = obtener_datos_spy_diario_api("2022-01-01", fecha_entrada + pd.DateOffset(days=1))
                                    data_for_date_anterior = data_for_date_anterior.drop(data_for_date_anterior.index[-1])
                                    data_for_date_anterior.columns = data_for_date_anterior.columns.str.lower()
                                    data_for_date_anterior.index.name = 'date'
                                    
                                    
                                    if not data_for_date_anterior.empty:
                                        etf_open_price_anterior = data_for_date_anterior['Open'].iloc[0] if not data_for_date_anterior.empty else None
                                        etf_close_price_anterior = data_for_date_anterior['Close'].iloc[0] if not data_for_date_anterior.empty else None
                                        #print("Precio del open de ayer")
                                        #print(etf_open_price_anterior)
                                        #print("Precio del close de ayer")
                                        #print(etf_close_price_anterior)
                                    
                                    if not data_for_date.empty:
                                        etf_open_price = data_for_date['Open'].iloc[0] if not data_for_date.empty else None
                                        etf_close_price = data_for_date['Close'].iloc[0] if not data_for_date.empty else None
                                        #print("Precio del open de hoy")
                                        #print(etf_open_price)
                                        #print("Precio del close de hoy")
                                        #print(etf_close_price)
                                    
                                    
                                    if not df_option.empty:
                                        option_open_price = df_option[precio_usar_apertura].iloc[0]
                                        option_close_price = df_option[precio_usar_cierre].iloc[index]
                                        
                                    trade_result_anterior = (option_open_price_opnd - precio_entrada_anterior) * 100 * num_contratos_anterior
                                    #print("Nuevo trade result anterior calculado:")
                                    #print(trade_result_anterior)
                                    
                                    
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
                                        'ROI SPY': ROI_SPY,
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
                                        # etf_data = yf.download("SPY", start="2022-01-01", end=date + pd.Timedelta(days=1), multi_level_index=False, auto_adjust=False)
                                        # Reemplazo API
                                        etf_data = obtener_datos_spy_diario_api("2022-01-01", date + pd.Timedelta(days=1))
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
                                            'ROI SPY': ROI_SPY,
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
                                        #print(fecha_entrada)
                                        #print(precio_entrada_anterior)
                                        #print(num_contratos_anterior)
                                        #print(trade_result_anterior)
                                        
                                        #print(option_name_anterior)
                                        # No registramos el resultado aún
                                        # Guardamos la señal actual para la siguiente iteración
                                        señal_anterior = señal_actual
                                        
                            else: #posicion_anterior_abierta = False
                                #print("No hay posiciones abiertas para la fecha de:")
                                #print(date)
                                #Abrimos una nueva posición
                                # data_for_date = yf.download("SPY", start="2022-01-01", end=date + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                                # Reemplazo API
                                data_for_date = obtener_datos_spy_diario_api("2022-01-01", date + pd.DateOffset(days=1))
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
                                    #print(option_price)
                                    
                                    
                                #option_date = encontrar_opcion_cercana(client, date, option_price, row[column_name], option_days, option_offset, ticker)
                                #option_date, actual_option_price = encontrar_strike_cercano(client, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                                option_date, actual_option_price, df_option = encontrar_strike_cercano_optimizado(
                                    client, api_key, date, option_price, row[column_name], 
                                    option_days, option_offset, ticker, method, offset
                                )
                                option_price = actual_option_price
                                if option_date:
                                    option_type = 'C' if row[column_name] == 1 else 'P'
                                    option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                                    
                                    # 🟢 OPTIMIZACIÓN: Eliminamos la llamada redundante a obtener_historico
                                    # Ya tenemos df_option desde encontrar_strike_cercano_optimizado
                                    # df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
                                    
                                    if not df_option.empty:
                                        posicion_actual_abierta = True
                                        option_open_price = df_option[precio_usar_apertura].iloc[0]
                                        #print("Precio de entrada para la opción día actual:")
                                        #print(option_open_price)
                                        option_close_price = df_option[precio_usar_cierre].iloc[index]
                                        #print("Precio de salida opción día actual:")
                                        #print(option_close_price)
                                        max_contract_value = option_open_price * 100
                                        
                                        if allocation_type == 'Porcentaje de asignación':
                                            num_contratos = int((balance * pct_allocation) / max_contract_value)
                                        else: #allocation_type == 'Monto fijo de inversión':
                                            if balance < max_contract_value:
                                                st.error("No hay suficiente dinero para abrir más posiciones. La ejecución del tester ha terminado.")
                                                return pd.DataFrame(resultados), balance
                                            else: #balance >= max_contract_value
                                                num_contratos = int(fixed_amount / max_contract_value)
                                        
                                        #print("Numero de contratos día actual:")
                                        #print(num_contratos)
                                        #print("Option Type actual:")
                                        #print(option_type)
                                        trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                                        if trade_result >= 0:
                                            balance += trade_result
                                            #print("trade result actual positivo:")
                                            #print(trade_result)
                                            # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                                            # etf_data = yf.download("SPY", start="2022-01-01", end=date + pd.Timedelta(days=1), multi_level_index=False, auto_adjust=False)
                                            # Reemplazo API
                                            etf_data = obtener_datos_spy_diario_api("2022-01-01", date + pd.Timedelta(days=1))
                                            etf_data = etf_data.drop(etf_data.index[-1])
                                            etf_data.columns = etf_data.columns.str.lower()
                                            etf_data.index.name = 'date'
                                            etf_open_price = etf_data['Open'].iloc[0] if not etf_data.empty else None
                                            #print("Precio de entrada día actual:")
                                            #print(etf_open_price)
                                            etf_close_price = etf_data['Close'].iloc[0] if not etf_data.empty else None
                                            #print("Precio salida día actual:")
                                            #print(etf_close_price)
                                            
                                            
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
                                                'ROI SPY': ROI_SPY,
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
                                            print("trade result negativo que se convertirá en mi anterior:")
                                            print(trade_result_anterior)
                                            precio_usar_cierre_anterior = precio_usar_cierre
                                            precio_usar_apertura_anterior = precio_usar_apertura
                                            #option_open_price_opnd = option_open_price
                                            fecha_entrada = date
                                            #print(fecha_entrada)
                                            #print(precio_entrada_anterior)
                                            #print(num_contratos_anterior)
                                            #print(trade_result_anterior)
                                            #print(option_name_anterior)
                                            #print(precio_usar_cierre_anterior)
                                            # No registramos el resultado aún
                                            # Guardamos la señal actual para la siguiente iteración
                                            señal_anterior = señal_actual
                        else:
                            continue
                        
                    else: #esce1 = False
                        # data_for_date = yf.download("SPY", start=date, end=date + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                        # Reemplazo API
                        data_for_date = obtener_datos_spy_diario_api(date, date + pd.DateOffset(days=1))
                        print("B")
                        #print("datos con data_for_date (yahoo finance)")
                        #print(data_for_date)
                        #data_for_date2 = get_open_and_close(ticker, api_av, fecha_inicio, fecha_fin)
                        #print("datos con get_open_and_close (alpha vantage")
                        #print(data_for_date2)
                        #df_option = obtener_historico_15min(option_name, api_key, date, date + timedelta(days=option_days))
                        #print("datos con obtener_historico_15min")
                        #print(df_option)
                        #data_for_date_fm = get_spy_intraday_financial_modeling(fecha_inicio, fecha_fin)
                        #print("datos con get_spy_intraday_financial_modeling")
                        #print(data_for_date_fm)
                        #data_for_date3 = open_close(ticker, api_key, fecha_inicio, fecha_fin)
                        
                        #data_for_date = data_for_date.drop(data_for_date.index[-1])
                        #data_for_date.columns = data_for_date.columns.str.lower()
                        data_for_date.index.name = 'date'
                        #data_for_date3.index.name = 'date'
                        #datee = pd.to_datetime(date)
                        #datee = pd.to_datetime(datee)
                        # Reemplazar la hora, minuto y segundo
                        #datee = datee.replace(hour=9, minute=35, second=0)
                        #print("datee")
                        #print(datee)
                        #print("date.index")
                        #print(data_for_date3.index[64])
                        #data_for_date3 = data_for_date3[data_for_date3.index >= datee]
                        #print("datos con data_for_date3")
                        #print(data_for_date3)
                        #print("datos eliminando ultimo index")
                        #print(data_for_date)
                        #print(data_for_date.columns)
                        if data_for_date.empty:
                            print("data_for_date empty")
                            continue
                        #if data_for_date3.empty:
                            #continue
                        if trade_type == 'Close to Close':
                            #print("Es close to close")
                            precio_usar_apertura = 'close'
                            precio_usar_cierre = 'close'
                            index = 1
                            option_price = round(data_for_date['Close'].iloc[0])
                            #print("option price 1:")
                            #print(option_price)
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
                            #print(option_price)
                        #option_date = encontrar_opcion_cercana(client, date, option_price, row[column_name], option_days, option_offset, ticker)
                        # option_date, actual_option_price = encontrar_strike_cercano(client, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)                        
                        option_date, actual_option_price, df_option = encontrar_strike_cercano_optimizado(client, api_key, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                        option_price = actual_option_price 
                        #print("Option_date:")
                        #print(option_date)
                        #print("option price 2:") 
                        #print(option_price)
                        if option_date:
                            option_type = 'C' if row[column_name] == 1 else 'P'
                            option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                            # df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
                            #print("df_option:")
                            #print(df_option)
                            if not df_option.empty:
                                option_open_price = df_option[precio_usar_apertura].iloc[0]
                                option_close_price = df_option[precio_usar_cierre].iloc[index]
                                #print("option open price:")
                                #print(option_open_price)
                                #print("option close price:")
                                #print(option_close_price)
                                max_contract_value = option_open_price * 100
                                #print("max_contract_value")
                                #print(max_contract_value)
                                if allocation_type == 'Porcentaje de asignación':
                                    num_contratos = int((balance * pct_allocation) / max_contract_value)
                                    #print("Número de contratos:")
                                    #print(num_contratos)
                                else: #allocation_type == 'Monto fijo de inversión':
                                    if balance < max_contract_value:
                                        st.error("No hay suficiente dinero para abrir más posiciones. La ejecución del tester ha terminado.")
                                        return pd.DataFrame(resultados), balance
                                    else: #balance >= max_contract_value
                                        num_contratos = int(fixed_amount / max_contract_value)
                                trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                                #print("trade result:")
                                #print(trade_result)
                                balance += trade_result
                                #print("Balance:")
                                #print(balance)
                                
                                # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                                # etf_data = yf.download("SPY", start=date, end=date + pd.Timedelta(days=1), multi_level_index=False, auto_adjust=False)
                                # Reemplazo API
                                etf_data = obtener_datos_spy_diario_api(date, date + pd.Timedelta(days=1))
                                #print("datos sin eliminar ultimo index-etf")
                                #print(etf_data)
                                #etf_data = etf_data.drop(etf_data.index[-1])
                                #etf_data.columns = etf_data.columns.str.lower()
                                etf_data.index.name = 'date'
                                #print("datos eliminando ultimo index")
                                #print(data_for_date)
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
                                    'ROI SPY': ROI_SPY,
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
                #print("Fecha date:",date)
                #print("Fecha inicio:",fecha_inicio)
                #print("Fecha fin:",fecha_fin)
                data_for_date2 = get_open_and_close(ticker, api_av, fecha_inicio, fecha_fin)
                #data_for_date3 = open_close(ticker, api_key, fecha_inicio, fecha_fin)
                #data_for_date4 = mostrar_datos()
                #data_for_date_fm = get_spy_intraday_financial_modeling(fecha_inicio, fecha_fin)
                #print(start)
                #print(data_for_date)
                #print ("dataframe fm")
                #print(data_for_date_fm)
                #print ("función open_close (Polygon)")
                #print(data_for_date3)
                #print ("función mostrar datos globales")
                #print(data_for_date4)
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
                    #option_price= round(data_for_date4.loc[date]['open'])
                elif trade_type == 'Close to Open':
                    precio_usar_apertura = 'close'
                    precio_usar_cierre = 'open'
                    index = 1               
                    #option_price = round(data_for_date2.loc[date]['close'])
                    option_price2= round(data_for_date2.loc[date]['open'])
                    #option_price= round(data_for_date4.loc[date]['open'])
                else: #Open to Close
                    precio_usar_apertura = 'open'
                    precio_usar_cierre = 'close'
                    index = 0                
                    #option_price2 = round(data_for_date['Open'].iloc[0])
                    #option_price2= round(data_for_date2.loc[date]['open'])
                    #option_price= round(data_for_date4.loc[date]['open'])
                    #print(option_price)
            
                #option_date = encontrar_opcion_cercana_15min(client, date, option_price, row[column_name], option_days, option_offset, ticker)
                option_date, actual_option_price = encontrar_strike_cercano(client, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                option_price = actual_option_price
                if option_date:
                    option_type = 'C' if row[column_name] == 1 else 'P'
                    option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                    #option_name2 = f'O:{ticker}{option_date2}{option_type}00{option_price2}000'
                    #print(option_name)
                    #print("option date2 - Diario")
                    #print(option_date2)
                    #print("option date - 15 min")
                    #print(option_date)
                #print(date)
                #print(timedelta(days=option_days))
                #print(date + timedelta(days=option_days))
                #df_option = obtener_historico_15min(option_name, api_key, date, date + timedelta(days=option_days))
                #df_option2 = obtener_historico_15min_pol(option_name, api_key, date, date + timedelta(days=option_days))
                df = get_open_and_close(ticker, api_av, fecha_inicio, fecha_fin)
                #df_glo = mostrar_datos()
                #df_option2 = obtener_historico_15min_pol(ticker, api_key, date, date + timedelta(days=option_days))
                #vo = verificar_opcion_15min(client, ticker, date, date + timedelta(days=option_days))
                #vo = verificar_opcion_15min(client, ticker, fecha_inicio, fecha_fin)
                #df_option2 = obtener_historico_15min_pol(option_name, api_key, date, date + timedelta(days=option_days))
                #df2 = obtener_historico_15min_pol(option_name, api_key, date, date + timedelta(days=option_days))
                #print("df_option:")
                #st.dataframe(df_option)
                #print("función get_open_and_close:")
                #st.dataframe(df)
                #print("df_option2:")
                #st.dataframe(df_option2)
                #print("verificar opción:")
                #print(vo)
                #print("Respuesta JSON completa:", data)  # También se muestra en Streamlit
                if not df_option.empty:   
                    #print("Entró por acá")
                    option_open_price = df_option['open'].iloc[0]
                    #print(open_hour)
                    #print(close_hour)
                    #print(option_open_price)
                    #print(df_option[precio_usar_cierre].iloc[index])
                    #print(df_option.iloc[0])
                    #print(df_option.iloc[-1])
                    #print(df_option)
                    
                    #print(df_option[precio_usar_cierre].iloc[index])
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
                        #if not data_for_date_fm.empty:
                        #if not df_option2.empty:
                            #etf_open_price = df_option2.at[date, 'open']
                            #etf_close_price = df_option2.at[date, 'close']
                            #etf_open_price= data_for_date_fm.at[date, 'open']
                            #etf_close_price= data_for_date_fm.at[date, 'close']
                            #etf_open_price2= data_for_date2.at[date, 'open']
                            #etf_close_price2= data_for_date2.at[date, 'close']
                            #etf_open_price3= data_for_date4.at[date, 'open']
                            #etf_close_price3= data_for_date4.at[date, 'close']
                        #else:
                            #etf_open_price = df.at[date, 'open']
                            #etf_close_price = df.at[date, 'close']
                        #etf_open_price = df_option2.at[date, 'open']
                        #print(df_option2.at[date, 'open'])
                        #print(df.at[date, 'open'])
    
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
                        'ROI SPY': ROI_SPY,
                        'Open': etf_open_price,
                        'Close': etf_close_price,
                        #'Open2': etf_open_price3,
                        #'Close2': etf_close_price3
                    })
                    print(trade_result)
    # Cerrar solo las posiciones cuyo end_time ya pasó según fecha_fin
    for pos in posiciones_abiertas:
        # Comparar end_time de la posición con fecha_fin del backtest
        if pos['end_time'] <= pd.Timestamp(fecha_fin):
            
            if contratos_especificos and "OptionName" in data.columns:
                # Esta posición SÍ debió cerrarse porque su end_time ya pasó
                trade_result_pos = (pos['df_option_cierre'][pos['precio_usar_cierre']] 
               - pos['option_open_price']) * 100 * pos['num_contratos']
            else:                       
                trade_result_pos = (pos['df_option_cierre'][pos['precio_usar_cierre']].iloc[pos['index']] 
                                   - pos['option_open_price']) * 100 * pos['num_contratos']
                       
            balance += trade_result_pos
            
            # Actualizar el resultado en resultados
            for resultado in resultados:
                if (resultado['Fecha Apertura'] == pos['start_time'] and 
                    resultado['Opcion'] == pos['option_name']):
                    resultado['Resultado'] = trade_result_pos
                    break

    resultados_df = pd.DataFrame(resultados)
    if not resultados_df.empty and 'Resultado' in resultados_df.columns:
        #graficar_resultados(resultados_df, balance, balance_inicial)
        resultados_df.to_excel('resultados_trades_GMO_contratos_bandera.xlsx')
    else:
        print("Failla de resultados validos")
        st.error("No se encontraron resultados válidos para el periodo especificado.")
        pass
    return resultados_df, balance

# Modificación en la definición de la función para aceptar 'spy_full_data'
def graficar_resultados(df, final_balance, balance_inicial, spy_full_data=None):
    if df.empty or 'Resultado' not in df.columns:
        st.error("No se pueden graficar resultados porque el DataFrame está vacío o falta la columna 'Resultado'.")
        return
    
    plt.figure(figsize=(14, 7))
    
    # --- Gráfica de Ganancias (Eje Izquierdo) ---
    # Convertimos 'Fecha' a datetime para asegurar compatibilidad con el índice de spy_full_data
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    df = df.sort_values('Fecha') # Asegurar orden cronológico
    
    df['Ganancia acumulada'] = df['Resultado'].cumsum() + balance_inicial
    
    # Graficamos la curva de equidad
    ax = plt.gca() # Obtener eje actual
    ax.plot(df['Fecha'], df['Ganancia acumulada'], marker='o', linestyle='-', color='b', label='Ganancia Acumulada')
    
    ax.set_title(f'Resultados del Backtesting de Opciones - Balance final: ${final_balance:,.2f}')
    ax.set_xlabel('Fecha')
    ax.set_ylabel('Ganancia/Pérdida Acumulada', color='b')
    ax.tick_params(axis='y', labelcolor='b')
    
    # Ajuste de fechas en eje X
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.xticks(rotation=45)

    ax.axhline(y=balance_inicial, color='r', linestyle='--', label='Balance Inicial')
    
    # --- Gráfica del SPY (Eje Derecho - Línea Amarilla/Naranja) ---
    ax2 = ax.twinx()
    
    # Lógica condicional: Si tenemos datos completos, los usamos. Si no, usamos los datos del trade (método antiguo)
    if spy_full_data is not None and not spy_full_data.empty:
        # Aquí graficamos TODOS los días del rango, no solo los trades
        ax2.plot(spy_full_data.index, spy_full_data['Close'], color='orange', linestyle='-', alpha=0.6, label='Precio del S&P (Close)')
    else:
        # Fallback al método anterior si no se pasan datos
        ax2.plot(df['Fecha'], df['Close'], color='orange', linestyle='-', label='Precio del S&P (Close - Solo Trades)')
        
    ax2.set_ylabel('Precio del S&P (Close)', color='orange')
    ax2.tick_params(axis='y', labelcolor='orange')
    
    # Unificar leyendas
    lines_1, labels_1 = ax.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')
    
    plt.grid(True, which='both', linestyle='-', linewidth=0.5, alpha=0.3)
    plt.tight_layout()
    plt.savefig('resultados_backtesting.png')
    fig = plt.gcf()
    st.pyplot(fig)
    plt.close()

def main():
    st.title("Backtesting ARKAD")

    # --- ESTILOS CSS ---
    tooltip_style = """
    <style>
    .tooltip {
        position: relative;
        display: inline-block;
        cursor: pointer;
        color: #3498db;
        float: left;
        margin-left: 5px;
        vertical-align: middle;
    }
    .tooltip .tooltiptext {
        visibility: hidden;
        width: 220px;
        background-color: black;
        color: #fff;
        text-align: left;
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
    st.markdown(tooltip_style, unsafe_allow_html=True)

    # --- CARGA DE ARCHIVOS ---
    directorio_datos = '.'
    archivos_disponibles = [archivo for archivo in os.listdir(directorio_datos) if archivo.endswith('.xlsx')]

    def extract_file_info(filename):
        default_values = ("Operación desconocida", "Modelo desconocido", "Responsable desconocido",
                          "Fecha de inicio desconocida", "Fecha de fin desconocida")
        parts = filename.split('_')
        if parts:
            parts[-1] = parts[-1].split('.')[0]
        padded_parts = parts + [None] * (5 - len(parts))
        try:
            operation = {'CC': 'Close to Close', 'OC': 'Open to Close', 'CO': 'Close to Open'}.get(
                padded_parts[0], default_values[0]) if padded_parts[0] else default_values[0]
            model_name = padded_parts[1] if padded_parts[1] else default_values[1]
            responsible = {'Valen': 'Valentina', 'Santi': 'Santiago', 'Andres': 'Andrés', 'Mateo': 'Mateo'}.get(
                padded_parts[2], default_values[2]) if padded_parts[2] else default_values[2]
            if padded_parts[3] and len(padded_parts[3]) >= 6:
                start_date = f"{padded_parts[3][0:2]}/{padded_parts[3][2:4]}/{padded_parts[3][4:6]}"
            else:
                start_date = default_values[3]
            if padded_parts[4] and len(padded_parts[4]) >= 6:
                end_date = f"{padded_parts[4][0:2]}/{padded_parts[4][2:4]}/{padded_parts[4][4:6]}"
            else:
                end_date = default_values[4]
            return operation, model_name, responsible, start_date, end_date
        except Exception:
            return default_values

    info_placeholder = st.empty()
    # -------------------------------------------------------------------------

    column_name = 'toggle_false'
    data_filepath = st.selectbox("*Seleccionar archivo de datos históricos:*", archivos_disponibles, key='select_archivo_historico')

    # --- VALIDACIÓN DE COLUMNAS ---
    if data_filepath:
        data = cargar_datos(data_filepath)
        if data is None:
            st.error("🚨 El archivo seleccionado no tiene una columna 'date'. Selecciona un archivo de insumo válido.")
            st.stop()
        
        if "OptionName" in data.columns:
            st.info("📋 Archivo con contratos específicos detectado.")
        else:
            st.info("📋 Archivo estándar detectado.")

    if data_filepath:
        operation, model_name, responsible, start_date, end_date = extract_file_info(data_filepath)
        if operation.endswith("desconocida"):
            tooltip_text = "<div class='tooltip'>&#9432; <span class='tooltiptext'>Error al decodificar el nombre del archivo. Verifique el formato.</span></div>"
        else:
            tooltip_text = f"""
            <div class="tooltip">
                &#9432;
                <span class="tooltiptext">
                Tipo de operación: {operation}<br>
                Nombre del Modelo: {model_name}<br>
                Responsable: {responsible}<br>
                Fechas: {start_date} - {end_date}
                </span>
            </div>
            """
        info_placeholder.markdown(tooltip_text, unsafe_allow_html=True)

    # --- INPUTS DE USUARIO ---
    option_days_input = st.number_input("*Option Days:*", min_value=0, max_value=90, value=30, step=1)
    option_offset_input = st.number_input("*Option Offset:*", min_value=0, max_value=90, value=7, step=1)
    balance_inicial = st.number_input("*Balance inicial*", min_value=0, value=100000, step=1000)

    allocation_type = st.radio("Seleccionar tipo de asignación de capital:", ('Porcentaje de asignación', 'Monto fijo de inversión'))
    if allocation_type == 'Porcentaje de asignación':
        pct_allocation = st.number_input("*Porcentaje de Asignación de Capital:*", min_value=0.001, max_value=0.6, value=0.05)
        fixed_amount = None
    else:
        fixed_amount = st.number_input("*Monto fijo de inversión:*", min_value=0.0, max_value=float(balance_inicial), value=1000.0, step=1000.0)
        pct_allocation = None

    periodo = st.radio("*Seleccionar periodo de datos*", ('Diario', '15 minutos'))

    if periodo == 'Diario':
        col1, col2 = st.columns([1, 1])
        with col1:
            esce1 = st.checkbox("Aplicar estrategia para manejo de pérdida de ganancias")
        with col2:
            st.markdown("""
            <div class="tooltip" style="display: inline;">
                &#9432;
                <span class="tooltiptext">Estrategia de recuperación de pérdidas intradía.</span>
            </div>
            """, unsafe_allow_html=True)
    else:
        esce1 = False

    fecha_inicio = st.date_input("*Fecha de inicio:*", min_value=datetime(2005, 1, 1))
    fecha_fin = st.date_input("*Fecha de fin:*", max_value=datetime.today())
    method = st.radio("*Seleccionar Strikes a Considerar*", ('ATM', 'OTM'))

    if method == "OTM":
        offset = st.number_input("*Strikes a desplazarse*", min_value=0, value=5, step=1)
    else:
        offset = 0

    trade_type = st.radio('*Tipo de Operación*', ('Open to Close', 'Close to Close', 'Close to Open'))

    # --- EJECUCIÓN DEL BACKTEST ---
    if st.button("Run Backtest"):
        # 1. Ejecutar Lógica Principal
        resultados_df, final_balance = realizar_backtest(
            data_filepath, 'rlD0rjy9q_pT4Pv2UBzYlXl6SY5Wj7UT', "SPY",
            balance_inicial, pct_allocation, fixed_amount, allocation_type,
            pd.Timestamp(fecha_inicio), pd.Timestamp(fecha_fin),
            option_days_input, option_offset_input, trade_type,
            periodo, column_name, method, offset, esce1
        )

        st.success("Backtest ejecutado correctamente!")

        # 2. Descargar Histórico Completo SPY (CAMBIO #4: usa API del VPS en lugar de yfinance)
        st.write("Obteniendo datos completos del SPY para graficar...")
        try:
            spy_full_data = obtener_datos_spy_diario_api(fecha_inicio, fecha_fin + timedelta(days=1))
        except Exception as e:
            st.warning(f"No se pudo obtener el histórico completo del SPY: {e}")
            spy_full_data = None

        # 3. PROCESAMIENTO DE MÉTRICAS
        resultados_df['Fecha'] = pd.to_datetime(resultados_df['Fecha'])
        resultados_df = resultados_df[
            (resultados_df['Fecha'] >= pd.Timestamp(fecha_inicio)) &
            (resultados_df['Fecha'] <= pd.Timestamp(fecha_fin))
        ]
        resultados_df = resultados_df.sort_values('Fecha').reset_index(drop=True)
        resultados_df['Ganancia acumulada'] = resultados_df['Resultado'].cumsum() + balance_inicial

        if trade_type == 'Close to Close':
            resultados_df['Direction'] = (resultados_df['Close'].shift(-1) > resultados_df['Close']).astype(int)
        elif trade_type == 'Close to Open':
            resultados_df['Direction'] = (resultados_df['Close'] < resultados_df['Open'].shift(-1)).astype(int)
        elif trade_type == 'Open to Close':
            resultados_df['Direction'] = (resultados_df['Open'] < resultados_df['Close']).astype(int)
        else:
            resultados_df['Direction'] = 0

        resultados_df['acierto'] = np.where(resultados_df['Direction'] == resultados_df[column_name], 1, 0)
        resultados_df['asertividad'] = resultados_df['acierto'].sum() / len(resultados_df['acierto']) if len(resultados_df['acierto']) > 0 else 0
        resultados_df['cumsum'] = resultados_df['acierto'].cumsum()
        resultados_df['accu'] = resultados_df['cumsum'] / (resultados_df.index + 1)

        if trade_type == 'Open to Close':
            resultados_df['open_to_close_pct'] = resultados_df['Close'] / resultados_df['Open'] - 1
            resultados_df['Ganancia'] = resultados_df.apply(lambda row: abs(row['open_to_close_pct']) if row['acierto'] else -abs(row['open_to_close_pct']), axis=1)
        elif trade_type == 'Close to Close':
            resultados_df['close_to_close_pct'] = resultados_df['Close'].shift(-1) / resultados_df['Close'] - 1
            resultados_df['Ganancia'] = resultados_df.apply(lambda row: abs(row['close_to_close_pct']) if row['acierto'] else -abs(row['close_to_close_pct']), axis=1)
        else:
            resultados_df['close_to_open_pct'] = resultados_df['Open'].shift(-1) / resultados_df['Close'] - 1
            resultados_df['Ganancia'] = resultados_df.apply(lambda row: abs(row['close_to_open_pct']) if row['acierto'] else -abs(row['close_to_open_pct']), axis=1)

        resultados_df['Ganancia_Acumulada'] = resultados_df['Ganancia'].cumsum()

        # Matriz de Confusión
        matrix = np.zeros((2, 2))
        for i in range(len(resultados_df)):
            try:
                if int(resultados_df[column_name][i]) == 1 and int(resultados_df['Direction'][i]) == 1:   matrix[0, 0] += 1
                elif int(resultados_df[column_name][i]) == 1 and int(resultados_df['Direction'][i]) == 0: matrix[0, 1] += 1
                elif int(resultados_df[column_name][i]) == 0 and int(resultados_df['Direction'][i]) == 1: matrix[1, 0] += 1
                elif int(resultados_df[column_name][i]) == 0 and int(resultados_df['Direction'][i]) == 0: matrix[1, 1] += 1
            except:
                pass

        tp, fp, fn, tn = matrix.ravel()
        resultados_df['tp'] = tp; resultados_df['tn'] = tn
        resultados_df['fp'] = fp; resultados_df['fn'] = fn

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        resultados_df['precision'] = precision
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        resultados_df['recall'] = recall
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        resultados_df['f1_score'] = f1_score

        st.session_state['resultados_df'] = resultados_df
        st.session_state['final_balance'] = final_balance
        st.session_state['balance_inicial'] = balance_inicial

        # 4. DESCARGAS
        st.write("### Descargar Resultados")
        excel_buffer = io.BytesIO()
        resultados_df.to_excel(excel_buffer, index=False)
        st.download_button(
            label="Descargar Resultados Excel",
            data=excel_buffer,
            file_name="resultados_trades_1.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # 5. GRÁFICO (CAMBIO #5: st.pyplot con figura explícita)
        st.write("### Gráfico")
        fig, ax = plt.subplots(figsize=(14, 7))

        ax.plot(resultados_df['Fecha'], resultados_df['Ganancia acumulada'], marker='o', linestyle='-', color='b', label='Ganancia Acumulada')
        ax.set_title(f'Resultados del Backtesting - Balance final: ${final_balance:,.2f}')
        ax.set_xlabel('Fecha')
        ax.set_ylabel('Ganancia/Pérdida Acumulada', color='b')
        ax.tick_params(axis='y', labelcolor='b')
        plt.xticks(rotation=45)
        ax.axhline(y=balance_inicial, color='r', linestyle='-', label='Balance Inicial')

        ax2 = ax.twinx()
        if spy_full_data is not None and not spy_full_data.empty:
            ax2.plot(spy_full_data.index, spy_full_data['Close'], color='orange', linestyle='-', alpha=0.6, label='SPY (Continuo)')
        else:
            ax2.plot(resultados_df['Fecha'], resultados_df['Close'], color='orange', linestyle='-', label='SPY (Solo Trades)')

        ax2.set_ylabel('Precio del S&P (Close)', color='orange')
        ax2.tick_params(axis='y', labelcolor='orange')

        lines_1, labels_1 = ax.get_legend_handles_labels()
        lines_2, labels_2 = ax2.get_legend_handles_labels()
        ax.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')

        plt.grid(True, which='both', linestyle='-', linewidth=0.5)
        plt.tight_layout()

        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png')
        st.pyplot(fig)  # ✅ figura explícita, no plt global
        plt.close(fig)
        st.download_button(label="Descargar Gráfico", data=img_buffer, file_name="resultados_backtesting.png", mime="image/png")

        # 6. ZIP
        with zipfile.ZipFile("resultados.zip", "w") as zf:
            zf.writestr("resultados_trades_1.xlsx", excel_buffer.getvalue())
            zf.writestr("resultados_backtesting.png", img_buffer.getvalue())

        with open("resultados.zip", "rb") as f:
            st.download_button(
                label="Descargar Resultados ZIP",
                data=f,
                file_name="resultados.zip",
                mime="application/zip"
            )

# Bloque de ejecución principal
if __name__ == "__main__":
    # La función main() de Streamlit ha sido reemplazada por ejecutar_backtest_local()
    main()