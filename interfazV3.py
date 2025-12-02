import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from polygon import RESTClient
from datetime import timedelta, date
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
import pyodbc

# Variables globales para almacenar datos1 y datos2
datos1 = None
datos2 = None

# --- NUEVAS VARIABLES DE CONEXIÓN A AZURE SQL (MODIFICAR CON TUS DATOS REALES) ---
AZURE_SQL_DRIVER = '{ODBC Driver 17 for SQL Server}'
AZURE_SQL_SERVER = 'moneylabsql.database.windows.net'  # Reemplazar
AZURE_SQL_DATABASE = 'BDmoneylab'                  # Reemplazar
AZURE_SQL_USERNAME = 'adminmoneylab'                       # Reemplazar
AZURE_SQL_PASSWORD = 'Moneylab1234'                       # Reemplazar

# Objeto de conexión que se usará globalmente si se activa el checkbox
sql_connection = None

def establecer_conexion_sql():
    """Establece la conexión a Azure SQL Database usando pyodbc."""
    global sql_connection
    try:
        connection_string = f'DRIVER={AZURE_SQL_DRIVER};SERVER={AZURE_SQL_SERVER};DATABASE={AZURE_SQL_DATABASE};UID={AZURE_SQL_USERNAME};PWD={AZURE_SQL_PASSWORD}'
        
        # Intentar conectar
        conn = pyodbc.connect(connection_string)
        sql_connection = conn
        #st.success("✅ Conexión a Azure SQL Database establecida correctamente.")
        return True
    except Exception as e:
        st.error(f"❌ Error al conectar a la Base de Datos: {e}")
        st.info("El backtesting continuará usando la API de Polygon.io.")
        sql_connection = None
        return False

def obtener_precios_sql(option_name: str, start_time: pd.Timestamp, end_time: pd.Timestamp) -> pd.DataFrame:
    """
    Obtiene los precios OHLCV de un contrato específico desde Azure SQL Database.
    
    MODIFICADO: Aplica redondeo, usa coincidencia estricta y busca el Timestamp más cercano 
    si no hay coincidencia exacta en el rango.
    """
    global sql_connection
    if sql_connection is None:
        st.error("No hay conexión a la base de datos SQL disponible.")
        return pd.DataFrame()

    # 1. Normalizar y Formatear los Timestamps para Coincidencia Exacta (YYYY-MM-DD HH:MM:SS)
    #    Esto es crucial para tu BD DATETIME2(0).
    start_time_rounded = start_time.tz_localize(None).round('s') if start_time.tzinfo else start_time.round('s')
    end_time_rounded = end_time.tz_localize(None).round('s') if end_time.tzinfo else end_time.round('s')
    
    # Usaremos estos para la consulta de rango
    sql_start_time = start_time_rounded.strftime('%Y-%m-%d %H:%M:%S')
    sql_end_time = end_time_rounded.strftime('%Y-%m-%d %H:%M:%S')
    
    table_name = "OptionData2"  # Ajustar si es diferente.
    
    # La consulta SQL para filtrar por OptionName y rango de tiempo
    sql_query = f"""
    SELECT 
        [Date], [Open], [High], [Low], [Close], [Volume] 
    FROM 
        {table_name}
    WHERE 
        [OptionName] = ?
        AND [Date] >= ?
        AND [Date] <= ?
    ORDER BY 
        [Date] ASC
    """
    
    try:
        cursor = sql_connection.cursor()
        
        # 🟢 PASO A: EJECUTAR LA CONSULTA DE RANGO
        cursor.execute(sql_query, (option_name, sql_start_time, sql_end_time))
        
        data = cursor.fetchall()
        columns = [column[0] for column in cursor.description]
        df = pd.DataFrame.from_records(data, columns=columns)

        # -------------------------------------------------------------
        # 3. ¿QUÉ PASA SI NO ENCUENTRA LA OPCIÓN EXACTA? -> SALTAR TRADE
        # -------------------------------------------------------------
        if df.empty:
            # Si el DataFrame está vacío, significa que el 'OptionName' no existe 
            # o no tiene datos en el rango. Esto salta el trade (retorna DF vacío).
            st.warning(f"⚠️ Opción no encontrada o sin datos en el rango: {option_name}")
            return pd.DataFrame()
        
        # -------------------------------------------------------------
        # 4. ¿QUÉ PASA SI NO ENCUENTRA ALGUNA FECHA EXACTA? -> USAR CERCANA
        # -------------------------------------------------------------
        
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Buscar el índice más cercano al start_time_rounded
        try:
            # Encuentra la posición del índice (iloc) para el valor más cercano al inicio
            loc_start = df['Date'].searchsorted(start_time_rounded, side='left')
            # Si el 'left' excede el límite y no es la fecha exacta, ajustamos.
            if loc_start >= len(df) or df.iloc[loc_start]['Date'] != start_time_rounded:
                loc_start = max(0, loc_start - 1) # Usar el anterior si no es exacto, a menos que sea el primero.
            
            # Buscar el índice más cercano al end_time_rounded
            # Encuentra la posición del índice (iloc) para el valor más cercano al final
            loc_end = df['Date'].searchsorted(end_time_rounded, side='left')
            # Si el 'left' excede el límite y no es la fecha exacta, ajustamos al último.
            if loc_end >= len(df):
                loc_end = len(df) - 1 # Usar el último registro disponible.
                
            # Recortar el DataFrame para asegurar que empiece en la fecha encontrada más cercana
            # y termine en la fecha más cercana.
            df = df.iloc[loc_start : loc_end + 1] # +1 para incluir el índice final
            
        except Exception as e:
            # Esto captura errores raros de indexación/conversión.
            st.error(f"❌ Error al ajustar fechas: {e}")
            return pd.DataFrame()


        if df.empty:
            st.warning(f"⚠️ Rango recortado quedó vacío después de buscar fechas cercanas.")
            return pd.DataFrame()

        # Procesamiento final (limpieza y formato)
        df.set_index('Date', inplace=True)
        df.index.name = None # Limpiamos el nombre del índice
        df.columns = [col.lower() for col in df.columns] 
        
        return df

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        st.error(f"❌ Error de consulta SQL: {sqlstate}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error general al procesar datos SQL: {e}")
        return pd.DataFrame()

def obtener_precios_spy_sql_final(date: pd.Timestamp) -> tuple:
    """
    Busca los precios Open y Close en Azure SQL Database usando una coincidencia exacta 
    de Fecha y Hora, redondeada al segundo, para coincidir con DATETIME2(0).
    """
    global sql_connection
    if sql_connection is None:
        # st.error("No hay conexión a la base de datos SQL disponible.") # Puedes descomentar para debug
        return None, None

    # --- PASO 1: Normalizar el Timestamp de Python ---
    # a) Eliminar zona horaria si la tiene (quedarse en 'naive')
    date_naive = date.tz_localize(None) if date.tzinfo is not None else date
    # b) Redondear el Timestamp a nivel de SEGUNDO para eliminar nanosegundos
    date_rounded = date_naive.round('s') 
    
    # --- PASO 2: Crear la Cadena SQL exacta para la BD ---
    # Forzamos la cadena a YYYY-MM-DD HH:MM:SS (exactamente lo que tienes en tu BD)
    sql_datetime_str = date_rounded.strftime('%Y-%m-%d %H:%M:%S')
    
    table_name = "SPYhistorical" 
    
    # Consulta SQL
    sql_query = f"""
    SELECT 
        [Open], [Close] 
    FROM 
        {table_name}
    WHERE 
        [Date] = ?
    """
    
    try:
        cursor = sql_connection.cursor()
        
        # 🟢 Pasamos la CADENA DE TEXTO (sql_datetime_str) en lugar del objeto Timestamp
        # Esto funciona de forma más fiable con DATETIME2(0) en pyodbc.
        cursor.execute(sql_query, (sql_datetime_str,)) 
        
        row = cursor.fetchone()
        
        if row:
            # st.success(f"✅ Éxito al encontrar datos para: {sql_datetime_str}") # Puedes descomentar para debug
            return row[0], row[1]
        else:
            # st.warning(f"❌ SQL Fallo: No se encontró el Timestamp exacto: {sql_datetime_str}") # Puedes descomentar para debug
            return None, None

    except Exception as e:
        # st.error(f"❌ Error durante la ejecución SQL: {e}") # Puedes descomentar para debug
        return None, None


def open_close_30min(ticker, api_key, fecha_inicio, fecha_fin):
    client = RESTClient(api_key)
    local_tz = pytz.timezone('America/New_York')
    
    try:
        # --- LÍNEA MODIFICADA ---
        # Obtener datos agregados cada 30 minutos
        resp = client.get_aggs(ticker=ticker, multiplier=1, timespan="minute", 
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

def descargar_historico_completo_spy(api_key: str, fecha_inicio_str: str) -> pd.DataFrame:
    """
    Descarga el historial completo de datos minuto a minuto para el ETF SPY
    desde una fecha de inicio hasta la fecha actual.

    Esta función está diseñada para ser robusta, iterando día por día y pausando
    entre llamadas para respetar los límites de la API.

    Args:
        api_key (str): Tu clave de la API de Polygon.io.
        fecha_inicio_str (str): Fecha de inicio en formato 'YYYY-MM-DD'.

    Returns:
        pd.DataFrame: Un único DataFrame con todos los datos OHLCV desde la fecha de inicio
                      hasta hoy. Retorna un DataFrame vacío si hay un error.
    """
    # --- CORREGIDO: Usando el ticker para el ETF SPY ---
    ticker = "SPY"
    client = RESTClient(api_key)
    local_tz = pytz.timezone('America/New_York')
    
    todos_los_datos = []
    
    try:
        fecha_inicio = datetime.strptime(fecha_inicio_str, '%Y-MM-DD').date()
        fecha_fin = date.today()
        
        print(f"Iniciando descarga masiva para {ticker} desde {fecha_inicio} hasta {fecha_fin}.")
        
        fecha_actual = fecha_inicio
        while fecha_actual <= fecha_fin:
            # Solo procesar días de semana (lunes=0, domingo=6)
            if fecha_actual.weekday() < 5:
                fecha_actual_str = fecha_actual.strftime('%Y-MM-d')
                print(f"  - Obteniendo datos para el día: {fecha_actual_str}...")
                
                try:
                    resp = client.get_aggs(
                        ticker=ticker,
                        multiplier=1,
                        timespan="minute",
                        from_=fecha_actual_str,
                        to=fecha_actual_str,
                        limit=50000
                    )
                    
                    if resp:
                        datos_dia = [{
                            'fecha': pd.to_datetime(agg.timestamp, unit='ms').tz_localize('UTC').tz_convert(local_tz),
                            'open': agg.open, 'high': agg.high, 'low': agg.low,
                            'close': agg.close, 'volume': agg.volume
                        } for agg in resp]
                        todos_los_datos.extend(datos_dia)
                        print(f"    -> {len(datos_dia)} registros encontrados.")
                    else:
                        print("    -> No se encontraron datos para este día (posible feriado).")

                except Exception as e:
                    print(f"    -> ERROR al obtener datos para {fecha_actual_str}: {e}")
                
                # Pausa de seguridad para no superar el límite de la API (5 llamadas/min)
                print("    ... Pausa de 13 segundos para respetar el límite de la API.")
                time.sleep(13)
            else:
                print(f"  - Omitiendo {fecha_actual.strftime('%Y-%m-%d')} (fin de semana).")

            fecha_actual += timedelta(days=1)

        if not todos_los_datos:
            print("\nAdvertencia: No se pudo descargar ningún dato en el rango especificado.")
            return pd.DataFrame()

        # Crear el DataFrame final a partir de todos los datos recopilados
        df_completo = pd.DataFrame(todos_los_datos)
        df_completo['fecha'] = df_completo['fecha'].dt.tz_localize(None)
        df_completo.set_index('fecha', inplace=True)
        
        print(f"\n¡Descarga completada! Total de registros obtenidos: {len(df_completo)}")
        return df_completo

    except Exception as e:
        print(f"\nError fatal durante el proceso de descarga: {str(e)}")
        return pd.DataFrame()


def open_close(ticker, api_key, fecha_inicio, fecha_fin):
    global datos1, datos2
    ticker = "SPY"
    client = RESTClient(api_key)
    local_tz = pytz.timezone('America/New_York')
    i = 1
    try:
        # Obtener datos agregados cada 15 minutos
        resp = client.get_aggs(ticker=ticker, multiplier=1, timespan="minute", #VOLVER A CAMBIAR A 15 MIN
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
            
            # Verificamos si existe
            if verificar_opcion(client, option_name, base_date, base_date + timedelta(days=1)):
                # Si existe, retornamos esta fecha inmediatamente (la más cercana a la meta)
                return option_date
                
    return None

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
        establecer_conexion_sql()
        
        #st.write("El archivo contiene datos intra día")
        nombre_de_la_columna = 'start_time'
        # Se crea la columna una sola vez, antes de recorrer
        #data[f'siguiente_{nombre_de_la_columna}'] = data[nombre_de_la_columna].shift(-1)
        
        # Función para encontrar el siguiente start_time válido
        def encontrar_siguiente_start_time_valido(idx):
            if idx >= len(data) - 1:  # Si es la última fila
                return pd.NaT
            
            end_time_actual = data.iloc[idx]['end_time']
            
            # Buscar en las filas siguientes
            for j in range(idx + 1, len(data)):
                if data.iloc[j]['start_time'] >= end_time_actual:
                    return data.iloc[j]['start_time']
            
            return pd.NaT  # Si no encuentra ninguno válido
        
        # Crear la columna con los siguientes start_time válidos
        data['siguiente_start_time'] = [
            encontrar_siguiente_start_time_valido(i) 
            for i in range(len(data))
        ]

    data_for_ROI = yf.download("SPY", start=fecha_inicio, end=fecha_fin + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)     
    ROI_SPY = ((data_for_ROI['Close'].iloc[-1] - data_for_ROI['Open'].iloc[0]) / data_for_ROI['Open'].iloc[0]) * 100
    for date, row in data.iterrows():
        
        if periodo == 'Diario':
            date = date.date()
        else:
            date = pd.Timestamp(date)
            
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
            start_time = start_time.tz_localize(colombia_tz).tz_convert(ny_tz)
            #start_time = start_time.tz_localize(ny_tz)
            start_time = start_time.tz_localize(None)
            
            #start_time = start_time.round('s')

            next_start_time = pd.to_datetime(row['siguiente_start_time'])
            # Verificar que existe un siguiente válido
            if pd.notna(next_start_time):
                next_start_time = next_start_time.tz_localize(ny_tz)
                next_start_time = next_start_time.tz_localize(None)
            
            end_time = pd.to_datetime(row['end_time'])
            end_time = end_time.tz_localize(colombia_tz).tz_convert(ny_tz)
            #end_time = end_time.tz_localize(ny_tz)
            end_time = end_time.tz_localize(None)
            
            # 'start_time' ya es el Timestamp exacto de la fila del Excel (con fecha y hora)
            spy_open, spy_close = obtener_precios_spy_sql_final(start_time)
            if spy_open is not None and spy_close is not None:
                ## Usamos los precios obtenidos de SQL para ese Timestamp exacto
                precio_usar_apertura_excel = spy_open
                precio_usar_cierre_excel = spy_close
                # option_price usa el precio de apertura para encontrar el strike
                option_price = round(spy_open)
            else:
                st.write("No se encontraron datos de open o close del subyacente SPY en la BD")
                #precio_usar_apertura_excel = row['start_price']
                #precio_usar_cierre_excel = row['end_price']
                #option_price = round(row['start_price'])
        
            
            
            #st.write(f"1. Timestamp de Python (start_time): {start_time}")
            #st.write(f"   - Tipo: {type(start_time)}")
            #st.write(f"   - Zona Horaria: {start_time.tzinfo}")
            
            #spy_open, spy_close = obtener_precios_spy_sql_final(start_time)
            #st.write("Precio del open del SPY:")
            #st.write(spy_open)
            #st.write("Precio del close del SPY:")
            #st.write(spy_close)
            #if spy_open is not None and spy_close is not None:
                ## Usamos los precios obtenidos de SQL para ese Timestamp exacto
                #precio_usar_apertura_excel = spy_open
                #precio_usar_cierre_excel = spy_close
                # option_price usa el precio de apertura para encontrar el strike
                #option_price = round(spy_open)
            
            #Eliminar esto o comentarlo cuando esté la lógica de los precios del ETF sacados de SQL Database   
            #precio_usar_apertura_excel = row['start_price']
            #precio_usar_cierre_excel = row['end_price']
            #option_price = round(row['start_price'])
            
            #st.write(f"Descargando historial intradía del SPY para la fecha {start_time}...")
            # Llama a tu función existente para obtener los datos del ETF
            spy_intraday_historial = open_close_30min("SPY", api_key, fecha_inicio, fecha_fin)
            #st.write(spy_intraday_historial)
            
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
                    #st.write(f"✅ Cerrada posición: {pos['option_name']}, Resultado: ${trade_result_pos:.2f}")
                    
                else:
                    # Esta posición sigue abierta, mantenerla
                    posiciones_a_mantener.append(pos)
            
            # Actualizar la lista de posiciones abiertas (sin las que se cerraron)
            posiciones_abiertas = posiciones_a_mantener
            
            # Actualizar balance_posiciones después de los cierres
            balance_posiciones = balance - sum([p['cost_trade'] for p in posiciones_abiertas])
            # ========== FIN DE CIERRE DE POSICIONES ==========
            
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
                
                if contratos_especificos and "OptionName" in data.columns:
                    option_type = 'C' if row[column_name] == 1 else 'P'
                    option_name = row['OptionName']
                    df_option_prices_db = obtener_precios_sql(option_name, start_time, end_time)
                    st.write(f"Precios de la Opción '{option_name} obtenidos de SQL:")
                    st.dataframe(df_option_prices_db)
                    
                    if not df_option_prices_db.empty: 
                        df_option_cierre = df_option_prices_db.iloc[-1]
                        st.write("df option cierre:")
                        st.write(df_option_cierre)
                        posicion_actual_abierta = True
                        option_open_price = df_option_prices_db[precio_usar_apertura].iloc[0]##PENDIENTE DE REVISAR
                        st.write("Precio de entrada para la opción día actual:")
                        st.write(option_open_price)
                        option_close_price = df_option_prices_db[precio_usar_cierre].iloc[-1] #Revisar si debería ser -1 y no index(0)
                        st.write("Precio de salida opción día actual:")
                        st.write(option_close_price)
                        #option_close_price_cierre = df_option_cierre[precio_usar_cierre].iloc[index]#A revisar también
                        #st.write("Precio de salida opción día de cierre:")
                        #st.write(option_close_price_cierre)
                        max_contract_value = option_open_price * 100
                        #st.write("max_contract_value")
                        #st.write(max_contract_value)
                        
                        # Calcular número de contratos basado en balance_posiciones
                        if allocation_type == 'Porcentaje de asignación':
                            num_contratos = int((balance_posiciones * pct_allocation) / max_contract_value)
                        else: #allocation_type == 'Monto fijo de inversión':
                            if balance_posiciones < max_contract_value:
                                continue
                                #return pd.DataFrame(resultados), balance
                            else:
                                num_contratos = int(fixed_amount / max_contract_value)
                    
                        #st.write("Numero de contratos día actual:")
                        #st.write(num_contratos)
                        #st.write("Option Type actual:")
                        #st.write(option_type)
                        cost_trade = max_contract_value * num_contratos
                        #st.write("Costo de la operación:")
                        #st.write(cost_trade)
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
                        
                        trade_result_display = (df_option_cierre[precio_usar_cierre] - option_open_price) * 100 * num_contratos
                        resultados.append({
                            'Fecha': start_time, 
                            'Tipo': 'Call' if row[column_name] == 1 else 'Put',
                            'toggle_false': row[column_name],
                            'toggle_true': row[column_name],
                            'Fecha Apertura': start_time,
                            'Fecha Cierre': end_time,
                            'Precio Entrada': option_open_price, 
                            'Precio Salida Utilizado': df_option_cierre[precio_usar_cierre],
                            'Resultado': 0,  # Solo para mostrar
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
                        #print(trade_result)
                            
                            
                else:
                    option_date, actual_option_price = encontrar_strike_cercano(client, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                    option_price = actual_option_price
                    #st.write("option date")
                    #st.write(option_date)
                    
                    #option_date, actual_option_price = encontrar_strike_cercano(client, date, option_price, row[column_name], option_days, option_offset, ticker, method, offset)
                    #option_price = actual_option_price
                    #st.write("option date")
                    #st.write(option_date)
                    if option_date:
                        option_type = 'C' if row[column_name] == 1 else 'P'
                        option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                        #st.write("Option name:")
                        #st.write(option_name)
                        
                        df_option_start_time = obtener_historico_30min_start_time(option_name, api_key, date, date + timedelta(days=option_days))
                        #st.write("df_option_start_time:")
                        #st.write(df_option_start_time)
                        #df_option_end_time = obtener_historico_30min(option_name, api_key, date, date + timedelta(days=option_days)) #para end_time de minuto
                        df_option_end_time = obtener_historico_30min_start_time(option_name, api_key, date, date + timedelta(days=option_days))
                        #st.write("df_option_end_time:")
                        #st.write(df_option_end_time)
                        df_option_start_time = df_option_start_time.loc[start_time:]
                        #st.write("df_option_start_time recortado a start_time:")
                        #st.write(df_option_start_time)
                        #df_option_end_time = df_option_end_time.loc[start_time:] #para end_time de minuto
                        df_option_end_time = df_option_start_time.loc[start_time:]
                        #st.write("df_option_end_time recortado a start_time: esto es lo que quiero revisar")
                        #st.write(df_option_end_time)
                        
                        if not df_option_start_time.empty:
                            
                            #st.write("Esto es lo nuevo")
                            
                            # 1. Calcular la diferencia de tiempo (valor absoluto) para todos los índices
                            #time_diff_series = abs(df_option_start_time.index - end_time).to_series()
                            
                            # 2. Encontrar el índice (timestamp) que tiene la mínima diferencia
                            # idxmin() retorna el índice (timestamp) cuyo valor absoluto de diferencia es mínimo
                            #ts_mas_cercano = time_diff_series.idxmin()
                            
                            # 3. Crear el DataFrame final de cierre (df_option_cierre) con la fila más cercana
                            # Usamos doble corchete para que el resultado sea un DataFrame de una sola fila
                            #df_option_cierre = df_option_start_time.loc[[ts_mas_cercano]]
                            
                            #st.write("df option cierre (NUEVO)")
                            #st.write(df_option_cierre)
                            
                            #st.write("entra porque el df_option_start_time no está vacío")
                            #st.write(df_option_end_time.index)
                            #df_option_end_time = df_option_end_time.loc[end_time:] #para end_time de minuto
                            df_option_end_time = df_option_start_time.loc[end_time:]
                            #st.write("data frame empezando desde end_time o cercano: debería de ser el exacto")
                            #st.write(df_option_end_time)
                            #if not end_time in df_option.index:
                                #hacer end_time el siguiente registro del dataframe df_option.index 
                            #if end_time in df_option_end_time.index:
                            if not df_option_end_time.empty:
                                #st.write("entra acá porque end_time si está en df_option.index")
                                df_option_cierre = df_option_end_time.loc[end_time:] #para end_time de minuto
                                df_option_cierre = df_option_start_time.loc[end_time:]
                                #st.write("df_option recortado al cierre: a revisar")
                                #st.write(df_option_cierre)
                                posicion_actual_abierta = True
                                option_open_price = df_option_start_time[precio_usar_apertura].iloc[0]##PENDIENTE DE REVISAR
                                #st.write("Precio de entrada para la opción día actual:")
                                #st.write(option_open_price)
                                option_close_price = df_option_start_time[precio_usar_cierre].iloc[index]
                                #st.write("Precio de salida opción día actual:")
                                #st.write(option_close_price)
                                option_close_price_cierre = df_option_cierre[precio_usar_cierre].iloc[index]#A revisar también
                                #st.write("Precio de salida opción día de cierre:")
                                #st.write(option_close_price_cierre)
                                max_contract_value = option_open_price * 100
                                #st.write("max_contract_value")
                                #st.write(max_contract_value)
                                
                                # Calcular número de contratos basado en balance_posiciones
                                if allocation_type == 'Porcentaje de asignación':
                                    num_contratos = int((balance_posiciones * pct_allocation) / max_contract_value)
                                else: #allocation_type == 'Monto fijo de inversión':
                                    if balance_posiciones < max_contract_value:
                                        continue
                                        #return pd.DataFrame(resultados), balance
                                    else:
                                        num_contratos = int(fixed_amount / max_contract_value)
                                
                                
                                #st.write("Numero de contratos día actual:")
                                #st.write(num_contratos)
                                #st.write("Option Type actual:")
                                #st.write(option_type)
                                cost_trade = max_contract_value * num_contratos
                                #st.write("Costo de la operación:")
                                #st.write(cost_trade)
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
                                    'Resultado': 0,  # Solo para mostrar
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
                                #print(trade_result)
                            else:
                                #st.write("No entró al end_time en df_option.index")
                                df_option_end_time = df_option_start_time.loc[start_time:]
                                #st.write("Nuevo df_option_end_time")
                                #st.write(df_option_end_time)
                                
                                # PASO 1: Asegurarse de que el índice es de tipo Datetime.
                                # Esto debe hacerse ANTES de cualquier operación de zona horaria.
                                df_option_end_time.index = pd.to_datetime(df_option_end_time.index)
                                #st.write("paso 1 (df_option_end_time.index)")
                                #st.write(df_option_end_time.index)
                                
                                # PASO 2: ASIGNAR la zona horaria original (Localize). ¡ESTE ES EL PASO CLAVE!
                                # Le decimos a pandas que tus datos están en UTC. Si estuvieran en hora de Colombia,
                                # usarías 'America/Bogota'. 'UTC' es la opción más común y segura para datos de APIs.
                                try:
                                    # Intentamos localizar. Si ya tiene zona horaria, esto dará un error y pasaremos al except.
                                    df_localized = df_option_end_time.tz_localize('UTC')
                                except TypeError:
                                    # Si ya tenía una zona horaria, no hacemos nada y usamos el DataFrame como está.
                                    df_localized = df_option_end_time
                                
                                # PASO 3: CONVERTIR la zona horaria a la de Nueva York.
                                # Ahora que pandas sabe que los datos originales son UTC, sí puede convertirlos.
                                df_ny_time = df_localized.tz_convert('America/New_York')
                                #st.write("df_ny_time")
                                #st.write(df_ny_time)
                                
                                # -------------------------------------------------------------------------
                                # A partir de aquí, trabajamos SIEMPRE con 'df_ny_time'
                                # -------------------------------------------------------------------------
                                
                                # Encontrar el último timestamp exacto en horario de NY
                                latest_timestamp_ny = df_ny_time.index.max()
                                
                                # DECISIÓN AUTOMÁTICA: Determinar la hora de corte según la temporada
                                if latest_timestamp_ny.tzname() == 'EDT':
                                    HORA_DE_CORTE_NY = 15
                                    #st.write(f"La fecha {latest_timestamp_ny.date()} está en horario de VERANO (EDT).")
                                else:
                                    HORA_DE_CORTE_NY = 14
                                    #st.write(f"La fecha {latest_timestamp_ny.date()} está en horario de INVIERNO (EST).")
                                
                                #st.write(f"==> Se usará la hora de corte: {HORA_DE_CORTE_NY}:00 Hora de NY")
                                
                                # Construir el punto de inicio del filtro usando la hora decidida y la zona horaria de NY
                                punto_de_inicio_ny = pd.Timestamp(
                                    f"{latest_timestamp_ny.date()} {HORA_DE_CORTE_NY}:00:00",
                                    tz='America/New_York')
                                
                                
                                # Le quitas la zona horaria
                                punto_de_inicio_ny = punto_de_inicio_ny.tz_localize(None)
                                #st.write("punto de inicio:")
                                #st.write(punto_de_inicio_ny)
                                
                                #st.write("Punto de inicio para el filtro:")
                                #st.write(punto_de_inicio_ny)
                                
                                # PASO 4 (CORREGIDO): Cortar/Filtrar el DataFrame que SÍ está en la zona horaria de NY
                                #df_recortado_final = df_ny_time.loc[punto_de_inicio_ny:]
                                df_recortado_final = df_option_end_time.loc[punto_de_inicio_ny:]
                                #st.write("df_recortado_final:")
                                #st.write(df_recortado_final)
                                
                                if df_recortado_final.empty:
                                    #st.write("Por estar vacío el df_recortado_final")
                                    #st.write(df_option_end_time.index[-1])
                                    df_recortado_final = df_option_end_time.loc[df_option_end_time.index[-1]:]
                                    #st.write("df recortado final al index[-1], es decir estaba vacío el otro")
                                    #st.write(df_recortado_final)
                                    df_option_cierre = df_option_end_time.loc[df_option_end_time.index[-1]:] #para end_time de minuto
                                    df_option_cierre = df_option_start_time.loc[df_option_end_time.index[-1]:]
                                else:
                                    #st.write("Sin estar vacío el df_recortado_final")
                                    #st.write(df_option_end_time.index[-1])
                                    #st.write("entra acá porque end_time si está en df_option.index")
                                    df_option_cierre = df_option_end_time.loc[punto_de_inicio_ny:] #para end_time de minuto
                                    df_option_cierre = df_option_start_time.loc[punto_de_inicio_ny:]
                                    #df_recortado_final
                                # Ahora, la variable `df_recortado_final` contiene el resultado correcto.
                                #st.write("DataFrame después de ser cortado:")
                                #st.write(df_recortado_final)              
                                
                                
                                
                                
                                #st.write("df_option recortado al cierre: a revisar (este es el que lo corta en punto_de_inicio_ny)")
                                #st.write(df_option_cierre)
                                #posicion_actual_abierta = True
                                option_open_price = df_option_start_time[precio_usar_apertura].iloc[0]##PENDIENTE DE REVISAR
                                #st.write("Precio de entrada para la opción día actual:")
                                #st.write(option_open_price)
                                option_close_price = df_option_start_time[precio_usar_cierre].iloc[index]
                                #st.write("Precio de salida opción día actual:")
                                #st.write(option_close_price)
                                option_close_price_cierre = df_option_cierre[precio_usar_cierre].iloc[index]#A revisar también
                                #st.write("Precio de salida opción día de cierre:")
                                #st.write(option_close_price_cierre)
                                max_contract_value = option_open_price * 100
                                #st.write(max_contract_value)
                                
                                if allocation_type == 'Porcentaje de asignación':
                                    #st.write("Entra en este allocation_type")
                                    num_contratos = int((balance_posiciones * pct_allocation) / max_contract_value)
                                
                                
                                #st.write("Numero de contratos día actual:")
                                #st.write(num_contratos)
                                #st.write("Option Type actual:")
                                #st.write(option_type)
                                cost_trade = max_contract_value * num_contratos
                                #st.write("Costo de la operación:")
                                #st.write(cost_trade)
                                
                                # Restar costo y agregar a posiciones abiertas
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
                                
                                # Solo para display
                                trade_result = (df_option_cierre[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                                
                                
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
                                    #'Precio Salida': df_option_start_time[precio_usar_cierre].iloc[index],
                                    #'Precio Salida Utilizado': df_option[precio_usar_cierre].iloc[index],
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
                                    #'Open Posición Abierta': etf_open_price,
                                    #'Close Posición Abierta': etf_close_price
                                })
                            
        
        else: #El archivo no es Trades_H1
            if periodo == 'Diario':
                señal_actual = row[column_name]
                
                if 'probability' in data.columns:
                    #st.write("Si tiene la columna de modelos seleccionados")
                    ensamble = True
                else:
                    #st.write("El archivo no tiene la columna de modelos seleccionados")
                    ensamble = False
                    
                #if 'Selected_Models' in data.columns:
                    #st.write("Si tiene la columna de modelos seleccionados")
                    #ensamble = True
                #else:
                    #st.write("El archivo no tiene la columna de modelos seleccionados")
                    #ensamble = False
                
                #if ensamble and row['Selected_Models'] == "[]":
                if ensamble and row['probability'] == 0:
                    continue
                else: #Si tiene modelos
                    
                    # Nueva estrategia cuando el checkbox está seleccionado y el periodo es 'Diario'
                    if esce1:
                        if señal_actual in [0, 1]:
                            if posicion_anterior_abierta:  #posicion_anterior_abierta = True
                                #st.write("Hay posiciones abiertas...")
                                #st.write("date actual:")
                                #st.write(date)
                                #Abrimos una nueva posición del día actual
                                data_for_date = yf.download("SPY", start="2022-01-01", end=date + pd.DateOffset(days=1), multi_level_index=False, auto_adjust=False)
                                data_for_date = data_for_date.drop(data_for_date.index[-1])
                                #data_for_date.columns = data_for_date.columns.str.lower()
                                data_for_date.index.name = 'date'
                                #st.write("data_for_date")
                                #st.write(data_for_date)
                                print(data_for_date.columns)
                                if data_for_date.empty:
                                    continue
                                if trade_type == 'Close to Close':
                                    #st.write("Es close to close")
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
                                    #st.write("option type para posición abierta:")
                                    #st.write(option_type)
                                    option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
                                df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
                                df_option_anterior = obtener_historico(option_name_anterior, api_key, date, date + timedelta(days=option_days))
                                option_open_price_opnd = df_option_anterior[precio_usar_apertura_anterior].iloc[0]
                                option_close_price_opnd = df_option_anterior[precio_usar_cierre_anterior].iloc[0]
                                #st.write("Precio de entrada posición abierta siguiente día:")
                                #st.write(option_open_price_opnd)
                                #st.write("Precio de entrada posición abierta siguiente día:")
                                #st.write(option_close_price_opnd)
                                
                                
                                    
                                
                                if señal_actual == señal_anterior: #Tenemos posibilidad de recuperar ganancia
                                    #st.write("Señales iguales")
                                    #st.write("Manteniendo señal hasta el final del día...")
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
                                        #st.write("Precio del open de ayer")
                                        #st.write(etf_open_price_anterior)
                                        #st.write("Precio del close de ayer")
                                        #st.write(etf_close_price_anterior)
                                    
                                    if not data_for_date.empty:
                                        etf_close_price = data_for_date['Close'].iloc[0] if not data_for_date.empty else None
                                        etf_open_price = data_for_date['Open'].iloc[0] if not data_for_date.empty else None
                                        #st.write("Precio del open de hoy")
                                        #st.write(etf_open_price)
                                        #st.write("Precio del close de hoy")
                                        #st.write(etf_close_price)
                                        
                                        
                                    if not df_option.empty:
                                        option_close_price = df_option[precio_usar_cierre].iloc[index]
                                    trade_result_anterior = (option_close_price_opnd - precio_entrada_anterior) * 100 * num_contratos_anterior
                                    #st.write("Nuevo trade result anterior calculado:")
                                    #st.write(trade_result_anterior)
                                    
                                    
                                    
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
                                    #st.write("Señales no iguales")
                                    #st.write("Cerrando posición...")
                                    #st.write(date)
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
                                        #st.write("Precio del open de ayer")
                                        #st.write(etf_open_price_anterior)
                                        #st.write("Precio del close de ayer")
                                        #st.write(etf_close_price_anterior)
                                    
                                    if not data_for_date.empty:
                                        etf_open_price = data_for_date['Open'].iloc[0] if not data_for_date.empty else None
                                        etf_close_price = data_for_date['Close'].iloc[0] if not data_for_date.empty else None
                                        #st.write("Precio del open de hoy")
                                        #st.write(etf_open_price)
                                        #st.write("Precio del close de hoy")
                                        #st.write(etf_close_price)
                                    
                                    
                                    if not df_option.empty:
                                        option_open_price = df_option[precio_usar_apertura].iloc[0]
                                        option_close_price = df_option[precio_usar_cierre].iloc[index]
                                        
                                    trade_result_anterior = (option_open_price_opnd - precio_entrada_anterior) * 100 * num_contratos_anterior
                                    #st.write("Nuevo trade result anterior calculado:")
                                    #st.write(trade_result_anterior)
                                    
                                    
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
                                        #st.write(fecha_entrada)
                                        #st.write(precio_entrada_anterior)
                                        #st.write(num_contratos_anterior)
                                        #st.write(trade_result_anterior)
                                        
                                        #st.write(option_name_anterior)
                                        # No registramos el resultado aún
                                        # Guardamos la señal actual para la siguiente iteración
                                        señal_anterior = señal_actual
                                        
                            else: #posicion_anterior_abierta = False
                                #st.write("No hay posiciones abiertas para la fecha de:")
                                #st.write(date)
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
                                        #st.write("Precio de entrada para la opción día actual:")
                                        #st.write(option_open_price)
                                        option_close_price = df_option[precio_usar_cierre].iloc[index]
                                        #st.write("Precio de salida opción día actual:")
                                        #st.write(option_close_price)
                                        max_contract_value = option_open_price * 100
                                        
                                        if allocation_type == 'Porcentaje de asignación':
                                            num_contratos = int((balance * pct_allocation) / max_contract_value)
                                        else: #allocation_type == 'Monto fijo de inversión':
                                            if balance < max_contract_value:
                                                st.error("No hay suficiente dinero para abrir más posiciones. La ejecución del tester ha terminado.")
                                                return pd.DataFrame(resultados), balance
                                            else: #balance >= max_contract_value
                                                num_contratos = int(fixed_amount / max_contract_value)
                                        
                                        #st.write("Numero de contratos día actual:")
                                        #st.write(num_contratos)
                                        #st.write("Option Type actual:")
                                        #st.write(option_type)
                                        trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                                        if trade_result >= 0:
                                            balance += trade_result
                                            #st.write("trade result actual positivo:")
                                            #st.write(trade_result)
                                            # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                                            etf_data = yf.download("SPY", start="2022-01-01", end=date + pd.Timedelta(days=1), multi_level_index=False, auto_adjust=False)
                                            etf_data = etf_data.drop(etf_data.index[-1])
                                            etf_data.columns = etf_data.columns.str.lower()
                                            etf_data.index.name = 'date'
                                            etf_open_price = etf_data['Open'].iloc[0] if not etf_data.empty else None
                                            #st.write("Precio de entrada día actual:")
                                            #st.write(etf_open_price)
                                            etf_close_price = etf_data['Close'].iloc[0] if not etf_data.empty else None
                                            #st.write("Precio salida día actual:")
                                            #st.write(etf_close_price)
                                            
                                            
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
            # Esta posición SÍ debió cerrarse porque su end_time ya pasó
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
        resultados_df.to_excel('resultados_trades_1.xlsx')
    else:
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
    st.pyplot(plt) # Usar st.pyplot para renderizar en Streamlit correctamente


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
        float: left:
        margin-left: 5px;
        vertical_align: middle;
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
        # Variables de relleno si faltan partes
        default_values = ("Operación desconocida", "Modelo desconocido", "Responsable desconocido", 
                          "Fecha de inicio desconocida", "Fecha de fin desconocida")
                          
        parts = filename.split('_')
        # Quitar la extensión del último segmento (ej: 251125.xlsx -> 251125)
        if parts:
            parts[-1] = parts[-1].split('.')[0]
        
        # Rellenar la lista 'parts' si tiene menos de 5 elementos (Operación, Modelo, Responsable, F_Inicio, F_Fin)
        padded_parts = parts + [None] * (5 - len(parts))
    
        try:
            # 1. TIPO DE OPERACIÓN (Ej: OC) - Índice 0
            operation = {'CC': 'Close to Close', 'OC': 'Open to Close', 'CO': 'Close to Open'}.get(
                padded_parts[0], default_values[0]) if padded_parts[0] else default_values[0]
    
            # 2. NOMBRE DEL MODELO (Ej: ModeloOpciones) - Índice 1
            model_name = padded_parts[1] if padded_parts[1] else default_values[1]
            
            # 3. RESPONSABLE (Ej: Mateo) - Índice 2
            responsible = {'Valen': 'Valentina', 'Santi': 'Santiago', 'Andres': 'Andrés', 'Mateo': 'Mateo'}.get(
                padded_parts[2], default_values[2]) if padded_parts[2] else default_values[2]
                
            # 4. FECHA DE INICIO (ddmmyy -> dd/mm/yy) - Índice 3
            if padded_parts[3] and len(padded_parts[3]) >= 6:
                # Los dos primeros dígitos son el DÍA (dd), los siguientes dos el MES (mm), y los últimos el AÑO (yy)
                start_date = f"{padded_parts[3][0:2]}/{padded_parts[3][2:4]}/{padded_parts[3][4:6]}"
            else:
                start_date = default_values[3]
    
            # 5. FECHA DE FIN (ddmmyy -> dd/mm/yy) - Índice 4
            if padded_parts[4] and len(padded_parts[4]) >= 6:
                # Los dos primeros dígitos son el DÍA (dd), los siguientes dos el MES (mm), y los últimos el AÑO (yy)
                end_date = f"{padded_parts[4][0:2]}/{padded_parts[4][2:4]}/{padded_parts[4][4:6]}"
            else:
                end_date = default_values[4]
    
            # Devolvemos los 5 campos en el nuevo orden
            return operation, model_name, responsible, start_date, end_date
    
        except Exception:
            # Si ocurre cualquier error, devolvemos los valores por defecto
            return default_values
        
    info_placeholder = st.empty()
    toggle_activated = st.toggle("Se opera si se supera el Threshold", key='toggle_threshold')
    contratos_especificos= st.checkbox(
        "### Realizar testing con contratos específicos",
        value=False,
        key='check_contratos_especificos'
    )
    
    # --- NUEVA LÓGICA CONDICIONAL DE CONEXIÓN (A INSERTAR) ---
    if contratos_especificos:
        # st.empty() es un buen lugar para mostrar el estado de la conexión
        status_placeholder = st.empty()
        status_placeholder.info("Intentando conectar a Azure SQL Database...")
        establecer_conexion_sql()
        status_placeholder.empty() # Limpia el mensaje 'Intentando...'
    # --------------------------------------------------------
    
    column_name = 'toggle_true' if toggle_activated else 'toggle_false'
    data_filepath = st.selectbox("*Seleccionar archivo de datos históricos:*", archivos_disponibles, key='select_archivo_historico')
    
    # -------------------------------------------------------------
    # VALIDACIÓN DE COLUMNA ESPECÍFICA (NUEVO BLOQUE)
    # -------------------------------------------------------------
    if data_filepath:
        data = cargar_datos(data_filepath)
        # 1. Validación de 'OptionName' si el checkbox está marcado
        if contratos_especificos and 'OptionName' not in data.columns:
            st.error("🚨 Error: Al seleccionar 'Testing con contratos específicos', el archivo de entrada debe contener una columna llamada 'OptionName'.")
            st.stop() # Detiene la ejecución de la app
        # 2. Validación de columnas intradía si las necesitamos (asumiendo que es intradía si se usa OptionName)
        if contratos_especificos and not all(col in data.columns for col in ['start_time', 'end_time']):
            st.warning("⚠️ Advertencia: Se recomienda incluir 'start_time' y 'end_time' en el archivo Excel para el backtesting intradía con contratos específicos.")
    
    if data_filepath:
       # Ahora solo se esperan 5 valores: (operación, nombre_modelo, responsable, f_inicio, f_fin)
       operation, model_name, responsible, start_date, end_date = extract_file_info(data_filepath)
       
       # La comprobación de errores se puede hacer verificando si el primer campo es "desconocida"
       if operation.endswith("desconocida"):
           tooltip_text = f"<div class='tooltip'>&#9432; <span class='tooltiptext'>Error al decodificar el nombre del archivo. Verifique el formato.</span></div>"
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
    balance_inicial = st.number_input("*Balance inicial*", min_value=0, value=100000, step= 1000)
    
    allocation_type = st.radio("Seleccionar tipo de asignación de capital:", ('Porcentaje de asignación', 'Monto fijo de inversión'))
    if allocation_type == 'Porcentaje de asignación':
        pct_allocation = st.number_input("*Porcentaje de Asignación de Capital:*", min_value=0.001, max_value=0.6, value=0.05)
        fixed_amount = None
    else:
        fixed_amount = st.number_input("*Monto fijo de inversión:*", min_value=0.0, max_value=float(balance_inicial), value=1000.0, step=1000.0)
        pct_allocation = None
        
    periodo = st.radio("*Seleccionar periodo de datos*", ('Diario','15 minutos'))
    
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
    method = st.radio("*Seleccionar Strikes a Considerar*", ('ATM','OTM'))
    
    if method == "OTM":   
        offset = st.number_input("*Strikes a desplazarse*", min_value=0, value=5, step=1)
    else:
        offset = 0
    
    trade_type = st.radio('*Tipo de Operación*', ('Open to Close', 'Close to Close', 'Close to Open'))
    
    # --- EJECUCIÓN DEL BACKTEST ---
    if st.button("Run Backtest"):
        # 1. Ejecutar Lógica Principal
        resultados_df, final_balance = realizar_backtest(data_filepath, 'rlD0rjy9q_pT4Pv2UBzYlXl6SY5Wj7UT', "SPY", balance_inicial, pct_allocation, fixed_amount, 
        allocation_type, pd.Timestamp(fecha_inicio), pd.Timestamp(fecha_fin), option_days_input, option_offset_input, trade_type, periodo, column_name, method, offset, esce1, contratos_especificos=contratos_especificos)
        
        st.success("Backtest ejecutado correctamente!")
        
        # 2. Descargar Histórico Completo SPY (para la gráfica)
        st.write("Obteniendo datos completos del SPY para graficar...")
        try:
            spy_full_data = yf.download("SPY", start=fecha_inicio, end=fecha_fin + timedelta(days=1), progress=False)
            if isinstance(spy_full_data.columns, pd.MultiIndex):
                spy_full_data.columns = spy_full_data.columns.get_level_values(0)
        except Exception as e:
            st.warning(f"No se pudo descargar el histórico completo del SPY: {e}")
            spy_full_data = None
        
        # 3. PROCESAMIENTO DE MÉTRICAS (LO HACEMOS AHORA PARA QUE EL EXCEL SALGA COMPLETO)
        # Aseguramos formato fecha y orden
        resultados_df['Fecha'] = pd.to_datetime(resultados_df['Fecha'])
        
        # FILTRO IMPORTANTE: Aseguramos que el DF solo tenga datos del rango seleccionado por el usuario
        # Esto corrige el error de que la línea azul empiece antes de tiempo
        resultados_df = resultados_df[(resultados_df['Fecha'] >= pd.Timestamp(fecha_inicio)) & (resultados_df['Fecha'] <= pd.Timestamp(fecha_fin))]
        resultados_df = resultados_df.sort_values('Fecha').reset_index(drop=True)
        
        # Recalculamos acumulado después del filtro
        resultados_df['Ganancia acumulada'] = resultados_df['Resultado'].cumsum() + balance_inicial
        
        # Lógica de Métricas (Direction, Acierto, etc.)
        if trade_type == 'Close to Close':
            resultados_df['Direction'] = (resultados_df['Close'].shift(-1) > resultados_df['Close']).astype(int)
        elif trade_type == 'Close to Open':
            resultados_df['Direction'] = (resultados_df['Close'] < resultados_df['Open'].shift(-1)).astype(int)
        elif trade_type == 'Open to Close':
            resultados_df['Direction'] = (resultados_df['Open'] < resultados_df['Close']).astype(int)
        else:
            resultados_df['Direction'] = 0

        resultados_df['acierto'] = np.where(resultados_df['Direction'] == resultados_df[column_name], 1, 0)
        resultados_df['asertividad'] = resultados_df['acierto'].sum()/len(resultados_df['acierto']) if len(resultados_df['acierto']) > 0 else 0
        resultados_df['cumsum'] = resultados_df['acierto'].cumsum()
        resultados_df['accu'] = resultados_df['cumsum']/(resultados_df.index + 1)
        
        if trade_type == 'Open to Close':
            resultados_df['open_to_close_pct'] = resultados_df['Close']/resultados_df['Open'] - 1
            resultados_df['Ganancia'] = resultados_df.apply(lambda row: abs(row['open_to_close_pct']) if row['acierto'] else -abs(row['open_to_close_pct']), axis=1)
        elif trade_type == 'Close to Close':
            resultados_df['close_to_close_pct'] = resultados_df['Close'].shift(-1) / resultados_df['Close'] - 1
            resultados_df['Ganancia'] = resultados_df.apply(lambda row: abs(row['close_to_close_pct']) if row['acierto'] else -abs(row['close_to_close_pct']), axis=1)
        else:
            resultados_df['close_to_open_pct'] = resultados_df['Open'].shift(-1) / resultados_df['Close'] - 1 
            resultados_df['Ganancia'] = resultados_df.apply(lambda row: abs(row['close_to_open_pct']) if row['acierto'] else -abs(row['close_to_open_pct']), axis=1)

        resultados_df['Ganancia_Acumulada'] = resultados_df['Ganancia'].cumsum()

        # Cálculo de métricas ML (Matriz de Confusión)
        matrix=np.zeros((2,2)) 
        for i in range(len(resultados_df)):
            try:
                if int(resultados_df[column_name][i])==1 and int(resultados_df['Direction'][i])==1: 
                    matrix[0,0]+=1 
                elif int(resultados_df[column_name][i])==1 and int(resultados_df['Direction'][i])==0:
                    matrix[0,1]+=1 
                elif int(resultados_df[column_name][i])==0 and int(resultados_df['Direction'][i])==1:
                    matrix[1,0]+=1 
                elif int(resultados_df[column_name][i])==0 and int(resultados_df['Direction'][i])==0:
                    matrix[1,1]+=1 
            except:
                pass # Manejo de errores por si hay nulos
        
        tp, fp, fn, tn = matrix.ravel()
        resultados_df['tp'] = tp; resultados_df['tn'] = tn; resultados_df['fp'] = fp; resultados_df['fn'] = fn
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        resultados_df['precision'] = precision
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        resultados_df['recall'] = recall
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        resultados_df['f1_score'] = f1_score

        # Guardar resultados finales en session
        st.session_state['resultados_df'] = resultados_df
        st.session_state['final_balance'] = final_balance
        st.session_state['balance_inicial'] = balance_inicial
        
        # 4. GENERAR DESCARGAS (Ahora sí usamos el DF completo con todas las columnas)
        st.write("### Descargar Resultados")
        excel_buffer = io.BytesIO()
        resultados_df.to_excel(excel_buffer, index=False)
        st.download_button(label="Descargar Resultados Excel", data=excel_buffer, file_name="resultados_trades_1.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        
        # 5. GENERAR GRÁFICO (Con DF filtrado y completo)
        st.write("### Gráfico")
        fig, ax = plt.subplots(figsize=(14, 7))
        
        # Usamos resultados_df que ya filtramos por fechas arriba
        ax.plot(resultados_df['Fecha'], resultados_df['Ganancia acumulada'], marker='o', linestyle='-', color='b', label='Ganancia Acumulada')
        
        ax.set_title(f'Resultados del Backtesting - Balance final: ${final_balance:,.2f}')
        ax.set_xlabel('Fecha')
        ax.set_ylabel('Ganancia/Pérdida Acumulada', color='b')
        ax.tick_params(axis='y', labelcolor='b')
        plt.xticks(rotation=45)
        ax.axhline(y=balance_inicial, color='r', linestyle='-', label='Balance Inicial')
        
        # Eje Derecho (SPY)
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
        st.image(img_buffer)
        st.download_button(label="Descargar Gráfico", data=img_buffer, file_name="resultados_backtesting.png", mime="image/png")

        # 6. CREAR ZIP
        with zipfile.ZipFile("resultados.zip", "w") as zf:
            zf.writestr("resultados_trades_1.xlsx", excel_buffer.getvalue())
            zf.writestr("resultados_backtesting.png", img_buffer.getvalue())
            # Opcional: si quieres guardar también el archivo 'datos.xlsx' que es el mismo
            zf.writestr("datos.xlsx", excel_buffer.getvalue())

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
