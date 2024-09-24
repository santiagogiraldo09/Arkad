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
            
           #st.write("DataFrame filtrado por rango de fechas:", df)
            #st.write("Valores de Open y Close para el rango de fechas:", df_completo[['open', 'close']])
            
            return df
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
    
    
def obtener_historico(ticker_opcion, api_key, fecha_inicio, fecha_fin):
    client = RESTClient(api_key)
    resp = client.get_aggs(ticker=ticker_opcion, multiplier=1, timespan="day", from_=fecha_inicio.strftime('%Y-%m-%d'), to=fecha_fin.strftime('%Y-%m-%d'))
    datos = [{'fecha': pd.to_datetime(agg.timestamp, unit='ms'), 'open': agg.open, 'close': agg.close} for agg in resp]
    df = pd.DataFrame(datos)
    df.set_index('fecha', inplace=True)
    df.index = df.index.date
    return df

def obtener_historico_15min(ticker_opcion, api_key, fecha_inicio, fecha_fin):
    client = RESTClient(api_key)
    api_av = "KCIUEY7RBRKTL8GI"
    try:
        # Obtener datos agregados cada 15 minutos
        resp = client.get_aggs(ticker=ticker_opcion, multiplier=15, timespan="minute", 
                               from_=fecha_inicio.strftime('%Y-%m-%d'), to=fecha_fin.strftime('%Y-%m-%d'))
        
        # Procesar la respuesta para crear el DataFrame
        datos = [{'fecha': pd.to_datetime(agg.timestamp, unit='ms'), 'open': agg.open, 'high': agg.high, 
                  'low': agg.low, 'close': agg.close, 'volume': agg.volume} for agg in resp]
        df = pd.DataFrame(datos)
        
        # Establecer la columna 'fecha' como el índice del DataFrame
        df.set_index('fecha', inplace=True)
        df.index = pd.to_datetime(df.index)
        
        # Filtrar el DataFrame por las fechas de inicio y fin
        df = df[(df.index >= fecha_inicio) & (df.index <= fecha_fin)]
        
        return df
    
    except Exception as e:
        print(f"Error al obtener datos para {ticker_opcion}: {str(e)}")
        return pd.DataFrame()
    
    # Usar Alpha Vantage para obtener datos del subyacente
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": ticker_opcion,
        "interval": "15min",
        "apikey": api_av,
        "outputsize": "full",
        "extended_hours": "false"
    }
    try:
        response = requests.get(url, params=params)
        data = response.json()
        st.write("Respuesta JSON completa:", data)  # También se muestra en Streamlit
        
    except Exception as e:
        print(f"Error al obtener datos para {ticker_opcion}: {str(e)}")



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
        st.write("Respuesta JSON completa:", data)  # También se muestra en Streamlit
        
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
    min_days = option_days - option_offset
    max_days = option_days + option_offset
    best_date = None
    for offset in range(min_days, max_days + 1):
        option_date = (base_date + timedelta(days=offset)).strftime('%y%m%d')
        option_type = 'C' if column_name == 1 else 'P'
        option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
        if verificar_opcion(client, option_name, base_date, base_date + timedelta(days=1)):
            best_date = option_date
            break
    return best_date

                # Obtener el precio de apertura del ETF del índice para la fecha correspondiente con Yahoo Finance
                #etf_data = yf.download(ticker, start=date, end=date + pd.Timedelta(days=1))
                #etf_open_price = etf_data['Open'].iloc[0] if not etf_data.empty else None
                #etf_close_price = etf_data['Close'].iloc[0] if not etf_data.empty else None

def realizar_backtest(data_filepath, api_key, ticker, balance_inicial, pct_allocation, fecha_inicio, fecha_fin, option_days=30, option_offset=0, trade_type='Close to Close', periodo='Diario', column_name='toggle_false'):
    data = cargar_datos(data_filepath)
    balance = balance_inicial
    resultados = []
    client = RESTClient(api_key)
    
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

        #data_for_date = yf.download(ticker, start=date - pd.DateOffset(days=1), end=date + pd.DateOffset(days=1))
        #if data_for_date.empty or len(data_for_date) < 2:
            #continue


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
            
        option_price = round(data_for_date[precio_usar_apertura.capitalize()].iloc[0])
        option_date = encontrar_opcion_cercana(client, date, option_price, row[column_name], option_days, option_offset, ticker)
        if option_date:
            option_type = 'C' if row[column_name] == 1 else 'P'
            option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
            
            if periodo == 'Diario':
                df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
                #st.dataframe(df_option)
                #st.write("Respuesta JSON completa:", data)  # También se muestra en Streamlit
            else:  # '15 Minutos'
                df_option = obtener_historico_15min(option_name, api_key, date, date + timedelta(days=option_days))
                df = get_open_and_close(ticker, api_av, fecha_inicio, fecha_fin)
                #st.dataframe(df_option)
                #st.write("Respuesta JSON completa:", data)  # También se muestra en Streamlit
            if not df_option.empty:
                if periodo == 'Diario':
                    option_open_price = df_option[precio_usar_apertura].iloc[0]
                    option_close_price = df_option[precio_usar_cierre].iloc[index]
                else:  # '15 Minutos'
                    st.write("Entró por acá")
                    option_open_price = df_option['open'].iloc[0]
                    st.write(df_option.iloc[0])
                    st.write(df_option.iloc[-1])
                    st.write(df_option)
                    st.write(df_option[precio_usar_cierre].iloc[index])
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
                    'Close': etf_close_price
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
    
    #archivo_seleccionado = st.selectbox("Selecciona el archivo de datos:", archivos_disponibles)
    #archivo_seleccionado_path = os.path.join(directorio_datos, archivo_seleccionado)
    
    # Option Days input
    option_days_input = st.number_input("*Option Days:* (Número de días de vencimiento de la opción que se está buscando durante el backtesting)", min_value=0, max_value=90, value=30, step=1)
    
    # Option Offset input
    option_offset_input = st.number_input("*Option Offset:* (Rango de días de margen alrededor del número de días objetivo dentro del cual se buscará la opción más cercana)", min_value=0, max_value=90, value=7, step=1)
    
    # Additional inputs for the backtest function
    #data_filepath = 'datos_8.xlsx'
    #api_key = st.text_input("API Key", "tXoXD_m9y_wE2kLEILzsSERW3djux3an")
    #ticker = st.text_input("Ticker Symbol", "SPY")
    balance_inicial = st.number_input("*Balance iniciall*", min_value=0, value=100000, step= 1000)
    pct_allocation = st.number_input("*Porcentaje de Asignación de Capital:*", min_value=0.001, max_value=0.6, value=0.05)
    fecha_inicio = st.date_input("*Fecha de inicio del periodo de backtest:*", min_value=datetime(2020, 1, 1))
    fecha_fin = st.date_input("*Fecha de finalización del periodo de backtest:*", max_value=datetime.today())
    trade_type = st.radio('*Tipo de Operación*', ('Close to Close', 'Open to Close', 'Close to Open'))
    
    # Nuevos inputs para la hora de apertura y cierre
    #open_time = st.time_input("*Seleccionar Hora de Apertura:*", value=datetime.strptime("09:30", "%H:%M").time())
    #close_time = st.time_input("*Seleccionar Hora de Cierre:*", value=datetime.strptime("16:00", "%H:%M").time())
    
    periodo = st.radio("*Selecionar periodo de datos*", ('Diario','15 minutos'))

    #if trade_type == 'Close to Close':
       #close_to_close = True
    #else:
        #close_to_close = False

    #API KEY que se tenía
    #tXoXD_m9y_wE2kLEILzsSERW3djux3an
    #KCIUEY7RBRKTL8GI
    
    if st.button("Run Backtest"):
        resultados_df, final_balance = realizar_backtest(data_filepath, 'tXoXD_m9y_wE2kLEILzsSERW3djux3an', "SPY", balance_inicial, pct_allocation, pd.Timestamp(fecha_inicio), pd.Timestamp(fecha_fin), option_days_input, option_offset_input, trade_type, periodo, column_name)
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

        #the above code adds up the frequencies of the tps,tns,fps,fns and a matrix is formed
        
        datos.to_excel(excel_buffer, index=False)
        
        # datos[(datos['Fecha'] >= fecha_inicio)
        #               & (datos['Fecha'] <= fecha_fin)]
        
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