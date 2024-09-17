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



def listar_archivos_xlxs(directorio):
    archivos = [archivo for archivo in os.listdir(directorio) if archivo.endswith('.xlsx')]
    return archivos


def cargar_datos(filepath):
    try:
        data = pd.read_excel(filepath)       
        data['date'] = pd.to_datetime(data['date'])        
        # No modificamos la columna 'date', manteniendo tanto fecha como hora
        data = data.set_index('date')
        
        #Verificar columnas
        required_columns = ['proba(0)', 'proba(1)', 'toggle_true', 'toggle_false']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError("Faltan las siguientes columnas en el Dataframse: " + ",".join(missing_columns))
           
        threshold_value = data['threshold'].iloc[0] if 'threshold' in data.columns else None
        return data[required_columns], threshold_value
    except Exception as e:
        print(F"Error cargando los datos: {e}")
        return pd.DataFrame(), None
        #if 'threshold' in data.columns:
            #threshold_value = data['threshold'].iloc[0]
        #else:
            #st.error("No cuenta con dato de threshold óptimo este archivo.")
            #threshold_value = None
            
        return data[['proba(0)', 'proba(1)', 'toggle_true', 'toggle_false']], threshold_value

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
        
        if "Time Series (15min)" not in data:
            print(f"No se recibieron datos para {ticker_opcion}")
            return pd.DataFrame()
        
        time_series = data["Time Series (15min)"]
        
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
   
def encontrar_opcion_cercana(client, base_date, option_price, pred, option_days, option_offset, ticker):
    min_days = option_days - option_offset
    max_days = option_days + option_offset
    best_date = None
    for offset in range(min_days, max_days + 1):
        option_date = (base_date + timedelta(days=offset)).strftime('%y%m%d')
        option_type = 'C' if pred == 1 else 'P'
        option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
        if verificar_opcion(client, option_name, base_date, base_date + timedelta(days=1)):
            best_date = option_date
            break
    return best_date

def realizar_backtest(data_filepath, api_key, ticker, balance_inicial, pct_allocation, fecha_inicio, fecha_fin, option_days=30, option_offset=0, trade_type='Close to Close', periodo='Diario', toggle_activated=False):
    data = cargar_datos(data_filepath)
    balance = balance_inicial
    resultados = []
    client = RESTClient(api_key)
    
    #Elegir la columna correcta en función del toggle
    column_name = 'toggle_true' if toggle_activated else 'toggle_false'
    
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
        
        action = row[column_name]

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
        option_date = encontrar_opcion_cercana(client, date, option_price, action, option_days, option_offset, ticker)
        if option_date:
            option_type = 'C' if action == 1 else 'P'
            option_name = f'O:{ticker}{option_date}{option_type}00{option_price}000'
            
            if periodo == 'Diario':
                df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))
            else:  # '15 Minutos'
                df_option = obtener_historico_15min(option_name, api_key, date, date + timedelta(days=option_days))
            
            if not df_option.empty:
                if periodo == 'Diario':
                    option_open_price = df_option[precio_usar_apertura].iloc[0]
                    option_close_price = df_option[precio_usar_cierre].iloc[index]
                else:  # '15 Minutos'
                    option_open_price = df_option['open'].iloc[0]
                    option_close_price = df_option['close'].iloc[-1]  # Último cierre del día

            
            df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))    
            
            if not df_option.empty:
                option_open_price = df_option[precio_usar_apertura].iloc[0]
                option_close_price = df_option[precio_usar_cierre].iloc[index]
                max_contract_value = option_open_price * 100
                num_contratos = int((balance * pct_allocation) / max_contract_value)
                trade_result = (df_option[precio_usar_cierre].iloc[index] - option_open_price) * 100 * num_contratos
                balance += trade_result

                # Obtener el símbolo del ETF del índice (por ejemplo, 'SPY' para el índice S&P 500)
                #etf_symbol = 'SPY'  # Reemplaza 'SPY' con el símbolo correcto de tu ETF de índice
    
                # Obtener el precio de apertura del ETF del índice para la fecha correspondiente
                etf_data = yf.download(ticker, start=date, end=date + pd.Timedelta(days=1))
                etf_open_price = etf_data['Open'].iloc[0] if not etf_data.empty else None
                etf_close_price = etf_data['Close'].iloc[0] if not etf_data.empty else None


                resultados.append({
                    'Fecha': date, 
                    'Tipo': 'Call' if row['pred'] == 1 else 'Put',
                    'Pred': row['pred'],
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
        text-align: left;  /*Alineación de texto*/
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

    # Agregamos el ícono o el botón con tooltip
    #st.markdown('''
    #<div class="tooltip">
        #&#9432;  <!-- Ícono de información -->
        #<span class="tooltiptext">Selecciona el archivo .xlsx con los datos históricos</span>
    #</div>
    #''', unsafe_allow_html=True)
    
    
    
    
    #st.write("Use this interface to set the values for 'option_days' and 'option_offset'.")
    #if 'show_popup' not in st.session_state:
        #st.session_state.show_popup = False
    
    # Directorio donde se encuentran los archivos .xlsx
    directorio_datos = '.'
    archivos_disponibles = [archivo for archivo in os.listdir(directorio_datos) if archivo.endswith('.xlsx')]
    
    
    #Extraer información del nombre del archivo seleccionado
    def extract_file_info(filename):
        parts = filename.split('_')
        if len(parts) < 5:  # Verifica que haya suficientes partes en el nombre del archivo
            return "Información desconocida del algoritmo", "Información desconocida del algoritmo", "Información desconocida del algoritmo", "Información desconocida del algoritmo", "Información desconocida del algoritmo"
    
        try:
            operation = {'CC': 'Close to Close', 'OC': 'Open to Close', 'CO': 'Close to Open'}.get(parts[0], 'Operación desconocida')
            responsible = {'Valen': 'Valentina', 'Santi': 'Santiago', 'Andres': 'Andrés'}.get(parts[1], 'Responsable desconocido')
            start_date = parts[2][2:4] + '/' + parts[2][4:6] #+ '/20' + parts[2][0:2]
            end_date = parts[3][2:4] + '/' + parts[3][4:6] #+ '/20' + parts[3][0:2]
            version = parts[4].split('.')[0]
        except IndexError:
            return "Información desconocida del algoritmo", "Información desconocida del algoritmo", "Información desconocida del algoritmo", "Información desconocida del algoritmo", "Información desconocida del algoritmo"
    
        return operation, responsible, start_date, end_date, version

    #placeholder para el ícono de información
    info_placeholder = st.empty()
    
    # Opción de selección del archivo .xlsx
    data_filepath = st.selectbox("**Seleccionar archivo de datos históricos**:", archivos_disponibles)
    

    if data_filepath:
       operation, responsible, start_date, end_date, version = extract_file_info(data_filepath)
       data, threshold_value = cargar_datos(data_filepath)
       
       if threshold_value is not None:
           st.write(f"**Threshold óptimo: {threshold_value}**")
       else:
           st.write("**Threshold óptimo:** No se pudo encontrar el valor del threshold en el archivo.")
       # Actualizar el tooltip
       if operation.startswith("Información desconocida"):
           tooltip_text = f"<div class='tooltip'>&#9432; <span class='tooltiptext'>{operation}</span></div>"
       else:
           tooltip_text = f"""
           <div class="tooltip">
                &#9432;  <!-- Ícono de información -->
                <span class="tooltiptext">
                Tipo de operación: {operation}<br>
                Responsable del algoritmo: {responsible}<br>
                Rango de fechas: {start_date}<br>
                {end_date}<br>
                Versión: {version}
                </span>
           </div>
            """
       info_placeholder.markdown(tooltip_text, unsafe_allow_html=True)
    #Botón para activar el pop-up
    #if st.button("Información"):
        #st.session_state.show_popup = True
        
    #Mostrar el po-up si está activado
    #if st.session_state.show_popup:
        #with st.container():
            #st.markdown("## Este es un Pop-up")
            #st.write("Aquí puedes agregar cualquier contenido que desees mostrar en el pop-up.")
            #if st.button("Cerrar"):
                #st.session_state.show_popup = False
            
    #st.warning("información relevante.")
    
    #archivo_seleccionado = st.selectbox("Selecciona el archivo de datos:", archivos_disponibles)
    #archivo_seleccionado_path = os.path.join(directorio_datos, archivo_seleccionado)
    
    #Toogle
    toggle_activated = st.toggle("Operar según el Threshold")
    # Option Days input
    option_days_input = st.number_input("**Option Days:** (Número de días de vencimiento de la opción que se está buscando durante el backtesting)", min_value=0, max_value=90, value=30, step=1)
    
    # Option Offset input
    option_offset_input = st.number_input("**Option Offset:** (Rango de días de margen alrededor del número de días objetivo dentro del cual se buscará la opción más cercana)", min_value=0, max_value=90, value=7, step=1)
    
    # Additional inputs for the backtest function
    #data_filepath = 'datos_8.xlsx'
    #api_key = st.text_input("API Key", "tXoXD_m9y_wE2kLEILzsSERW3djux3an")
    #ticker = st.text_input("Ticker Symbol", "SPY")
    balance_inicial = st.number_input("**Balance iniciall**", min_value=0, value=100000, step= 1000)
    pct_allocation = st.number_input("**Porcentaje de Asignación de Capital:**", min_value=0.001, max_value=0.6, value=0.05)
    fecha_inicio = st.date_input("**Fecha de inicio del periodo de backtest:**", min_value=datetime(2020, 1, 1))
    fecha_fin = st.date_input("**Fecha de finalización del periodo de backtest:**", max_value=datetime.today())
    trade_type = st.radio('**Tipo de Operación**', ('Close to Close', 'Open to Close', 'Close to Open'))
    
    # Nuevos inputs para la hora de apertura y cierre
    #open_time = st.time_input("**Seleccionar Hora de Apertura:**", value=datetime.strptime("09:30", "%H:%M").time())
    #close_time = st.time_input("**Seleccionar Hora de Cierre:**", value=datetime.strptime("16:00", "%H:%M").time())
    
    periodo = st.radio("**Selecionar periodo de datos**", ('Diario','15 minutos'))

    #if trade_type == 'Close to Close':
       #close_to_close = True
    #else:
        #close_to_close = False

    
    if st.button("Run Backtest"):
        resultados_df, final_balance = realizar_backtest(data_filepath, 'tXoXD_m9y_wE2kLEILzsSERW3djux3an' , "SPY", balance_inicial, pct_allocation, pd.Timestamp(fecha_inicio), pd.Timestamp(fecha_fin), option_days_input, option_offset_input, trade_type, periodo, toggle_activated)
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
            datos['Direction'] == datos['Pred'], 1, 0)
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
        for i in range(len(datos['Pred'])): #the confusion matrix is for 2 classes: 1,0
                #1=positive, 0=negative
            if int(datos['Pred'][i])==1 and int(datos['Direction'][i])==1: 
                matrix[0,0]+=1 #True Positives
            elif int(datos['Pred'][i])==1 and int(datos['Direction'][i])==0:
                   matrix[0,1]+=1 #False Positives
            elif int(datos['Pred'][i])==0 and int(datos['Direction'][i])==1:
                  matrix[1,0]+=1 #False Negatives
            elif int(datos['Pred'][i])==0 and int(datos['Direction'][i])==0:
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