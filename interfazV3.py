import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from polygon import RESTClient
from datetime import timedelta
from datetime import datetime
import streamlit as st
import io
import os
import zipfile
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

def listar_archivos_xlxs(directorio):
    archivos = [archivo for archivo in os.listdir(directorio) if archivo.endswith('.xlsx')]
    return archivos

def cargar_datos(filepath):
    data = pd.read_excel(filepath)
    data['date'] = pd.to_datetime(data['date'])
    data['date'] = data['date'].dt.date
    data = data.set_index('date')
    return data[['pred']]

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
    resp = client.get_aggs(
        ticker=ticker_opcion,
        multiplier=15,
        timespan="minute",
        from_=fecha_inicio.strftime('%Y-%m-%d %H:%M'),  # Incluye horas y minutos
        to=fecha_fin.strftime('%Y-%m-%d %H:%M')  # Incluye horas y minutos
    )
    datos = [
        {
            'fecha': pd.to_datetime(agg.timestamp, unit='ms'),
            'open': agg.open,
            'high': agg.high,
            'low': agg.low,
            'close': agg.close,
            'volume': agg.volume,
            'vwap': agg.vwap
        } for agg in resp
    ]
    df = pd.DataFrame(datos)
    df.set_index('fecha', inplace=True)
    return df

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

def realizar_backtest(data_filepath, api_key, ticker, balance_inicial, pct_allocation, fecha_inicio, fecha_fin, 
                      option_days=30, option_offset=0, trade_type='Close to Close', periodo='Diario'):
    data = cargar_datos(data_filepath)
    balance = balance_inicial
    resultados = []
    client = RESTClient(api_key)
    fecha_inicio = fecha_inicio.date()
    fecha_fin = fecha_fin.date()

    for date, row in data.iterrows():
        if date < fecha_inicio or date > fecha_fin:
            continue
        if row['pred'] not in [0, 1]:
            continue

        data_for_date = yf.download(ticker, start=date, end=date + pd.DateOffset(days=1))
        if data_for_date.empty:
            continue

        if trade_type == 'Close to Close':
            precio_usar_apertura = 'close'
            precio_usar_cierre = 'close'
            index = 1
        elif trade_type == 'Close to Open':
            precio_usar_apertura = 'close'
            precio_usar_cierre = 'open'
            index = 1
        else:  # Open to Close
            precio_usar_apertura = 'open'
            precio_usar_cierre = 'close'
            index = 0
            
        option_price = round(data_for_date[precio_usar_apertura.capitalize()].iloc[0])
        option_date = encontrar_opcion_cercana(client, date, option_price, row['pred'], option_days, option_offset, ticker)
        if option_date:
            option_type = 'C' if row['pred'] == 1 else 'P'
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

                max_contract_value = option_open_price * 100
                num_contratos = int((balance * pct_allocation) / max_contract_value)
                trade_result = (option_close_price - option_open_price) * 100 * num_contratos
                balance += trade_result

                etf_data = yf.download(ticker, start=date, end=date + pd.Timedelta(days=1))
                etf_open_price = etf_data['Open'].iloc[0] if not etf_data.empty else None
                etf_close_price = etf_data['Close'].iloc[0] if not etf_data.empty else None

                resultados.append({
                    'Fecha': date, 
                    'Tipo': 'Call' if row['pred'] == 1 else 'Put',
                    'Pred': row['pred'],
                    'Fecha Apertura': df_option.index[0],
                    'Fecha Cierre': df_option.index[index] if periodo == 'Diario' else df_option.index[-1],
                    'Precio Entrada': option_open_price, 
                    'Precio Salida': option_close_price, 
                    'Resultado': trade_result,
                    'Contratos': num_contratos,
                    'Opcion': option_name,
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
    
    directorio_datos = '.'
    archivos_disponibles = [archivo for archivo in os.listdir(directorio_datos) if archivo.endswith('.xlsx')]
    
    data_filepath = st.selectbox("**Seleccionar archivo de datos históricos**: (Trabajar en estos momentos con **modelo_andres_datos_act** el cual contiene datos desde 2022)", archivos_disponibles)
    
    option_days_input = st.number_input("**Option Days:** (Número de días de vencimiento de la opción que se está buscando durante el backtesting)", min_value=0, max_value=90, value=30, step=1)
    
    option_offset_input = st.number_input("**Option Offset:** (Rango de días de margen alrededor del número de días objetivo dentro del cual se buscará la opción más cercana)", min_value=0, max_value=90, value=7, step=1)
    
    balance_inicial = st.number_input("**Balance inicial**", min_value=0, value=100000, step=1000)
    pct_allocation = st.number_input("**Porcentaje de Asignación de Capital:**", min_value=0.001, max_value=0.6, value=0.05)
    fecha_inicio = st.date_input("**Fecha de inicio del periodo de backtest:**", min_value=datetime(2020, 1, 1))
    fecha_fin = st.date_input("**Fecha de finalización del periodo de backtest:**", max_value=datetime.today())
    trade_type = st.radio('**Tipo de Operación**', ('Close to Close', 'Open to Close', 'Close to Open'))

    # Nueva opción para seleccionar el período
    periodo = st.radio("**Seleccionar período de datos:**", ('Diario', '15 Minutos'))

    if st.button("Run Backtest"):
        resultados_df, final_balance = realizar_backtest(data_filepath, 'tXoXD_m9y_wE2kLEILzsSERW3djux3an', "SPY", 
                                                         balance_inicial, pct_allocation, pd.Timestamp(fecha_inicio), 
                                                         pd.Timestamp(fecha_fin), option_days_input, option_offset_input, 
                                                         trade_type, periodo)
        st.success("Backtest ejecutado correctamente!")

        st.session_state['resultados_df'] = resultados_df
        st.session_state['final_balance'] = final_balance
        st.session_state['balance_inicial'] = balance_inicial
        
        st.write("### Descargar Resultados")
        
        excel_buffer = io.BytesIO()
        resultados_df.to_excel(excel_buffer, index=False)
        st.download_button(label="Descargar Resultados Excel", data=excel_buffer, file_name="resultados_trades_1.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        
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
        datos['acierto'] = np.where(datos['Direction'] == datos['Pred'], 1, 0)
        datos['asertividad'] = datos['acierto'].sum()/len(datos['acierto'])
        datos['cumsum'] = datos['acierto'].cumsum()
        datos['accu'] = datos['cumsum']/(datos.index + 1)
        
        datos['open_to_close_pct'] = datos['Close']/datos['Open'] - 1
        datos['Ganancia'] = datos.apply(lambda row: abs(row['open_to_close_pct']) if row['acierto'] else -abs(row['open_to_close_pct']), axis=1)
        datos['Ganancia_Acumulada'] = datos['Ganancia'].cumsum()

        matrix = np.zeros((2,2))
        for i in range(len(datos['Pred'])):
            if int(datos['Pred'][i])==1 and int(datos['Direction'][i])==1: 
                matrix[0,0]+=1
            elif int(datos['Pred'][i])==1 and int(datos['Direction'][i])==0:
                matrix[0,1]+=1
            elif int(datos['Pred'][i])==0 and int(datos['Direction'][i])==1:
                matrix[1,0]+=1
            elif int(datos['Pred'][i])==0 and int(datos['Direction'][i])==0:
                matrix[1,1]+=1
                    
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

        with open("resultados.zip", "rb") as f:
            st.download_button(
                label="Descargar Resultados ZIP",
                data=f,
                file_name="resultados.zip",
                mime="application/zip"
            )

if __name__ == "__main__":
    main()
