import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import timedelta, datetime
import requests
import os
from polygon import RESTClient


def listar_archivos_xlxs(directorio):
    archivos = [archivo for archivo in os.listdir(directorio) if archivo.endswith('.xlsx')]
    return archivos


def cargar_datos(filepath):
    data = pd.read_excel(filepath)
    data['date'] = pd.to_datetime(data['date'])
    
    # No modificamos la columna 'date', manteniendo tanto fecha como hora
    data = data.set_index('date')
    return data[['pred']]


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


def obtener_valor_cercano(df, hora, tipo='open'):
    """
    Encuentra el valor más cercano a la hora solicitada dentro de los datos de 15 minutos.
    """
    try:
        # Filtra los datos que sean mayores o iguales a la hora solicitada
        df_filtro = df[df.index.time >= hora]
        if not df_filtro.empty:
            # Devuelve el primer valor que sea mayor o igual a la hora
            return df_filtro.iloc[0][tipo]
        else:
            # Si no hay ningún valor mayor o igual, se queda con el último valor disponible antes de la hora
            return df.iloc[-1][tipo]
    except:
        return None


def realizar_backtest(data_filepath, api_key, ticker, balance_inicial, pct_allocation, fecha_inicio, fecha_fin, option_days=30, option_offset=0, trade_type='Close to Close', periodo='Diario', hora_open=None, hora_close=None):
    data = cargar_datos(data_filepath)
    balance = balance_inicial
    resultados = []
    
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
        if row['pred'] not in [0, 1]:
            continue
        
        if periodo == 'Diario':
            # Si se selecciona una hora personalizada, usamos Alpha Vantage para obtener datos intradía
            if hora_open or hora_close:
                df_option = obtener_historico_15min(ticker, api_key, date, date + timedelta(days=1))
                if df_option.empty:
                    continue
                
                if hora_open:
                    option_price = obtener_valor_cercano(df_option, hora_open, 'open')
                else:
                    option_price = df_option['open'].iloc[0]  # Hora predeterminada 9:30am

                if hora_close:
                    close_price = obtener_valor_cercano(df_option, hora_close, 'close')
                else:
                    close_price = df_option['close'].iloc[-1]  # Hora predeterminada 4:00pm
            else:
                # Usar yFinance para obtener datos estándar si no hay hora personalizada
                data_for_date = yf.download(ticker, start=date, end=date + pd.DateOffset(days=1))
                if data_for_date.empty:
                    continue
                option_price = round(data_for_date['Close'].iloc[0])  # Precio de cierre del día anterior
                close_price = round(data_for_date['Close'].iloc[0])
        else:  # Período de 15 minutos
            # Obtener datos intradía desde AlphaVantage
            df_option = obtener_historico_15min(ticker, api_key, date, date + timedelta(days=1))
            if df_option.empty:
                continue
            
            if hora_open:
                option_price = obtener_valor_cercano(df_option, hora_open, 'open')
            else:
                option_price = df_option['open'].iloc[0]
            
            if hora_close:
                close_price = obtener_valor_cercano(df_option, hora_close, 'close')
            else:
                close_price = df_option['close'].iloc[-1]

        # Calcular número de contratos y resultado del trade
        num_contratos = int((balance * pct_allocation) / (option_price * 100))
        trade_result = (close_price - option_price) * 100 * num_contratos
        balance += trade_result

        resultados.append({
            'Fecha': date, 
            'Tipo': 'Call' if row['pred'] == 1 else 'Put',
            'Pred': row['pred'],
            'Precio Entrada': option_price, 
            'Precio Salida': close_price, 
            'Resultado': trade_result,
            'Contratos': num_contratos
        })

    resultados_df = pd.DataFrame(resultados)
    return resultados_df, balance


def main():
    st.title("Backtesting ARKAD")
    
    directorio_datos = '.'
    archivos_disponibles = listar_archivos_xlxs(directorio_datos)
    data_filepath = st.selectbox("Seleccionar archivo de datos históricos:", archivos_disponibles)
    
    option_days_input = st.number_input("Option Days", min_value=0, max_value=90, value=30, step=1)
    option_offset_input = st.number_input("Option Offset", min_value=0, max_value=90, value=7, step=1)
    
    balance_inicial = st.number_input("Balance inicial", min_value=0, value=100000, step=1000)
    pct_allocation = st.number_input("Porcentaje de Asignación de Capital:", min_value=0.001, max_value=0.6, value=0.05)
    
    fecha_inicio = st.date_input("Fecha de inicio del backtest", min_value=datetime(2020, 1, 1))
    fecha_fin = st.date_input("Fecha de fin del backtest", max_value=datetime.today())
    
    trade_type = st.radio('Tipo de Operación', ('Close to Close', 'Open to Close', 'Close to Open'))
    periodo = st.radio("Periodo de datos", ('Diario', '15 minutos'))
    
    # Inputs para hora de open y close personalizada, funcionan tanto en "Diario" como "15 minutos"
    hora_open = st.time_input("Seleccionar hora de Open", value=datetime.strptime("09:30", "%H:%M").time())
    hora_close = st.time_input("Seleccionar hora de Close", value=datetime.strptime("16:00", "%H:%M").time())

    if st.button("Run Backtest"):
        resultados_df, final_balance = realizar_backtest(data_filepath, 'tu_api_key', "SPY", balance_inicial, pct_allocation, pd.Timestamp(fecha_inicio), pd.Timestamp(fecha_fin), option_days_input, option_offset_input, trade_type, periodo, hora_open, hora_close)
        st.success("Backtest ejecutado correctamente!")
        # Mostrar resultados
        st.dataframe(resultados_df)


if __name__ == "__main__":
    main()

