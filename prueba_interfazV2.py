import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from polygon import RESTClient
from datetime import timedelta
from datetime import datetime
import streamlit as st


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

def encontrar_opcion_cercana(client, base_date, option_price, pred, option_days, option_offset):
    # Define the range of acceptable expiration dates
    min_days = option_days - option_offset
    max_days = option_days + option_offset
    best_date = None
    for offset in range(min_days, max_days + 1):
        option_date = (base_date + timedelta(days=offset)).strftime('%y%m%d')
        option_type = 'C' if pred == 1 else 'P'
        option_name = f'O:SPY{option_date}{option_type}00{option_price}000'
        if verificar_opcion(client, option_name, base_date, base_date + timedelta(days=1)):
            best_date = option_date
            break
    return best_date

def realizar_backtest(data_filepath, api_key, balance_inicial, pct_allocation, fecha_inicio, fecha_fin, option_days=30, option_offset=0, close_to_close=False):
    data = cargar_datos(data_filepath)
    balance = balance_inicial
    resultados = []
    client = RESTClient(api_key)
    #fecha_inicio = fecha_inicio.date()
    #fecha_fin = fecha_fin.date()

    for date, row in data.iterrows():
        if date < fecha_inicio or date > fecha_fin:
            continue
        if row['pred'] not in [0, 1]:
            continue

        # Descargar datos históricos de SPY
        spy_for_date = yf.download('SPY', start=date, end=date + pd.DateOffset(days=1))
        if spy_for_date.empty:
            continue

        if close_to_close:
            precio_usar_apertura = 'close'
            precio_usar_cierre = 'close'
        else:
            precio_usar_apertura = 'open'
            precio_usar_cierre = 'close'
            
        option_price = round(spy_for_date[precio_usar_apertura.capitalize()].iloc[0])
        option_date = encontrar_opcion_cercana(client, date, option_price, row['pred'], option_days, option_offset)
        if option_date:
            option_type = 'C' if row['pred'] == 1 else 'P'
            option_name = f'O:SPY{option_date}{option_type}00{option_price}000'
            
            df_option = obtener_historico(option_name, api_key, date, date + timedelta(days=option_days))    
            
            if not df_option.empty:
                option_open_price = df_option[precio_usar_apertura].iloc[0]
                max_contract_value = option_open_price * 100
                num_contratos = int((balance * pct_allocation) / max_contract_value)
                trade_result = (df_option[precio_usar_cierre].iloc[-1] - option_open_price) * 100 * num_contratos
                balance += trade_result
                resultados.append({
                    'Fecha': date, 
                    'Tipo': 'Call' if row['pred'] == 1 else 'Put', 
                    'Precio Entrada': option_open_price, 
                    'Precio Salida': df_option[precio_usar_cierre].iloc[-1], 
                    'Resultado': trade_result,
                    'Contratos': num_contratos
                })

    resultados_df = pd.DataFrame(resultados)
    graficar_resultados(resultados_df, balance, balance_inicial)
    resultados_df.to_excel('resultados_trades_1.xlsx')
    
def format_y_axis(value, _):
    if value >= 1e6:
        return f"{value / 1e6:.0f} M"
    elif value >= 1e3:
        return f"{value / 1e3:.0f} k"
    else:
        return str(value)


def graficar_resultados(df, final_balance, balance_inicial):
    plt.figure(figsize=(14, 7))
    df['Ganancia acumulada'] = df['Resultado'].cumsum() + balance_inicial
    ax = df.set_index('Fecha')['Ganancia acumulada'].plot(kind='line', marker='o', linestyle='-', color='b')
    ax.set_title(f'Resultados del Backtesting de Opciones - Balance final: ${final_balance:,.2f}')
    ax.set_xlabel('Fecha')
    ax.set_ylabel('Ganancia/Pérdida Acumulada')
    plt.xticks(rotation=45)

    ax.axhline(y=balance_inicial, color='r', linestyle='-', label='Balance Inicial')

    # Cambiar el formato del eje Y
    ax.yaxis.set_major_formatter(FuncFormatter(format_y_axis))    

    plt.legend()
    plt.grid(True, which='both', linestyle='-', linewidth=0.5)
    plt.tight_layout()
    plt.savefig('resultados_backtesting.png')
    plt.show()

def main():
    
    # Establecer el estilo de la página
    st.markdown(
        """
        <style>
        body {
            background-color: #ffffff;
        }
        .st-bc {
            background-color: #172a3a;
        }
        .st-dm {
            color: #ffffff; /* Cambiar color del texto a blanco */
        }
        .st-dm {
            color: #172a3a;
        }
        </style>
        """,
        
        unsafe_allow_html=True
    )
    
    st.title("Backtesting ARKAD")
    #st.image(r'C:\Users\Lenovo Thinkpad E14\Downloads\Arkad.jpeg', width=200)
    
        # Widget para ingresar el valor de option_days
    option_days_input = st.number_input("**Option Days:** (Número de días de vencimiento de la opción que se está buscando durante el backtesting)", min_value=0, max_value=90, value=30, step=1)

    # Widget para ingresar el valor de option_offset
    option_offset_input = st.number_input("**Option Offset:** (Rango de días de margen alrededor del número de días objetivo dentro del cual se buscará la opción más cercana)", min_value=0, max_value=90, value=0, step=1)
    
    # Widget para ingresar el valor del balance inicial
    balance_inicial_input = st.number_input ("Balance Inicial:", min_value=0, max_value=1000000, value=100000, step=1000)
    
    # Widget para ingresar el valor del porcentaje de asignación de capital
    pct_allocation_input = st.number_input("Porcentaje de Asignación de Capital:", min_value=0.001, max_value=0.6, value=0.05, step=0.01)
    
    # Variable para seleccionar close to close o open to close
    close_to_close_option = st.selectbox("Seleccione el método de cálculo:", options=["close to close", "open to close"], index=0)
    
    # Fecha de inicio del periodo de backtest
    start_date = st.date_input("Fecha de inicio del periodo de backtest:", min_value=datetime(2020, 1, 1))
    
    # Fecha de finalización del periodo de backtest
    end_date = st.date_input("Fecha de finalización del periodo de backtest:", max_value=datetime.today())
    
    # Botón para ejecutar el backtest
    if st.button("Ejecutar Backtest"):
        close_to_close = close_to_close_option == "close to close"
        #realizar_backtest(r'C:\Users\Lenovo Thinkpad E14\Downloads/datos_8.xlsx', 'tXoXD_m9y_wE2kLEILzsSERW3djux3an', balance_inicial_input, pct_allocation_input,start_date, end_date, option_days_input, option_offset_input, close_to_close)
        realizar_backtest('datos_8.xlsx', 'tXoXD_m9y_wE2kLEILzsSERW3djux3an', balance_inicial_input, pct_allocation_input,start_date, end_date, option_days_input, option_offset_input, close_to_close)

if __name__ == "__main__":
    main()


