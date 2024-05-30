import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta, MO, FR
from sklearn.metrics import confusion_matrix


def accuracy_spy(start_date, name_date, end_date=None):

    # Set end_date to today if not provided
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    datos = pd.read_excel(
        r"D:\laura\Desktop\Andres Florez\ARCAD\SPY-Valentina-Open\hist_luisa\modelv5\out\accuracy\final_df.xlsx")

    datos = datos[(datos['L0-date'] >= start_date)
                  & (datos['L0-date'] <= end_date)]
    datos = datos.reset_index(drop=True)
    datos = datos[['L0-date', 'L0-direction_SPY',
                   'prediction', 'L0-open_SPY', 'L0-close_SPY']]
    datos['acierto'] = np.where(
        datos['L0-direction_SPY'] == datos['prediction'], 1, 0)
    # desempeño de modelo en entrenamiento
    datos['asertividad'] = datos['acierto'].sum()/len(datos['acierto'])
    datos['cumsum'] = datos['acierto'].cumsum()
    # desempeño portafolio acumulado importante si definimos un inicio
    datos['accu'] = datos['cumsum']/(datos.index + 1)

    # Muestra el DataFrame actualizado
    datos['open_to_close_pct'] = datos['L0-close_SPY']/datos['L0-open_SPY'] - 1

    # Calcula la ganancia
    datos['Ganancia'] = datos.apply(lambda row: abs(
        row['open_to_close_pct']) if row['acierto'] else -abs(row['open_to_close_pct']), axis=1)

    # Calcula la ganancia acumulada
    datos['Ganancia_Acumulada'] = datos['Ganancia'].cumsum()

    # f1 Score
    conf_matrix = confusion_matrix(
        datos['L0-direction_SPY'], datos['prediction'])

    # Calculate F1-score
    tp, fp, fn, tn = conf_matrix.ravel()
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

    datos.to_excel(
        f"D:\laura\Desktop\Andres Florez\ARCAD\SPY-Valentina-Open\hist_luisa\modelv5\out\Accuracy\Accuracy_{name_date}.xlsx")
    return


# Semana vursatil anterior
# Calcular el lunes anterior
today = datetime.now()
lunes_anterior = today - timedelta(days=today.weekday())
# Restar una semana para obtener el lunes antes del anterior
lunes_anterior -= timedelta(weeks=1)
# Calcular el domingo anterior
viernes_anterior = lunes_anterior + timedelta(days=4)
# Analizar datos para la semana anterior (desde el lunes antes del actual hasta el viernes anterior)
start_date = lunes_anterior.strftime('%Y-%m-%d')
end_date = viernes_anterior.strftime('%Y-%m-%d')
accuracy_spy(start_date, "_semana_anterior", end_date)

# Últimos 15 días vursatiles
today = datetime.now()
# Calcular el primer día del mes anterior
primer_dia_mes_anterior = today - relativedelta(weekday=MO(-3))
# Calcular el último día del mes anterior
viernes_pasado = today - relativedelta(weekday=FR(-1))
# Analizar datos para el mes pasado (desde el primer día hasta el último día del mes anterior)
start_date = primer_dia_mes_anterior.strftime('%Y-%m-%d')
end_date = viernes_pasado.strftime('%Y-%m-%d')
accuracy_spy(start_date, "15dias_vursatiles", end_date)

# Últimos 30 dias vursatiles
primer_dia_mes_actual = datetime.now().replace(day=1)
# Calcular el primer día del mes anterior
primer_dia_mes_anterior = primer_dia_mes_actual - relativedelta(months=+1)
# Calcular el último día del mes anterior
ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)
# Analizar datos para el mes pasado (desde el primer día hasta el último día del mes anterior)
start_date = primer_dia_mes_anterior.strftime('%Y-%m-%d')
end_date = ultimo_dia_mes_anterior.strftime('%Y-%m-%d')
accuracy_spy(start_date, "mes_anterior", end_date)

# Últimos 90 dias vursatil
primer_dia_mes_actual = datetime.now().replace(day=1)
# Calcular el primer día hace tres meses
dia_uno_tres_meses_atras = primer_dia_mes_actual - relativedelta(months=+3)
# Calcular el último día del mes anterior
ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)
# Analizar datos para el último mes (desde el primer día hasta el último día del mes anterior)
start_date = dia_uno_tres_meses_atras.strftime('%Y-%m-%d')
end_date = ultimo_dia_mes_anterior.strftime('%Y-%m-%d')
accuracy_spy(start_date, "trimestre_anterior", end_date)

# Personalizado
start_date = '2011-06-24'
accuracy_spy(start_date)
