import pandas as pd
from google.protobuf.timestamp_pb2 import Timestamp
from firebase_admin import credentials, firestore
import firebase_admin
from datetime import datetime
import os

# Configuración de Firebase (usando credenciales desde GitHub Secrets)
def initialize_firebase():
    try:
        # Verifica que el archivo existe y es legible
        with open("firebase_key.json", "r") as f:
            content = f.read()
            if not content.strip():
                raise ValueError("El archivo firebase_key.json está vacío")
                
        cred = credentials.Certificate("firebase_key.json")
        return firestore.client()
    except Exception as e:
        print(f"❌ Error inicializando Firebase: {str(e)}")
        print("Contenido actual del archivo firebase_key.json:")
        with open("firebase_key.json", "r") as f:
            print(f.read())
        raise

# Función para obtener y procesar datos
def get_information_firebase(collection, db):
    documents = db.collection(collection).stream()
    data_list = []

    for doc in documents:
        data = doc.to_dict()
        pred_date = data.get("predDate")
        pred_direction = data.get("predDirection")
        direction = data.get("direction")
        result = data.get("result")
        
        # Procesamiento de fechas
        if isinstance(pred_date, Timestamp):
            pred_date = datetime.fromtimestamp(pred_date.seconds)
        if isinstance(pred_date, datetime) and pred_date.tzinfo is not None:
            pred_date = pred_date.replace(tzinfo=None)
        
        data_list.append({
            "date": pred_date,
            "Direction": direction,
            "toggle_false": pred_direction,
            "Resultado": result
        })
    
    # Crear DataFrame y ajustar datos
    df = pd.DataFrame(data_list)
    df["Direction"] = df["Direction"].shift(-1)
    df["Resultado"] = df["Resultado"].shift(-1)
    
    return df

# Función principal
def main():
    db = initialize_firebase()
    collections = ["spyVOC", "spyCanalSOC","spyMOC", "spyEnsembleVM", "spyEnsembleVS", "spySOC", "spyEnsembleVSM", "spyEnsembleEOE"]
    
    for collection in collections:
        try:
            df = get_information_firebase(collection, db)
            output_file = f"firebase_data_{collection}_automation.xlsx"
            
            # Guardar en Excel (en el repositorio)
            df.to_excel(output_file, index=True)
            print(f"Datos de {collection} guardados en {output_file}")
            
        except Exception as e:
            print(f"Error procesando {collection}: {str(e)}")

if __name__ == "__main__":
    main()