import pandas as pd
from google.protobuf.timestamp_pb2 import Timestamp
from firebase_admin import credentials, firestore
import firebase_admin
from datetime import datetime
import os
import numpy as np

# Configuración de Firebase (usando credenciales desde GitHub Secrets)
def initialize_firebase():
    cred = credentials.Certificate(r"C:\Users\Lenovo Thinkpad E14\Documents\ARKAD\MODEL-VOLUMEN\money-lab-d086e-f157c6cfe6ec.json")
    #cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
    return firestore.client()

# Función para obtener y procesar datos
def get_information_firebase(collection,db):
    collection_name = collection
    documents = db.collection(collection_name).stream()
 
    data_list = []
    for doc in documents:
        data = doc.to_dict()  # Convertir el documento en un diccionario
        pred_date = data.get("predDate")
        pred_direction = data.get("predDirection")
        direction = data.get("direction")
        result = data.get("result")
        vector = data.get("vector")
 
        if vector is None:
            vector = np.array([100, 0]) if pred_direction == 0 else np.array([0, 100])
        else:
            vector = np.array(vector, dtype=float)  # Asegura que sea tipo float y numpy array
            if vector[0] <= 1 and vector[1] <= 1:
                vector *= 100
 
        
        if isinstance(pred_date, Timestamp):
            pred_date = datetime.fromtimestamp(pred_date.seconds)
        if isinstance(pred_date, datetime) and pred_date.tzinfo is not None:
            pred_date = pred_date.replace(tzinfo=None)
        
        data_list.append({
        "date": pred_date,
        "Direction": direction,
        "toggle_false": pred_direction,
        "Resultado": result,
        "proba0": vector[0]/100,
        "proba1": vector[1]/100
    })
        
    df = pd.DataFrame(data_list)
    df["Direction"] = df["Direction"].shift(-1)
    df["Resultado"] = df["Resultado"].shift(-1)
    #print(df)
    output_file = f"firebase_data_from_{collection_name}.xlsx"
    df.to_excel(output_file, index=True)
    
 
    print(f"Datos guardados en el archivo {output_file}")
    
    return df


# Función principal
def main():
    db = initialize_firebase()
    collections = ["spyVOC", "spyCanalSOC","spyMOC", "spyEnsembleVM", "spyEnsembleVS", "spySOC", "spyEnsembleVSM", "spyEnsembleEOE", "spyEnsembleVSMO", "spyEnsembleEOE+VSMO"]
    
    for collection in collections:
        try:
            df = get_information_firebase(collection, db)
            output_file = f"firebase_data_{collection}.xlsx"
            
            # Guardar en Excel (en el repositorio)
            #df.to_excel(output_file, index=True)
            #print(f"Datos de {collection} guardados en {output_file}")
            
        except Exception as e:
            print(f"Error procesando {collection}: {str(e)}")

if __name__ == "__main__":
    main()
