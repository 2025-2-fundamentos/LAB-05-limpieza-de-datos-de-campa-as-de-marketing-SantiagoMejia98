"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import pandas as pd
import os
import zipfile
import glob

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    ruta_zip = "files/input"
    ruta_final = 'files/output'
    completo = pd.DataFrame()
    client = pd.DataFrame()
    campaign = pd.DataFrame()
    economics = pd.DataFrame()
    
    zip_files = glob.glob(f"{ruta_zip}/*")


    for zip in zip_files:
        with zipfile.ZipFile(zip, 'r') as z:
            with z.open(z.namelist()[0]) as file:
                df = pd.read_csv(file)
                completo = pd.concat([completo, df], ignore_index=True)

    client["client_id"] = completo["client_id"].copy()
    client["age"] = completo["age"].copy()
    client["job"] = completo["job"].copy().str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    client["marital"] = completo["marital"].copy()
    client["education"] = completo["education"].copy().str.replace(".", "_", regex=False).replace("unknown", pd.NA)
    client["credit_default"] = completo["credit_default"].copy().apply(lambda x: 1 if x == "yes" else 0)
    client["mortgage"] = completo["mortgage"].copy().apply(lambda x: 1 if x == "yes" else 0)

    campaign["client_id"] = completo["client_id"].copy()
    campaign["number_contacts"] = completo["number_contacts"].copy()
    campaign["contact_duration"] = completo["contact_duration"].copy()
    campaign["previous_campaign_contacts"] = completo["previous_campaign_contacts"].copy()
    campaign["previous_outcome"] = completo["previous_outcome"].copy().apply(lambda x: 1 if x == "success" else 0)
    campaign["campaign_outcome"] = completo["campaign_outcome"].copy().apply(lambda x: 1 if x == "yes" else 0)
    campaign["last_contact_date"] = pd.to_datetime(completo["day"].astype(str) + "-" + completo["month"]  + "-2022", format="%d-%b-%Y").dt.strftime("%Y-%m-%d")

    economics["client_id"] = completo["client_id"].copy()
    economics["cons_price_idx"] = completo["cons_price_idx"].copy()
    economics["euribor_three_months"] = completo["euribor_three_months"].copy()

    if os.path.exists(ruta_final):
        for file in glob.glob(f"{ruta_final}/*"):
            os.remove(file)
        os.rmdir(ruta_final)
    os.makedirs(ruta_final)

    client.to_csv(os.path.join(ruta_final, "client.csv"), index=False)
    campaign.to_csv(os.path.join(ruta_final, "campaign.csv"), index=False)
    economics.to_csv(os.path.join(ruta_final, "economics.csv"), index=False)
    return    


if __name__ == "__main__":
    clean_campaign_data()
