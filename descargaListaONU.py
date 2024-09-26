from datetime import datetime
import os
import requests
import ssl
from requests.adapters import HTTPAdapter
import smtplib
import pandas as pd
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
import json
import pymysql
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()
class SSLAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.options |= ssl.OP_LEGACY_SERVER_CONNECT
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)

url = "https://scsanctions.un.org/resources/xml/en/consolidated.xml?_gl=1*jwjza9*_ga*NTg3NjQxODA1LjE3MjQ3ODM5OTY.*_ga_TK9BQL5X7Z*MTcyNjA3MjA3OS40LjEuMTcyNjA3MjQzNi4wLjAuMA"
session = requests.Session()
session.mount('https://', SSLAdapter())

response = session.get(url)

if response.status_code == 200:
    with open("consolidated.xml", "wb") as file:
        file.write(response.content)
    print("Download successful!")
    # Leer y analizar el archivo XML
    tree = ET.parse('consolidated.xml')
    root = tree.getroot()
    # Crear una lista para almacenar los datos
    data = []
    # Creamos una variable para almacenar la fecha de publicación de la lista
    publication_date_elem = root.attrib.get('dateGenerated')
    # Extraer el valor de la fecha de publicación
    if publication_date_elem is not None:
        publication_date = pd.to_datetime(publication_date_elem, errors='coerce')
        print(f"Fecha de publicación: {publication_date}")
    else:
        print("No se encontró el elemento PUBLICATION_DATE en el XML.")
    for individual in root.findall('.//INDIVIDUAL'):
        individual_data = {}
        for child in individual:
            if child.tag == 'INDIVIDUAL_DOCUMENT':
                for subchild in child:
                    if subchild.tag == 'TYPE_OF_DOCUMENT':
                        individual_data['DOCUMENT_TYPE'] = subchild.text
                    if subchild.tag == 'NUMBER':
                        individual_data['DOCUMENT_NUMBER'] = subchild.text
            else:
                if child.tag not in individual_data:
                    individual_data[child.tag] = child.text
                else:
                    if isinstance(individual_data[child.tag], list):
                        individual_data[child.tag].append(child.text)
                    else:
                        individual_data[child.tag] = [individual_data[child.tag], child.text]
        data.append(individual_data)
    # Convertir la lista de diccionarios en un DataFrame de pandas
    df_onu_individual = pd.DataFrame(data)
    data_entitites = []
    for entity in root.findall('.//ENTITY'):
        entity_data = {}
        for child in entity:
            if child.tag not in entity_data:
                entity_data[child.tag] = child.text
            else:
                if isinstance(entity_data[child.tag], list):
                    entity_data[child.tag].append(child.text)
                else:
                    entity_data[child.tag] = [entity_data[child.tag], child.text]
        data_entitites.append(entity_data)
    df_onu_entity = pd.DataFrame(data_entitites)
    # Crear la conexión a la base de datos
    engine = create_engine('mysql+pymysql://root:password@localhost:3306/tufondo_db')
    # Crear una lista para almacenar los datos
    data = []
    # Iterar sobre cada columna en el DataFrame
    for index, row in df_onu_individual.iterrows():
        individual_data = {
            'lista_control_id': 2,
            'tipo': 'Individual',
            'tipo_documento': row.get('DOCUMENT_TYPE'),
            'numero_documento': row.get('DOCUMENT_NUMBER'),
            'primer_nombre': row.get('FIRST_NAME'),
            'segundo_nombre': row.get('SECOND_NAME'),
            'primer_apellido': row.get('THIRD_NAME'),
            'segundo_apellido': row.get('FOURTH_NAME'),
            'es_colombiano': 1 if row.get('COUNTRY') == 'COLOMBIA' else 0,
            'data': json.dumps(row.to_dict())
        }
        data.append(individual_data)
    # Iterar sobre cada columna en el DataFrame
    for index, row in df_onu_entity.iterrows():
        entity_data = {
            'lista_control_id': 2,
            'tipo': 'Entity',
            'tipo_documento': row.get('TYPE_OF_DOCUMENT'),
            'numero_documento': row.get('NUMBER'),
            'primer_nombre': row.get('FIRST_NAME'),
            'segundo_nombre': row.get('SECOND_NAME'),
            'primer_apellido': row.get('THIRD_NAME'),
            'segundo_apellido': row.get('FOURTH_NAME'),
            'es_colombiano': 1 if row.get('COUNTRY') == 'COLOMBIA' else 0,
            'data': json.dumps(row.to_dict())
        }
        data.append(entity_data)
    # Convertir la lista de diccionarios en un DataFrame de pandas
    df = pd.DataFrame(data)
    # Actualizar un registro en la base de datos
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME'),
    }

    # Establecer la conexión y ejecutar la consulta
    try:
        connection = pymysql.connect(**db_config)
        with connection.cursor() as cursor:
            query = "UPDATE controlVigilancia_listas_control SET fecha_publicacion = %s, updated_at = NOW() WHERE id = 2"
            data = (publication_date,)
            cursor.execute(query, data)
            connection.commit()
            print("Fecha de publicación y fecha de actualización actualizadas exitosamente!")
    except Exception as e:
        print(f"Error al actualizar la base de datos: {e}")
    finally:
        connection.close()

    try:
        connection = pymysql.connect(**db_config)
        with connection.cursor() as cursor:
            query = "DELETE FROM controlVigilancia_detalles_lista_control WHERE lista_control_id = 2"
            cursor.execute(query)
            connection.commit()
            print("Registros eliminados exitosamente!")
    except Exception as e:
        print(f"Error al eliminar los registros: {e}")
    finally:
        connection.close()
    
    # Insertar los datos en la base de datos
    df.to_sql('controlVigilancia_detalles_lista_control', con=engine, if_exists='append', index=False)
    print("Datos insertados en la base de datos.")
    sender_email = os.getenv('EMAIL_SENDER')
    password = os.getenv('EMAIL_APP_PASSWORD')
    receiver_email = os.getenv('EMAIL_RECEIVER')
    subject = "Sanciones ONU - Descarga Exitosa"
    message = EmailMessage()
    message['Subject'] = subject
    message['From'] = sender_email
    message['To'] = receiver_email
    message.set_content(f"La descarga de la lista de sanciones de la ONU se ha realizado exitosamente.\n Fecha de publicación: {publication_date}\n Registros insertados en la base de datos.\n Hora de ejecución: {datetime.now()}")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, password)
        server.send_message(message)
        server.quit()
    print("Se ha enviado un correo de notificación.")
else:
    print(f"Failed to download file. Status code: {response.status_code}")
    sender_email = os.getenv('EMAIL_SENDER')
    password = os.getenv('EMAIL_APP_PASSWORD')
    receiver_email = os.getenv('EMAIL_RECEIVER')
    message = EmailMessage()
    message['Subject'] = "Error: Fallo en la descarga de la lista de sanciones de la ONU"
    message['From'] = sender_email
    message['To'] = receiver_email
    message.set_content(f"No se pudo descargar el archivo de la lista de sanciones de la ONU. Status code: {response.status_code}\n Fallo en la fecha {datetime.now()}")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, password)
        server.send_message(message)
        server.quit()
    print("Se ha enviado un correo de notificación.")