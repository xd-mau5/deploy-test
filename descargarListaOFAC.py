from datetime import datetime
from email.message import EmailMessage
import requests
import smtplib
import pandas as pd
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine
import json
import pymysql

# URL de la lista de sanciones de OFAC en formato XML
url = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.XML"

# Realizar la solicitud HTTP para descargar el archivo
response = requests.get(url)

# Guardar el archivo descargado en el disco
if response.status_code == 200:
    with open("OFAC.xml", "wb") as file:
        file.write(response.content)
    print("Descarga exitosa!")
    # Definir el namespace
    namespace = {'ns': 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML'}
    # Leer y analizar el archivo XML
    tree = ET.parse('OFAC.XML')
    root = tree.getroot()
    # Crear una lista para almacenar los datos
    data = []
    # Buscar todas las entradas sdnEntry con el namespace
    entries = root.findall('.//ns:sdnEntry', namespace)
    # Obtener la fecha de publicación de la lista
    publication_date_elem = root.find('.//ns:Publish_Date', namespace)
    if publication_date_elem is not None:
        publication_date = publication_date_elem.text
    else:
        publication_date = None
    for entry in entries:
        entry_data = {}
        for child in entry:
            # Añadir el namespace al tag en las consultas
            if child.tag == '{https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML}idList':
                entry_data['IDList'] = []
                for id_elem in child.findall('.//ns:id', namespace):
                    id_data = {
                        'ID_UID': id_elem.find('ns:uid', namespace).text,
                        'IDType': id_elem.find('ns:idType', namespace).text,
                        'IDNumber': id_elem.find('ns:idNumber', namespace).text,
                        'IDCountry': id_elem.find('ns:idCountry', namespace).text if id_elem.find('ns:idCountry', namespace) is not None else None
                    }
                    entry_data['IDList'].append(id_data)
            elif child.tag == '{https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML}akaList':
                entry_data['AKAList'] = []
                for aka_elem in child.findall('.//ns:aka', namespace):
                    aka_data = {
                        'AKA_UID': aka_elem.find('ns:uid', namespace).text,
                        'Type': aka_elem.find('ns:type', namespace).text,
                        'Category': aka_elem.find('ns:category', namespace).text,
                        'FirstName': aka_elem.find('ns:firstName', namespace).text if aka_elem.find('ns:firstName', namespace) is not None else None,
                        'LastName': aka_elem.find('ns:lastName', namespace).text
                    }
                    entry_data['AKAList'].append(aka_data)
            elif child.tag == '{https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML}dateOfBirthList':
                dob_elem = child.find('.//ns:dateOfBirthItem/ns:dateOfBirth', namespace)
                if dob_elem is not None:
                    entry_data['DateOfBirth'] = dob_elem.text
            elif child.tag == '{https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML}placeOfBirthList':
                pob_elem = child.find('.//ns:placeOfBirthItem/ns:placeOfBirth', namespace)
                if pob_elem is not None:
                    entry_data['PlaceOfBirth'] = pob_elem.text
            else:
                entry_data[child.tag.replace('{https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML}', '')] = child.text
        data.append(entry_data)
    df = pd.DataFrame(data)
    # Crear la conexión a la base de datos
    engine = create_engine('mysql+pymysql://root:password@localhost:3306/tufondo_db')
    # Crear una lista para almacenar los datos
    data = []
    # Iterar sobre cada fila en el DataFrame para "Individual"
    for index, row in df.iterrows():
        id_list = row.get('IDList')
        tipo = row.get('sdnType')
        tipo_documento = None
        numero_documento = None
        pais = None

        # Validar que IDList es una lista válida y no esté vacía
        if isinstance(id_list, list) and len(id_list) > 0:
            first_id = id_list[0]
            tipo_documento = first_id.get('IDType')
            numero_documento = first_id.get('IDNumber')
            pais = first_id.get('IDCountry')

        individual_data = {
            'lista_control_id': 1,
            'tipo': tipo,
            'tipo_documento': tipo_documento,
            'numero_documento': numero_documento,
            'primer_nombre': row.get('firstName'),
            'segundo_nombre': None,
            'primer_apellido': row.get('lastName'),
            'segundo_apellido': None,
            'es_colombiano': (1 if pais == 'Colombia' else 0),
            'data': json.dumps(row.to_dict())
        }
        data.append(individual_data)

    # Convertir la lista de diccionarios en un DataFrame de pandas
    df = pd.DataFrame(data)
    publication_date = pd.to_datetime(publication_date, errors='coerce')
    print(f"Publication Date: {publication_date}")
    # Configuración de la conexión a la base de datos
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'password',
        'database': 'tufondo_db'
    }

    # Establecer la conexión y ejecutar la consulta
    try:
        connection = pymysql.connect(**db_config)
        with connection.cursor() as cursor:
            query = "UPDATE controlVigilancia_listas_control SET fecha_publicacion = %s, updated_at = NOW() WHERE id = 1"
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
            query = "DELETE FROM controlVigilancia_detalles_lista_control WHERE lista_control_id = 1"
            cursor.execute(query)
            connection.commit()
            print("Registros eliminados exitosamente!")
    except Exception as e:
        print(f"Error al eliminar los registros: {e}")
    finally:
        connection.close()

    # Insertar los datos en la base de datos
    df.to_sql('controlVigilancia_detalles_lista_control', con=engine, if_exists='append', index=False)
    print("Datos insertados exitosamente en la base de datos!")
    sender_email = "sistemas@tufondo.net"
    password = 'ndntjtqvgkodxlfc'
    receiver_email = 'omar.izquierdo@tdpsolutions.co'
    subject = "Sanciones OFAC - Descarga Exitosa"
    message = EmailMessage()
    message['Subject'] = subject
    message['From'] = sender_email
    message['To'] = receiver_email
    message.set_content(f"La descarga de la lista de sanciones de OFAC se ha realizado exitosamente.\n Fecha de publicación: {publication_date}\n Registros insertados en la base de datos.\n Hora de ejecución: {datetime.now()}")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, password)
        server.send_message(message)
        server.quit()
    print("Se ha enviado un correo de notificación.")
else:
    print("Error al descargar el archivo. Código de estado:", response.status_code)
    sender_email = "sistemas@tufondo.net"
    password = 'ndntjtqvgkodxlfc'
    receiver_email = 'omar.izquierdo@tdpsolutions.co'
    subject = "Error: Fallo en la descarga de la lista de sanciones de la ONU"
    message = EmailMessage()
    message['Subject'] = subject
    message['From'] = sender_email
    message['To'] = receiver_email
    message.set_content(f"No se pudo descargar el archivo de la lista de sanciones de la OFAC. Status code: {response.status_code}\n Fallo en la fecha {datetime.now()}")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender_email, password)
        server.send_message(message)
        server.quit()
    print("Se ha enviado un correo de notificación.")