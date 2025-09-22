""" Script permettant de récupérer des données météo placées dans un bucket S3
    et les importer dans une base MongoDB
"""
import os
import sys
import boto3
import pytz
import pandas as pd
import pymongo
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv

############# Fonctions de traitement des données
def to_percent(p_str):
    """ Conversion en int
    """
    # Nettoyage de la string
    p_clean = p_str.replace('\xa0', '').replace('%', '').strip()
    return int(p_clean)

def fahrenheit_to_celsius(f_str):
    """ Conversion en degrés Celsius
    """
    # Nettoyage de la string
    f_clean = f_str.replace('\xa0', '').replace('°F', '').strip()
    f_val = float(f_clean)
    # Conversion en Celsius
    c_val = (f_val - 32) * 5 / 9
    return round(c_val, 2)

def mph_to_kmh(speed_str):
    """ Conversion en km/h
    """
    # Clean the string
    mph_clean = speed_str.replace('\xa0', '').replace('mph', '').strip()
    mph_val = float(mph_clean)
    # Convert to km/h
    kmh_val = mph_val * 1.60934
    return round(kmh_val, 2)

def inhg_to_hpa(pressure_str):
    """ Conversion en hPa
    """
    # Clean the string
    inhg_clean = pressure_str.replace('\xa0', '').replace('in', '').strip()
    inhg_val = float(inhg_clean)
    # Convert to hPa
    hpa_val = inhg_val * 33.8639
    return round(hpa_val, 2)

def utc_time_1(time_str):
    """ Conversion d'une string date en date python
    """
    # Combine date and time, then localize
    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

def utc_time_2(time_str):
    """ Conversion d'une heure en date
    Time zone: Paris, Bruxelles
    """
    paris_tz = pytz.timezone("Europe/Paris")
    
    # Use today's date
    date_str = datetime.now().strftime("%Y-%m-%d")
    
    # Combine date and time, then localize
    full_str = f"{date_str} {time_str}"
    paris_dt = paris_tz.localize(datetime.strptime(full_str, "%Y-%m-%d %H:%M:%S"))
    
    # Convert to UTC
    utc_dt = paris_dt.astimezone(pytz.utc)
    return utc_dt

def wind_direction_to_degrees(wind_dir):
    """ Conversion d'une direction de vent en degrés
    """
    compass_map = {
        "N": 0, "NORTH": 0, "NNE": 22.5, "NE": 45, "ENE": 67.5,
        "E": 90, "EAST": 90, "ESE": 112.5, "SE": 135, "SSE": 157.5,
        "S": 180, "SOUTH": 180, "SSW": 202.5, "SW": 225, "WSW": 247.5,
        "W": 270, "WEST": 270, "WNW": 292.5, "NW": 315, "NNW": 337.5
    }

    if isinstance(wind_dir, str):
        wd_clean = wind_dir.upper().strip()
        if wd_clean in compass_map:
            return compass_map[wd_clean]
        else:
            raise ValueError(f"Unknown compass direction: {wind_dir}")
    elif isinstance(wind_dir, (int, float)):
        return float(wind_dir)
    else:
        raise TypeError(f"Unsupported wind direction type: {type(wind_dir)}")

def clean_solar(solar_str):
    """ Nettoie la string pour en faire un float
    """
    solar_str_clean = solar_str.replace('\xa0', '').replace('w/m²', '').strip()
    return float(solar_str_clean)

def precip_in_to_mm(precip_str):
    """ Conversion d'une données de inches en mm
    """
    clean = precip_str.replace('\xa0', '').replace('in', '').strip()
    val_in = float(clean)
    return round(val_in * 25.4, 2)

def map_records(report_list, station_list):
    """ Renommage et conversion des champs des jeux de données pour être en accord
        avec le schéma de données
    """
    reports = []
    errors = []
    for report in report_list:
        if (isinstance(report, str)):
            report = report.replace(':null', ':None').strip()
            eval_report = eval(report)
            copy_report = eval_report.copy()
        elif (isinstance(report, dict)):
            eval_report = report
            copy_report = report.copy()
        else:
            errors.append(f"Unknown report type: {report}")
            break
        last_key = ""
        try:
            # Renommage et conversion des champs
            for key in eval_report.keys():
                last_key = key
                val = copy_report.pop(key)
                if key not in report_mapping:
                    raise Exception(f"Unknown {key} key in report")
                copy_report[report_mapping[key]['field']] = report_mapping[key]['function_map'](val) if val else None
            # L'ID de la station est-il connu?
            station_id = copy_report['station_id']
            if station_id not in station_list:
                errors.append(f"Unknown station id: {station_id}")
                break
            # Ajout du dictionnaire dans la liste des relevés
            reports.append(copy_report)
        except Exception as e:
            errors.append(f"{station_id}, {last_key}: {str(e)}")
    return reports, errors

def log(log_collection, log_type, msg):
    """ Insertion d'un log dans la collection logs
    """
    print(msg)
    log_collection.insert_one(
        {
            'timestamp': datetime.now(),
            'type': log_type,
            'msg': msg
        }
    )

## Mapping permettant de renommer et convertir les champs
report_mapping = {
    'id_station': {'field': 'station_id', 'function_map': str},
    'station_id': {'field': 'station_id', 'function_map': str},
    'dh_utc': {'field': 'utc_time', 'function_map': utc_time_1},
    'Time': {'field': 'utc_time', 'function_map': utc_time_2},
    'temperature': {'field': 'temperature', 'function_map': float},
    'Temperature': {'field': 'temperature', 'function_map': fahrenheit_to_celsius},
    'pression': {'field': 'pressure', 'function_map': float},
    'Pressure': {'field': 'pressure', 'function_map': inhg_to_hpa},
    'humidite': {'field': 'humidity', 'function_map': to_percent},
    'Humidity': {'field': 'humidity', 'function_map': to_percent},
    'point_de_rosee': {'field': 'dew_point', 'function_map': float},
    'Dew Point': {'field': 'dew_point', 'function_map': fahrenheit_to_celsius},
    'visibilite': {'field': 'horizontal_visibility', 'function_map': int},
    'vent_moyen': {'field': 'mean_wind_speed', 'function_map': float},
    'Speed': {'field': 'mean_wind_speed', 'function_map': mph_to_kmh},
    'vent_rafales': {'field': 'wind_gust', 'function_map': float},
    'Gust': {'field': 'wind_gust', 'function_map': mph_to_kmh},
    'vent_direction': {'field': 'wind_direction', 'function_map': int},
    'Wind': {'field': 'wind_direction', 'function_map': wind_direction_to_degrees},
    'pluie_3h': {'field': 'precipitation_3h', 'function_map': float},
    'pluie_1h': {'field': 'precipitation_1h', 'function_map': float},
    'neige_au_sol': {'field': 'snow_depth', 'function_map': float},
    'nebulosite': {'field': 'ncloud_cover', 'function_map': float},
    'temps_omm': {'field': 'temps_omm', 'function_map': float},
    'UV': {'field': 'uv', 'function_map': int},
    'Solar': {'field': 'solar', 'function_map': clean_solar},
    'Precip. Accum.': {'field': 'precipitation_accum', 'function_map': precip_in_to_mm},
    'Precip. Rate.': {'field': 'precipitation_rate', 'function_map': precip_in_to_mm}   
}

def main():
    # Chargement de l'environnement
    load_dotenv()
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    server = os.getenv('DB_SERVER')
    db_name = os.getenv('DB_NAME')
    
    # Connexion à la base de données
    try:
        client = pymongo.MongoClient(server)
    except Exception as e:
        print("Impossible de se connecter à la base de données:", str(e))
        return
    
    # Création ou récupération de la base de données 
    db = client[db_name]

    stations_col = None
    logs_col = None
    # Création ou récupération des collections
    try:
        stations_col = db.create_collection('stations', capped=False)
    except Exception:
        # Existe dèjà
        stations_col = db['stations']
    try:
        logs_col = db.create_collection('logs', capped=False)
    except Exception:
        # Existe dèjà
        logs_col = db['logs']

    log(logs_col, 'info', f"Début des synchronisations")
    # Creation session S3
    # Utilisation d'une clé d'accès créée spécifiquement pour télécharger les données
    log(logs_col, 'info', 'Récupération des données météo à partir du S3 bucket...')
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='eu-west-3'
    )
    s3 = session.client('s3')
    # Lecture des données
    bucket_name = 'meteo-nau81'
    # Station Ichtegem
    folder_prefix = 'meteo_sync/ichtegem/'
    # Liste des fichiers sous le même dossier
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            ichtegem_response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
            # Un seul fichier en principe, mais au cas où
            break
    else:
        ichtegem_response = None
    # Station Madeleine
    folder_prefix = 'meteo_sync/madeleine/'
    # Liste des fichiers sous le même dossier
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            madeleine_response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
            # Un seul fichier en principe, mais au cas où
            break
    else:
        madeleine_response = None
    # Station InfoClimat
    folder_prefix = 'meteo_sync/infoclimat/'
    # Liste des fichiers sous le même dossier
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            # Seulement le premier fichier pour l'instant
            infoclimat_response = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
            # Un seul fichier en principe, mais au cas où
            break
    else:
        infoclimat_response = None
    # Fermeture de la session
    s3.close()
    # Création des dataframe
    df_ichtegem = pd.read_csv(StringIO(ichtegem_response['Body'].read().decode('utf-8')), delimiter=',') if ichtegem_response else None
    df_madeleine = pd.read_csv(StringIO(madeleine_response['Body'].read().decode('utf-8')), delimiter=',') if madeleine_response else None
    df_infoclimat = pd.read_json(StringIO(infoclimat_response['Body'].read().decode('utf-8'))) if infoclimat_response else None
    # Récupération des stations météo
    stations = df_infoclimat['_airbyte_data'].to_dict()['stations']
    # Ajout des stations amateurs
    stations.append(
        {
            'id': 'IICHTE19',
            'name': 'WeerstationBS',
            'city': 'Ichtegem',
            'latitude': 51.092,
            'longitude': 2.999,
            'elevation': 15,
            'hardware': 'other',
            'software': 'EasyWeatherV1.6.6'
        }
    )
    stations.append(
        {
            'id': 'ILAMAD25',
            'name': 'La Madeleine',
            'city': 'La Madeleine',
            'latitude': 50.659,
            'longitude': 3.07,
            'elevation': 23,
            'hardware': 'other',
            'software': 'EasyWeatherPro_V5.1.6'
        } 
    )
    # Renommage de la colonne id en _id pour MongoDB
    for station in stations:
        if 'id' in station.keys():
            station['_id'] = station.pop('id')
    # Création d'un dataframe des stations météo
    df_stations = pd.DataFrame.from_records(stations, index='_id')
    
    # Initialisation des listes de relevés et des erreurs
    all_reports = []
    all_errors = []
    # Récupération des relevés Infoclimat
    station_list = df_stations.index.to_list()
    df_infoclimat_dict = df_infoclimat['_airbyte_data'].to_dict()
    df_infoclimat_dict_reports = df_infoclimat_dict['hourly']
    for station in df_infoclimat_dict_reports:
        if station in station_list:
            reports, errors = map_records(df_infoclimat_dict_reports[station], station_list)
            all_reports.extend(reports)
            all_errors.extend(errors)
            for e in errors:
                log(logs_col, 'error', str(e))
    # Récupération des relevés d'Ichtegem
    reports, errors = map_records(df_ichtegem['_airbyte_data'].to_list(), station_list)
    for e in errors:
        log(logs_col, 'error', str(e))
    all_reports.extend(reports)
    all_errors.extend(errors)
    # Récupération des relevés de La Madeleine
    reports, errors = map_records(df_madeleine['_airbyte_data'].to_list(), station_list)
    for e in errors:
        log(logs_col, 'error', str(e))
    all_reports.extend(reports)
    all_errors.extend(errors)
    #df_meteo = pd.DataFrame(all_reports)

    # Connexion à la base MongoDB
    try:
        # Récupération de la collection Stations
        log(logs_col, 'info', 'Synchronisation des stations météo...')
        # Insertion des stations
        for station in stations:
            already_exists = stations_col.find_one({'_id': station['_id']})
            if not already_exists:
                stations_col.insert_one(station)
        # Insertion des relevés
        log(logs_col, 'info', 'Synchronisation des relevés météo...')
        for report in all_reports:
            station_id = report.pop('station_id')
            stations_col.update_one(
                {
                    "_id": station_id,
                    "reports.utc_time": {"$ne": report["utc_time"]}
                },
                {
                    "$push": {"reports": report}
                }
            )
    except Exception as e:
        log(logs_col, 'error', str(e))
    finally:
        log(logs_col, 'info', f"Fin des synchronisations : {len(all_errors)} erreur(s) sur {len(all_reports)} relevés")
        client.close()

if __name__ == '__main__':
    sys.exit(main())