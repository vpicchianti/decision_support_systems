#!/usr/bin/env python
# coding: utf-8


import datetime
import json
from xml.etree import ElementTree as ET
import reverse_geocode as rg
import pycountry_convert as pc
import csv


# FUNCTIONS

def is_already_in_table(diz_row, table, generare_ID):
        
    ''' this function checks if the given row (diz_row) is already present in the table

    Args:
        - diz_row (dict): the row to check
        - table (dict): table to check against
        - generare_ID (bool): whether to generate an ID or if the ID is already provided (True if we need to generate it)

    Returns:
        - int or None: If the row is already present, returns the corresponding ID
                      If the row is not present, returns None
    '''
        
    if not generare_ID: # already have ID in Police.csv, no need to create  - e.g. Date 
        # check whether the ID is already in table
        id_to_check = None
        for k, v in diz_row.items():  
            if k.endswith('_fk') or k.endswith('_id'):
                id_to_check = v  
                break 
        if id_to_check in table.keys():
            return id_to_check
        return None 
     
    else: # the case where ID needs to be generated 
        valori_row = list(diz_row.values())
        for k, v in table.items():
            if valori_row == v[:-1]:
                return int(k)
    return None


def add_record(diz_row, table, generare_ID):

    '''this function adds a record (row) to the corrisponding table

    Args:
        - diz_row (dict): the row to add
        - table (dict): the table to add the row to
        - generare_ID (bool): whether to generate an ID or if the ID is already provided (True if we need to generate it)

    Returns:
        - int : the ID of the added row, whether it is the one already provided or the newly created one
    '''

    id_key = None

    for k, v in diz_row.items():
        if k.endswith('_fk') or k.endswith('_id'):
            id_key = k
            break

    if not table:   # lempty table
        if generare_ID:
            table[1] = list(diz_row.values()) + [1]
            return 1
        else:
            if id_key is not None:
                id = diz_row[id_key]
                table[id] = [v for k, v in diz_row.items() if k != id_key] + [id]
                return id
            else:
                return None

    already_in = is_already_in_table(diz_row, table, generare_ID)
    if already_in is None:
        if generare_ID: # incremental ID 
            pk = list(table.keys())[-1] + 1
            table[pk] = list(diz_row.values())  + [pk]
            return pk 
        else:  # e.g. Date
            id = None
            for k, v in diz_row.items():  
                if k.endswith('_fk') or k.endswith('_id'):
                    id = v  
                    break
            table[id] =  [v for k, v in diz_row.items() if k != id_key] + [id]
            return id

    else: # already_in not None 

        return already_in


def data_into_tables(rows, tables):

    '''this function insert data into tables

    Args:
        - rows (list): list of rows to insert
        - tables (dict): dictionary containing table names as keys and tuples (table, generate_ID) as values 

    Returns:
        - list: list of IDs corresponding to the inserted rows (these represent foreing keys in Custody)
    '''
        
    ids = []
    for table_name, row in zip(tables.keys(), rows):
        table, generate_ID = tables[table_name] 
        if table_name == 'geography':
            geo_key = (row['latitude'], row['longitude'])
            if geo_key in table:
                geo_fk = table[geo_key]
            else:
                # generate a new geo_id if not found
                new_geo_id = len(table) + 1
                table[geo_key] = new_geo_id 
                geo_fk = new_geo_id
            ids_line = geo_fk  # assign geo_fk to ids_line

        else:
            ids_line = add_record(row, table, generate_ID)
        ids.append(ids_line)
    return ids



def compute_gravity(age, type, status, partecipant_info):
    
    ''' this functions compute the crime gravity attribute for a given participant

    Args:
        - age (dict): dictionary representing the age-related factors in the gravity computation
        - type (dict): dictionary representing the type-related factors in the gravity computation
        - status (dict): dictionary representing the status-related factors in the gravity computation
        - partecipant_info (dict): dictionary containing information about the participant, including:
            - 'participant_age_group' (str): age group of the participant
            - 'participant_type' (str): type of the participant
            - 'participant_status' (str): status of the participant

    Returns:
        - float: the computed crime gravity value based on the given factors and participant information
    '''

    partecipant_age = partecipant_info.get('participant_age_group', None)
    partecipant_type = partecipant_info.get('participant_type', None)
    partecipant_status = partecipant_info.get('participant_status', None)
    crime_gravity = age[partecipant_age] * type[partecipant_type] * status[partecipant_status]

    return crime_gravity




def process_police(file_police, dimensional_tables, incident, custody, age, type, status): 

    '''this function process the police data from a CSV file, updating dimensional tables and incident/custody dictionaries

    Args:
        - file_police (str): path to the CSV file containing police data
        - dimensional_tables (dict): dictionary containing dimensional tables (partecipant, gun, geo, date)
        - incident (set): set to store unique incident IDs
        - custody (dict): dictionary to store custody records
        - age (dict): dictionary representing age-related factors for computing crime gravity
        - type (dict): dictionary representing type-related factors for computing crime gravity
        - status (dict): dictionary representing status-related factors for computing crime gravity

    Returns:
        None
    '''
        
    with open(file_police, 'r') as police:
        header = None
        for line in police:
            if header is None:
                header = line.strip().split(',')
            else:
                row_values = line.strip().split(',')
                row_dict = {header[i]: row_values[i] for i in range(len(header))}

                partec_info = {
                    'participant_age_group': row_dict['participant_age_group'],
                    'participant_gender': row_dict['participant_gender'],
                    'participant_status': row_dict['participant_status'],
                    'participant_type': row_dict['participant_type']
                }
                geo_info = {
                    'latitude': float(row_dict['latitude']),
                    'longitude': float(row_dict['longitude'])
                }
                gun_info = {
                    'gun_stolen': row_dict['gun_stolen'],
                    'gun_type': row_dict['gun_type']
                }
                date_info = {
                    'date_fk': int(row_dict['date_fk'])
                }


                # add data into dimension tables
                partecipant_fk, gun_fk, geo_fk, date_fk  = data_into_tables([partec_info, gun_info, geo_info, date_info],  dimensional_tables)
                
                # add incident info
                incident_id = int(row_dict['incident_id'])
                incident.add(incident_id)

                # compute crime_gravity 
                crime_gravity = compute_gravity(age, type, status, partec_info)
                
                custody_id = int(row_dict['custody_id'])
                custody_record = [custody_id, partecipant_fk, gun_fk, geo_fk, date_fk, crime_gravity, incident_id] 
                custody[custody_id] = custody_record



def read_xml_file(file_path):
    ''' ths function reads data from an XML file and extract relevant information

    Args:
        - file_path (str): path to the XML file

    Returns:
        - list: list of dictionaries containing extracted data
    '''
        
    xml_data = []
    tree = ET.parse(file_path)
    root = tree.getroot()
    for row in root.findall('row'):
        data = {
            'date': row.find('date').text,
            'date_pk': row.find('date_pk').text
        }
        xml_data.append(data)
    return xml_data




def enrich_date(date_table, xml_data):
    '''this functions enrich the date table with information from XML data

    Args:
        - date_table (dict): the date table to enrich
        - xml_data (list): list of dictionaries containing XML data with 'date' and 'date_pk' keys

    Returns:
        None
    '''
    for row in xml_data:
        date_pk = int(row['date_pk'])
        date = row['date'].split()[0].replace('-', '') # we want date to be in format YYYYMMDD without '-' or '/'

        date_object = datetime.datetime.strptime(date, '%Y%m%d')

        # extract year, month, and day 
        year = date_object.year
        month = date_object.month
        day = date_object.day

        # compute quarter 
        quarter = (month - 1) // 3 + 1

        # compute the name of the weekday as a string 
        datetime_obj = datetime.date(year, month, day)
        day_of_week_str = datetime_obj.strftime("%A")

        date_table[date_pk].extend([date, day, month, year, quarter, day_of_week_str])



def enrich_geography(geography):

    ''' this function enriches the geography dictionary with additional information (city, country, continent, lat, long) using reverse geocode

    Args:
        - geography (dict): a dictionary where keys are coordinate tuples (latitude, longitude)
                            and values are geography IDs.

    Returns:
        None
    '''
    
    def country_to_continent(country_name):

        ''' this helping function maps a country name to its continent

        Args:
            - country_name (str): the name of the country

        Returns:
            - str: continent name
        '''

        country_alpha2 = pc.country_name_to_country_alpha2(country_name)
        country_continent_code = pc.country_alpha2_to_continent_code(country_alpha2)
        country_continent_name = pc.convert_continent_code_to_continent_name(country_continent_code)
        return country_continent_name

    coords = [k for k in geography.keys()]
    results = (rg.search(coords))
    coordinates_to_results = dict(zip(coords, results))

    for coord, geo_id in geography.items():
        if coord in coordinates_to_results:
            result = coordinates_to_results[coord]
            continent = country_to_continent(result['country'])
            geography[coord] = [geo_id, result['city'], result['country'], continent, coord[0], coord[1]] 



def write_to_csv(structures):

    ''' this function writes data from structures to CSV files

    Args:
        - structures (list): list of tuples containing data, filename and header 
                             each tuple represents a structure to be written to a CSV file

    Returns:
        None
    '''
    
    for data, filename, header in structures:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            if header:
                writer.writerow(header)

            if isinstance(data, dict):
                for row in data.values():
                    row_list = list(row) if isinstance(row, set) else row
                    writer.writerow(row_list)
            elif isinstance(data, set):  
                for element in data:
                    writer.writerow([element])

                


if __name__ == "__main__":

    # initialization of structures representing both fact and dimension tables
    # dimension tables: partecipant, gun, geography, dates and incident
    # each dimensional table is initialized as an empty dictionary (expcept for Incident), 
    # and specific flags indicate whether we need generate ID 

    partecipant = {}
    geography = {}
    gun = {}
    dates = {}
    incident = set()
    custody = {}

    dimensional_tables = {
        'partecipant': (partecipant, True) ,  # True if we need to generate the ID, False otherwise (meaning that the ID is in Police.csv)
        'gun': (gun, True) ,
        'geography': (geography, True) ,
        'dates': (dates, False)
    }

    file_partecipant_age = 'dict_partecipant_age.json'
    file_partecipant_status = 'dict_partecipant_status.json'
    file_partecipant_type = 'dict_partecipant_type.json'
    file_police = 'Police.csv'
    file_xml = 'dates.xml'

    # loading json data which is needed to compute crime gravity 
    with open(file_partecipant_age, 'r') as F1:
        age = json.load(F1)    
    with open(file_partecipant_type, 'r') as F2:
        type = json.load(F2)
    with open(file_partecipant_status, 'r') as F3:
        status = json.load(F3)


    process_police(file_police, dimensional_tables, incident, custody, age, type, status)

    enrich_date(dates, read_xml_file(file_xml))

    enrich_geography(geography)



    # saving data into csv file to avoid losing data
    # also adding the header to be more clear 

    data = [
        (partecipant, 'partecipant.csv', ['age_group', 'gender', 'status', 'type', 'partecipant_id']),
        (gun, 'gun.csv', ['is_stolen', 'gun_type', 'gun_id']),
        (dates, 'dates.csv', ['date_id', 'date', 'day', 'month', 'year', 'quarter','week_day']),
        (custody, 'custody.csv', ['custody_id', 'partecipant_id', 'gun_id', 'geo_id', 'date_id', 'crime_gravity', 'incident_id']),
        (incident, 'incident.csv', ['incident_id']),
        (geography, 'geography.csv', ['geography_id', 'city', 'country', 'continent', 'latitude', 'longitude'])
    ]


    write_to_csv(data)

