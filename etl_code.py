import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

def extract_from_csv(csv_file):
    return pd.read_csv(csv_file)

def extract_from_json(json_file):
    return pd.read_json(json_file, lines = True)

def extract_from_xml(file_to_process): 
    dataframe = pd.DataFrame(columns=["name", "height", "weight"]) 
    tree = ET.parse(file_to_process) 
    root = tree.getroot() 
    for person in root: 
        name = person.find("name").text 
        height = float(person.find("height").text) 
        weight = float(person.find("weight").text) 
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True) 
    return dataframe

def extract():
    extracted_data = pd.DataFrame(columns=['name','height','weight'])

    #process all csv files
    for csv_file in glob.glob("*.csv"):
        extracted_data = pd.concat([extracted_data, extract_from_csv(csv_file)], ignore_index = True)

    for json_file in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(json_file)], ignore_index = True)

    for xml_file in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xml_file)], ignore_index = True)

    return extracted_data


def transform(data):
    #conver inches to meter
    data['height'] = round(data.height * 0.0254, 2)


    #convert lb to kg
    data['weight'] = round(data.weight * 0.45359237, 2)

    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M-%S' # # Year-Monthname-Day-Hour-Minute-Second 
    now= datetime.now() #get current date and time
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')



#Testing the pipeline

log_progress('Etl job started')

#extract
log_progress('Extract phase started')
extracted_data = extract()
log_progress('extract phase ended')


#transform
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
log_progress("Transform phase Ended") 

#load
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
log_progress("Load phase Ended") 

log_progress("ETL Job Ended") 


