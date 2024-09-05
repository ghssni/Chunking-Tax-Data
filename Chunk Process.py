import re
import os
import pandas as pd
from PyPDF2 import PdfReader
from pymongo import MongoClient

import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


# Function for extract PDF to text
def extract_text_from_pdf(pdf_path):
    reader = PdfReader(pdf_path)
    text = ''
    for page in reader.pages:
        text += page.extract_text()
    return text

# Function to chunk the text
def chunk_document(text):
    chunks = []
    bab = pasal = ayat = poin = None
    current_chunk = {"Teks": "", "Bab": None, "Pasal": None, "Ayat": None, "Poin": None, "Penjelasan": "Bukan"}

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        
        # Check if the line contains "Penjelasan" to switch to explanation mode
        if line.startswith("Penjelasan"):
            if current_chunk["Teks"]:
                chunks.append(current_chunk)
            current_chunk = {"Teks": "", "Bab": bab, "Pasal": pasal, "Ayat": ayat, "Poin": poin, "Penjelasan": "Ya"}
            
        # Check for "BAB" to set the Bab value
        elif line.startswith("BAB"):
            if current_chunk["Teks"]:
                chunks.append(current_chunk)
            bab_match = re.search(r"BAB\s+([IVXLC]+)", line)
            if bab_match:
                bab = bab_match.group(1)
            current_chunk = {"Teks": "", "Bab": bab, "Pasal": pasal, "Ayat": ayat, "Poin": poin, "Penjelasan": "Bukan"}
        
        # Check for "Pasal" at the start of the line to set the Pasal value
        elif line.startswith("Pasal"):
            if current_chunk["Teks"]:
                chunks.append(current_chunk)
            pasal_match = re.search(r"Pasal\s+(\d+)", line)
            if pasal_match:
                pasal = pasal_match.group(1)
            current_chunk = {"Teks": "", "Bab": bab, "Pasal": pasal, "Ayat": ayat, "Poin": poin, "Penjelasan": "Bukan"}

        # Check for "Ayat" (e.g., (1)) and set Ayat value
        elif re.match(r"\(\d+[a-z]?\)", line):
            if current_chunk["Teks"]:
                chunks.append(current_chunk)
            ayat_match = re.search(r"\((\d+[a-z]?)\)", line)
            if ayat_match:
                ayat = ayat_match.group(1)
                poin = None  # Reset poin
            current_chunk = {"Teks": "", "Bab": bab, "Pasal": pasal, "Ayat": ayat, "Poin": poin, "Penjelasan": "Bukan"}
            current_chunk["Teks"] += line + " "
        
        # Check for "Poin" and set Poin value (e.g., a. or Huruf a)
        elif re.match(r"^[a-z]\.|Huruf\s*[a-z]", line):
            if current_chunk["Teks"]:
                chunks.append(current_chunk)
            poin_match = re.search(r"([a-z])", line)
            if poin_match:
                poin = poin_match.group(1)  # Capture only the letter
            current_chunk = {"Teks": "", "Bab": bab, "Pasal": pasal, "Ayat": ayat, "Poin": poin, "Penjelasan": "Bukan"}
            current_chunk["Teks"] += line + " "

        # Otherwise, append the line to the current chunk's text
        else:
            current_chunk["Teks"] += line + " "

    # Append the last chunk
    if current_chunk["Teks"]:
        chunks.append(current_chunk)

    return chunks

def chunkProcess():
    # Extract text from Konsolidasi.pdf
    pdf_path = "./Konsolidasi.pdf" 
    document_text = extract_text_from_pdf(pdf_path)

    # Apply chunking function to the extracted text
    chunks = chunk_document(document_text)

    # Convert chunks to a pandas DataFrame
    df = pd.DataFrame(chunks)

    # Save the DataFrame to a CSV file
    df.to_csv('konsolidasi_data.csv', index=False)
    print("------Data Chunked------")

def insertMongoDB():
    # MongoDB Connection
    client = MongoClient('mongodb://localhost:27017/')
    db = client['inatax'] 
    collection = db['de'] 

    # Read CSV
    df = pd.read_csv('konsolidasi_data.csv')

    # Looping to MongoDB
    for i, row in df.iterrows():
        document = row.to_dict()
        collection.insert_one(document)
    print("Data successfully inserted into MongoDB.")

default_args = {
    'owner': 'Tyas',
    'start_date': dt.datetime(2024, 10, 3),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG('MyDBdag',
         default_args=default_args,
         schedule_interval='0 2 * * *', # Cron Job every 02:00 everyday
         ) as dag:

    chunkData = PythonOperator(task_id='ChunkingData',
                                 python_callable=chunkProcess)
    insertData = PythonOperator(task_id='InsertDataMongoDB',
                                 python_callable=insertMongoDB)

chunkData >> insertData