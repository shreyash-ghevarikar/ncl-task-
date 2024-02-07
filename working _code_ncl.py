import mysql.connector
from mysql.connector import Error
import requests as req
from ftplib import FTP
import unlzw3
from pathlib import Path
import urllib.request

def getFile(url, destination_file, username=None, password=None):
    try:
        with urllib.request.urlopen(url) as response, open(destination_file, 'wb') as file:
            file.write(response.read())
        print(f"File downloaded successfully: {destination_file}")
        return True
    except Exception as e:
        print(f"Failed to fetch data from {url}: {e}")
        return False

def uncompressedFile(source_file, destination_file):
    try:
        uncompressed_data = unlzw3.unlzw(Path(source_file))
        uncompressed_data = uncompressed_data.decode('utf-8') 
        with open(destination_file, 'w', encoding='utf-8') as file:
            file.write(uncompressed_data)
        print(f"File uncompressed and saved successfully: {destination_file}")
        return True
    except Exception as e:
        print(f"Failed to uncompress file {source_file}: {e}")
        return False

def ftpGetConnect(host, username, password):
    try:
        ftp = FTP(host)
        ftp.login(username, password)
        print(f"Connected to FTP server: {host}")
        return ftp
    except Exception as e:
        print(f"Failed to connect to FTP server: {e}")
        return None    

def loadNcl(mydb, file_path):
    try:
        cursor = mydb.cursor()
        cursor.execute('TRUNCATE TABLE ncl')
        load_data_query_ncl = f'''
            LOAD DATA LOCAL INFILE "{file_path}" INTO TABLE ncl
            CHARACTER SET latin1
            FIELDS TERMINATED BY ";" 
            LINES TERMINATED BY "\\n"
            IGNORE 1 LINES
            (run_dat, ship_cd, sail_dat, sail_day_qty, voyage_cd, package_type_cd,
            itinerary_desc, price_program_cd, category_cd, gateway_cd, sail_id,
            embark_port_cd, disembark_port_cd, meta_category_cd, total_pkg_tariff_amt,
            total_pkg_sgl_supp_amt, total_pkg_t4_adult_amt, total_pkg_t4_child_amt,
            total_pkg_t4_infant_amt, gtf_tariff_amt, gtf_sgl_supp_amt, gtf_t4_adult_amt,
            gtf_t4_child_amt, gtf_t4_infant_amt, fuel_suppl_comm_tariff_amt,
            fuel_suppl_comm_sgl_supp_amt, fuel_suppl_comm_t4_adult_amt,
            fuel_suppl_comm_t4_child_amt, fuel_suppl_comm_t4_infant_amt)
        '''
        cursor.execute(load_data_query_ncl)
        mydb.commit()
        cursor.close()
        print("Data loaded into ncl table")
        return True
    except Exception as e:
        print(f"Failed to load data into ncl table: {e}")
        return False

def loadNclIt(mydb, file_path):
    try:
        cursor = mydb.cursor()  
        cursor.execute('TRUNCATE TABLE nclit')
        load_data_query_nclit = f'''
            LOAD DATA LOCAL INFILE "{file_path}" INTO TABLE nclit
            CHARACTER SET latin1
            FIELDS TERMINATED BY ";"
            LINES TERMINATED BY "\\n"
            IGNORE 1 LINES
            (Run_dat, Sail_Id, Long_Ship_Nam, Sail_Dat, Port_Cd, Port_Nam, Arrival_Tim,
            Departure_Tim, offset_day_nbr, Voyage_cd, Brochure_Product_Desc)
        '''
        cursor.execute(load_data_query_nclit)
        mydb.commit()
        cursor.close()
        print("Data loaded into nclit table")
        return True
    except Exception as e:
        print(f"Failed to load data into nclit table: {e}")
        return False

if __name__ == '__main__':
    try:
        ftp_host = "ftp.ncl-europe.eu"
        ftp_username = "datafeed_it"
        ftp_password = "4Ftp@_it735"

        ftp_connection = ftpGetConnect(ftp_host, ftp_username, ftp_password)
        if ftp_connection:
            print("Connected to FTP server")

       
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root123",
            database="db2",
            auth_plugin="mysql_native_password"
        )

        if mydb.is_connected():
            print("Successfully connected to MySQL")

            file_url = "ftp://datafeed_it:4Ftp@_it735@ftp.ncl-europe.eu/ITALY.txt.Z"
            file_name = "ITALY.txt.Z"
            uncompressed_file_name = "ITALY.txt"
            
            if getFile(file_url, file_name):
                if uncompressedFile(file_name, uncompressed_file_name):
                    if loadNcl(mydb, uncompressed_file_name):
                        print("Ncl data loaded successfully")
                    else:
                        print("Failed to load Ncl data")
                else:
                    print("Failed to uncompress file")

            file_url = "ftp://datafeed_it:4Ftp@_it735@ftp.ncl-europe.eu/ITALY_Itinerary.txt.Z"
            file_name = "ITALY_Itinerary.txt.Z"
            uncompressed_file_name = "ITALY_Itinerary.txt"

            if getFile(file_url, file_name):
                if uncompressedFile(file_name, uncompressed_file_name):
                    if loadNclIt(mydb, uncompressed_file_name):
                        print("Nclit data loaded successfully")
                    else:
                        print("Failed to load Nclit data")
                else:
                    print("Failed to uncompress file")
        

            ftp_connection.quit()
            print("FTP Connection closed")

            mydb.close()
            print("MySQL Connection closed")
    except Exception as e:
        print(f"An error occurred: {e}")



 