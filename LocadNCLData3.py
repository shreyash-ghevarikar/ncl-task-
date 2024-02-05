import mysql.connector
from mysql.connector import Error
import requests as req
import urllib.request
from ftplib import FTP

def getFile(url, destination_file, username=None, password=None):
    try:
        with urllib.request.urlopen(url) as response, open(destination_file, 'wb') as file:
            file.write(response.read())
        print(f"File downloaded successfully: {destination_file}")
    except Exception as e:
        print(f"Failed to fetch data from {url}: {e}")

def ftpGetConnect(host, username, password):
    try:
        ftp = FTP(host)
        ftp.login(username, password)
        print(f"Connected to FTP server: {host}")
        return ftp
    except Exception as e:
        print(f"Failed to connect to FTP server: {e}")
        return None

# Function to load data into ncl table
def dataLoadNcl(mydb):
        cursor = mydb.cursor()
        cursor.execute('TRUNCATE TABLE ncl')
        load_data_query_ncl = '''
            LOAD DATA LOCAL INFILE "./ITALY.txt" INTO TABLE ncl
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ";" 
            LINES TERMINATED BY "\n"
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

# Function to load data into nclit table
def dataLoadNclIt(mydb):
        cursor = mydb.cursor()  
        cursor.execute('TRUNCATE TABLE nclit')
        load_data_query_nclit = '''
            LOAD DATA LOCAL INFILE "./ITALY_Itinerary.txt" INTO TABLE nclit
            CHARACTER SET utf8mb4
            FIELDS TERMINATED BY ";"
            LINES TERMINATED BY "\n"
            IGNORE 1 LINES
            (Run_dat, Sail_Id, Long_Ship_Nam, Sail_Dat, Port_Cd, Port_Nam, Arrival_Tim,
            Departure_Tim, offset_day_nbr, Voyage_cd, Brochure_Product_Desc)
        '''
        cursor.execute(load_data_query_nclit)
        mydb.commit()
        cursor.close()
        print("Data loaded into nclit table")
    

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

            
            getFile("ftp://datafeed_it:4Ftp@_it735@ftp.ncl-europe.eu/ITALY.txt.Z", "./ITALY.txt","datafeed_it", "4Ftp@_it735")
            # dataLoadNcl(mydb)

            getFile("ftp://datafeed_it:4Ftp@_it735@ftp.ncl-europe.eu/ITALY_Itinerary.txt.Z", "./ITALY_Itinerary.txt", "datafeed_it", "4Ftp@_it735")
            # dataLoadNclIt(mydb)

            ftp_connection.quit()
            print("FTP Connection closed")

          
            mydb.close()
            print("MySQL Connection closed")
    except Exception as e:
        print(f"An error occurred: {e}")


 