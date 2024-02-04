import mysql.connector 
from mysql.connector import Error

import urllib.request

def getFile(url, destination_file):
    try:
        urllib.request.urlretrieve(url, destination_file)
        print(f"File downloaded successfully: {destination_file}")
    except Exception as e:
        print(f"Failed to fetch data from {url}: {e}")


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
        ftp_username = "datafeed_it"
        ftp_password = "4Ftp@_it735"
        # Create a MySQL connection
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root123",
            database="db2",
            auth_plugin="mysql_native_password"
        )

        if mydb.is_connected():
            print("Successfully connected ")

            # Call functions to get file and load data
            getFile("ftp://datafeed_it:4Ftp@_it735@ftp.ncl-europe.eu/ITALY.txt.Z", "./ITALY.txt")
            dataLoadNcl(mydb)

            getFile("ftp://datafeed_it:4Ftp@_it735@ftp.ncl-europe.eu/ITALY_Itinerary.txt.Z", "./ITALY_Itinerary.txt")
            dataLoadNclIt(mydb)

            # Close the MySQL connection
            mydb.close()
            print("Connection closed")
    except Exception as e:
      print(f"An error occurred: {e}")


 