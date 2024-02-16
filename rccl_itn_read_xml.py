import os
import requests
import xml.etree.ElementTree as ET
import mysql.connector

def get_url(): # return url or None
               
    url = "http://prod.rcclpul.fdf.istinfor.com/WSPricingFeed.asmx?wsdl"
    headers = {
            'content-type': 'text/xml',
      }

    body = """<RequestSearchBySeaPricing>
          <Header>
            <CruiseLineCode>RCCL</CruiseLineCode>
            <SubsystemId>3</SubsystemId>
            <AgencyId1>000204175</AgencyId1>
            <AgencyId2>000204175</AgencyId2>
            <Currency>EUR</Currency>
            <AgencyConsumer></AgencyConsumer>
            <TransactionCounter TerminalID="">1</TransactionCounter>
          </Header>
          <SearchBySea>
            <Sea>ALL</Sea>
            <DepartureDate>ALL</DepartureDate>
            <Guests>2</Guests>
            <Price></Price>
            <Age1></Age1>
            <Age2></Age2>
            <Age3></Age3>
            <Age4></Age4>
          </SearchBySea>
        </RequestSearchBySeaPricing>"""
   
    response = requests.post(url, data=body, headers=headers)
    print(response.content)
    
def read_xml():
  ship_codes = ['AD', 'AL', 'AN','AT','AX','BR','BY','CS','EC','EG','EN','EQ','EX','FL','FR','GR','HM','IC','ID','IN','JW','LB','MA','ML','NV','OA','OV','OY','QN','RD','RF','RH','SC','Sl','SL','SM','SR','ST','SY','UT','VI','WN','XC','XO','XP']
  
  for ship_code in ship_codes:
   # print(ship_codes)
    path = os.path.join("./rccl_scripts", f"{ship_code}_data.xml")
    tree = ET.parse(path)
   # print("Path:", path) 
    myroot = tree.getroot()
   # print("Root Tag:", myroot.tag)
    cruises = []

    for tag1 in myroot.findall('.//{http://tempuri.org/}List'):
        for tag2 in tag1:
            rccl_itn_dict = {}
            rccl_itn_dict["CruiseId"] = tag1.attrib.get("CruiseId", "")
            rccl_itn_dict["ItineraryCode"] = tag1.attrib.get("ItineraryCode", "")
            rccl_itn_dict["PackageId"] = tag1.attrib.get("PackageId", "")

            for child in tag2:
                if child.tag == "{http://tempuri.org/}DWeek":
                    rccl_itn_dict["DWeek"] = child.text
                elif child.tag == "{http://tempuri.org/}DepartureDate":
                    rccl_itn_dict["DepartureDate"] = child.text
                elif child.tag == "{http://tempuri.org/}PortName":
                    rccl_itn_dict["PortName"] = child.text
                    rccl_itn_dict["PortName_Code"] = child.attrib.get("Code", "")
                    rccl_itn_dict["PortName_Activity"] = child.attrib.get("Activity", "")
                elif child.tag == "{http://tempuri.org/}ArrivalTime":
                    rccl_itn_dict["ArrivalTime"] = child.text
                elif child.tag == "{http://tempuri.org/}DepartureTime":
                    rccl_itn_dict["DepartureTime"] = child.text
                elif child.tag == "{http://tempuri.org/}Indicator":
                    rccl_itn_dict["Indicator"] = child.text

            cruises.append(rccl_itn_dict)
            
           
            
  return cruises 
    # use cruises and dump data into the table rccl_itn.
            

def dump_data(cruises):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root123",
        database="db2",
        auth_plugin="mysql_native_password"
    )

    cursor = mydb.cursor()

    for cruise in cruises:
        print(cruises)
        print(cruises)
        CruiseId = cruise.get("CruiseId")
        ItineraryCode = cruise.get("ItineraryCode")
        PackageId = cruise.get("PackageId")
        DWeek = cruise.get("DWeek")
        DepartureDate = cruise.get("DepartureDate")
        PortName = cruise.get("PortName")
        PortName_Code = cruise.get("PortName_Code")
        PortName_Activity = cruise.get("PortName_Activity")
        ArrivalTime = cruise.get("ArrivalTime")
        DepartureTime = cruise.get("DepartureTime")
        Indicator = cruise.get("Indicator")

        sql = "INSERT INTO rccl_itn (CruiseId, ItineraryCode, PackageId, DWeek, DepartureDate, PortName, PortName_Code, PortName_Activity, ArrivalTime, DepartureTime, Indicator) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        cursor.execute(sql, (CruiseId, ItineraryCode, PackageId, DWeek, DepartureDate, PortName, PortName_Code, PortName_Activity, ArrivalTime, DepartureTime, Indicator))
       # print(cursor.statement)
    mydb.commit()
    cursor.close()
    mydb.close()

if __name__ == "__main__":
     # get_url()
   
    cruises = read_xml()

    
    dump_data(cruises)

    
