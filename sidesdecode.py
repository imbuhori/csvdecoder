from ftplib import FTP
from datetime import datetime
from datetime import date
from io import BytesIO
import os
import time
import psycopg2
import requests

# import mysql.connector

# today = date.today()
# day = f"{today.day:02}"
# month = f"{today.month:02}"
# year = today.year

#Call Todays Date And Time
today_str_file = datetime.today().strftime('%d%m%Y')

#FTP connection details

ftp_server = ""
ftp_username = ""
ftp_password = ""
ftp_root_folder = ""  # FTP folder containing subfolders

#PostgreSQL connection details
pg_host = ""
pg_database = ""
pg_user = ""
pg_password = ""

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=pg_host,
    database=pg_database,
    user=pg_user,
    password=pg_password
)


cursor = conn.cursor()



# Connect to FTP
ftp = FTP(ftp_server)
ftp.login(user=ftp_username, passwd=ftp_password)
ftp.cwd(ftp_root_folder)

# Get today's date in yymmdd format
today_str = datetime.today().strftime('%y%m%d')

# List all subfolders in /files/JKR
folders = ftp.nlst()

#Move File to Error Folder Function
def move_to_error_folder(filename,filecontent,error_folder="/"+ftp_root_folder+"/ERROR/"):
    with FTP(ftp_server) as ftp:
        ftp.login(ftp_username, ftp_password)
        parts = error_folder.strip("/").split("/")
        current_path = ''
        for part in parts :
            current_path += "/" + part
            try :
                ftp.mkd(current_path)
            except Exception :
                pass
        ftp.cwd(error_folder)
        if isinstance(filecontent,str) :
            filecontent = filecontent.encode("utf-8")

        bio = BytesIO(filecontent)
        ftp.storbinary(f"STOR {filename}",bio)
        bio.close()
        print(f"⚠️ Moved {file_name} into {error_folder}")
        

        try:
            ftp.cwd('..')
            ftp.delete(filename)
            print(f"🗑️ Removed {file_name} from {ftp_root_folder}")
        except Exception as e:
            print(f"⚠️ Could not remove {file_name} from {ftp_root_folder}: {e}")

#Move File to Success Folder Function
def move_to_success_folder(filename,filecontent,success_folder="/"+ftp_root_folder+"/SUCCESS/"):
    with FTP(ftp_server) as ftp :
        ftp.login(ftp_username,ftp_password)
        

        parts = success_folder.strip("/").split("/")
        current_path =""
        for part in parts :
            current_path += "/" + part
            try :
                ftp.mkd(current_path)
                print(f"Created Folder : {current_path}")
            except Exception:
                pass

        ftp.cwd(success_folder)

        if isinstance(filecontent,str):
            filecontent = filecontent.encode("utf-8")

        with open("temp_upload.tmp", "wb") as temp_file :
            temp_file.write(filecontent)
        
        with open("temp_upload.tmp", "rb") as temp_file:
            ftp.storbinary(f"STOR {filename}",temp_file)

        os.remove("temp_upload.tmp")
        print(f"Uploaded {filename} to {success_folder}")

        try :
            ftp.cwd('..')
            ftp.delete(filename)
            print(f"🗑️ Removed {file_name} from {ftp_root_folder}")
        except Exception as e:
            print(f"⚠️ Could not remove {file_name} from {ftp_root_folder}: {e}")

#Send Alert Function
def send_alert_to_laravel(stationid,level,stationtype):
    
    payload = {
        "stationid" : stationid,
        "level" : level,
        "stationtype" :stationtype,
    }

    try:
        response = requests.post("",json=payload,timeout=5)
        print("Alert Sent:" ,response.status_code)
    except Exception as e:
        print("Failed to send Alert",e)


# Define processing function once
def process_line(line,filename=None):
    
    columns = line.strip().split(",")

    if len(columns) < 25:
        print(f"Skipping malformed line: {line}")

        #Uncomment If needed
        # error_folder= ftp_root_folder+"/ERROR"
        # print(error_folder)
        # try :
        #     move_to_error_folder(filename,line,error_folder="/"+ftp_root_folder+"/ERROR")
         
        # except Exception as e :
        #      print(f"⚠️ Failed to move malformed line to ERROR folder: {e}")
        # return


    # Variable Declaration based on CSV Column Data
    station_id = columns[1]
    timestamp = columns[4]

    sirenid = columns[18]
    siren = columns[19]
    
    anncumm = float(columns[21]) if columns[21] else None
    dailycumm = float(columns[22]) if columns[22] else None
    hourlycumm = float(columns[23]) if columns[23] else None
    currrf = float(columns[24]) if columns[24] else None
    waterlevel = float(columns[36]) if columns[36] else None
    wldgr = float(columns[17]) if columns[17] else None
    wlwarn = float(columns[16]) if columns[16] else None
    wlalert = float(columns[15]) if columns[15] else None
    battery = float(columns[6]) if columns[6] else None

  


    try:

        #Timestamp Format
        datetime_object = datetime.strptime(timestamp, "%y%m%d%H%M%S")

        #Uncomment If Using Separate Date And Time Column
        #     date_part = datetime_object.strftime("%Y-%m-%d")
        #     time_part = datetime_object.strftime("%H:%M:%S")

        #     date_part2 = datetime_object.date()
    

        #Rainfall Data Insertion Into Database
        if dailycumm != None or hourlycumm != None : 
            cursor.execute("""
                SELECT COUNT(*) 
                FROM rainfall 
                WHERE stationid = %s AND timestamp = %s 
            """, (station_id, datetime_object))
            record_exists = cursor.fetchone()[0] > 0

            if not record_exists:
                cursor.execute("""
                    INSERT INTO rainfall (stationid,timestamp, anncum, daily, hourly, currentrf,battery) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (station_id, datetime_object,anncumm, dailycumm, hourlycumm, currrf,battery))
                conn.commit()

            # If Rainfall Threshold Trigger
            if hourlycumm >= 30 :

                #Threshold Level Declarion
                if hourlycumm >= 30 and hourlycumm < 60 :
                    level = 'Warning'
                elif hourlycumm >= 60 :
                    level = 'Danger'
                else :
                    level = 'Normal'
                
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM notification 
                    WHERE stationid = %s AND timestamp = %s AND stationtype = %s
                """, (station_id, datetime_object,1))

                record_exists = cursor.fetchone()[0] > 0

                if not record_exists:
                    cursor.execute("""
                        INSERT INTO notification (stationid,timestamp, stationtype, level,active_time) 
                        VALUES (%s, %s, %s, %s, %s)
                    """, (station_id, datetime_object,1, level,datetime_object))
                    conn.commit()

                #Send Alert Message
                send_alert_to_laravel(station_id,level,1)


            print(f"Station ID {station_id}")
            print(f"Timestamp {timestamp}")
            print(f"Daily Cumm : {dailycumm}")
            print(f"Hourly : {hourlycumm}\n")
       
        #Water Level Data Insertion Into Database
        if waterlevel != None :
            cursor.execute("""
                SELECT COUNT(*) 
                FROM waterlevel  
                WHERE stationid = %s AND datetime = %s 
            """, (station_id, datetime_object))
            record_exists = cursor.fetchone()[0] > 0

            if not record_exists:
                cursor.execute("""
                    INSERT INTO waterlevel (stationid,datetime, waterlevel, alert, warning,danger) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (station_id, datetime_object,waterlevel, wlalert, wlwarn, wldgr))
                conn.commit()

            # If Water Level Threshold Trigger
            if waterlevel >= wlalert :

                if waterlevel >= wlalert and waterlevel < wlwarn :
                    level = 'Alert'
                elif waterlevel >= wlwarn and waterlevel < wldgr :
                    level = 'Warning'
                elif waterlevel >= wldgr:
                    level = 'Danger'
                else :
                    level = 'Normal'
                
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM notification 
                    WHERE stationid = %s AND timestamp = %s AND stationtype = %s
                """, (station_id, datetime_object,2))
                record_exists = cursor.fetchone()[0] > 0


                if not record_exists:
                    cursor.execute("""
                        INSERT INTO notification (stationid,timestamp, stationtype, level,active_time) 
                        VALUES (%s, %s, %s, %s, %s)
                    """, (station_id, datetime_object,2, level,datetime_object))
                    conn.commit()

                
                #Send Station Alert To API
                send_alert_to_laravel(station_id,level,1)
                
            print(f"Station ID {station_id}")
            print(f"Timestamp {timestamp}")
            print(f"Water Level : {waterlevel}")
            print(f"Danger : {wldgr}")
            print(f"Warning : {wlwarn}\n")
      

        #Siren Data Insertion Into Database
        if sirenid != None:    
            cursor.execute("""
                SELECT COUNT(*) 
                FROM siren  
                WHERE stationid = %s AND active_time = %s 
            """, (station_id, datetime_object))
            record_exists = cursor.fetchone()[0] > 0


            #Siren level Declaration
            if siren == 'H' :

                level = 'Danger'

            elif siren == 'L':

                level = 'Warning'

            else :

                level = 'Normal'

    
            # check siren condition
            if siren == 'N':
                if not record_exists:
                    cursor.execute("""
                        INSERT INTO siren (stationid, active_time, level) 
                        VALUES (%s, %s, %s)
                    """, (station_id, datetime_object, siren))
                    conn.commit()
       

                    print(f"Station ID {station_id}")
                    print(f"SRID {sirenid}")
                    print(f"Timestamp {timestamp}")
                    print(f"Alarm : {siren}\n")
                    print(f"Level : {level}\n")

            elif siren != 'N' :
                if not record_exists:
                    cursor.execute("""
                        INSERT INTO siren (stationid, active_time, level) 
                        VALUES (%s, %s, %s)
                    """, (station_id, datetime_object, siren))
                    conn.commit()
        

                    print(f"Station ID {station_id}")
                    print(f"SRID {sirenid}")
                    print(f"Timestamp {timestamp}")
                    print(f"Alarm : {siren}\n")

                

            #Send Station Alert To API
            if level != 'Normal' :
                send_alert_to_laravel(station_id,level,3)

    except Exception as e:
        conn.rollback()
        #     move_to_error_folder(filename,line,error_folder="/"+ftp_root_folder+"/ERROR")
        print(f"Error processing line: {e}")


#Start Process FTP File
try:

    #Folder File List Variable Declaration 
    files = ftp.nlst()

    #Loop Through the Folder/Files
    for file_name in files:

        #Skip Tideda File
        if "rf" in file_name.lower():
            #print(f"Skipping file: {file_name} (contains 'rf')")
            continue

        #Skip Others Day File   
        if today_str not in file_name:
            #print(f"Skipping file: {file_name} (not from today {today_str})")
            continue

        print(f"Processing file: {file_name}")
        all_lines = []
       
            
        ftp.retrlines(
            f"RETR {file_name}",
            lambda line: (all_lines.append(line), process_line(line, file_name))
        )
        file_content = "\n".join(all_lines)

        #Uncomment if needed
        # move_to_success_folder(file_name,file_content,success_folder="/"+ftp_root_folder+"/SUCCESS")

except Exception as e:
    print(f"Failed to process folder: {e}")

# send_alert_to_laravel('KBLG0026','Warning',1)
# send_alert_to_laravel('KBLG0026','Danger',2)
# send_alert_to_laravel('KBLG0031','Warning',3)

# Close connections
ftp.quit()
cursor.close()
conn.close()
