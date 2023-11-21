from googleapiclient.discovery import build
from google.oauth2 import service_account
from pymongo import MongoClient
import pandas as pd
import numpy as np
import json



#-----------------------------------------------------------------------------------------------------------------------------------------------------#
# def data():
#         client = MongoClient("mongodb+srv://bharath:AfckYvszaOBqGdwL@digischeduler.ooq3z.mongodb.net/digischeduler")
#         db = client["digischeduler"]
#         collection = db["course_schedules"]
#         global data1
#         data1 = list(collection.find())
#         df = pd.DataFrame(data1)
#         print("Got the data")
#         df["_id"]=df["_id"].astype(str)#.str.get('$oid')
#         df['retake'] = df['sessionDetail'].apply(lambda x: x['retake'])
#         df['attendance_mode'] = df['sessionDetail'].apply(lambda x: x['attendance_mode'])
#         df['attendance_mode'] = df['attendance_mode'].replace('COMPLETED', "completed")
#         df['_institution_id']=df["_institution_id"].str.get('$oid')
#         df['_institution_calendar_id']=df["_institution_calendar_id"].str.get('$oid')
#         df['_program_id']=df["_program_id"].str.get('$oid')
#         df['_student_group_id']=df["_student_group_id"].str.get('$oid')
#         df['_course_id']=df["_course_id"].str.get('$oid')
#         df['_session_id']=df["session"].str.get('_session_id')
#         df['_session_id']=df["_session_id"].str.get('$oid')
#         df['s_no']=df["session"].str.get('s_no')
#         df['delivery_symbol']=df["session"].str.get('delivery_symbol')
#         df['delivery_no']=df["session"].str.get('delivery_no')
#         df['session_type']=df["session"].str.get('session_type')
#         df['session_topic']=df["session"].str.get('session_topic')
#         df['student_groups_id'] = df['student_groups'].apply(lambda x: x[0].get('_id', None))
#         df['student_groups_group_id'] = df['student_groups'].apply(lambda x: x[0].get('group_id', None))
#         df['student_groups_gender'] = df['student_groups'].apply(lambda x: x[0].get('gender', None))
#         df['student_groups_group_no'] = df['student_groups'].apply(lambda x: x[0].get('group_no', None))
#         df['student_groups_group_name'] = df['student_groups'].apply(lambda x: x[0].get('group_name', None))
#         df['student_groups_strudent'] = df['student_groups'].apply(lambda x: x[0].get('students', None))
#         df['student_groups_Total_strudent'] = df['student_groups_strudent'].apply(len)
#         df['student_groups_session_group'] = df['student_groups'].apply(lambda x: x[0].get('session_group', None))
#         df['student_groups_len_session_group'] = df['student_groups_session_group'].apply(len)
#         # df['schedule_date']=df["schedule_date"].str.get('$date')
#         # df['schedule_date']=df["schedule_date"].str.get('$numberLong')
#         df = df.dropna(subset=['schedule_date'])
#         df['schedule_date'] = pd.to_datetime(df['schedule_date'], format='iso8601', errors='coerce')
#         df['schedule_date'] = df['schedule_date'].dt.strftime('%Y-%m-%d')
#         df['start'] = df['start'].apply(lambda x: f"{x['hour']}:{x['minute']} {x['format']}" if isinstance(x, dict) else None)
#         df['end'] = df['end'].apply(lambda x: f"{x['hour']}:{x['minute']} {x['format']}" if isinstance(x, dict) else None)
#         df['subject_name'] = df['subjects'].apply(lambda x: x[0]['subject_name'] if len(x) > 0 else None)
#         df['staff_status'] = df['staffs'].apply(lambda x: x[0]['status'] if len(x) > 0 else None)
#         df['staff_name'] = df['staffs'].apply(lambda x: f"{x[0]['staff_name']['first']} {x[0]['staff_name']['last']}" if len(x) > 0 else None)

#         df=df[["_id","retake","attendance_mode","rotation","merge_status","status","isDeleted","isActive","_institution_id","program_name","type","term","year_no","course_name","course_code","delivery_symbol","delivery_no","session_type","session_topic","student_groups_gender","student_groups_group_no","student_groups_group_name","student_groups_Total_strudent","student_groups_len_session_group","schedule_date","start","end","mode","subject_name","staff_status","staff_name","infra_name"]]


#         output_data=df.replace(np.nan, 0)
#         return output_data


#-----------------------------------------------------------------------------------------------------------------------------------------------------#




# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

SERVICE_ACCOUNT_FILE = 'Keys/Digival_solution_Google_api_key.json'


credentials=None
credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)



# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '12Nb8to5bnwaUXvCRN1rnbZnIY0APBqcxvWbVmUMOoF0'


service = build('sheets', 'v4', credentials=credentials)

# Call the Sheets API
sheet = service.spreadsheets()
# result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,range='All_Session_Data.csv!A1:Z1000').execute()
# values = result.get('values', [])

# print("starting",values)
# df = pd.DataFrame(values[1:], columns=values[0])
# data=data()
# list_of_lists = data.values.tolist()
# list_of_lists.insert(0, data.columns.tolist())
# # print(len(list_of_lists))
# service.spreadsheets().values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='course_schedules!A1:ZZ10000').execute()
# Update = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,range='course_schedules!A1:ZZ10000',valueInputOption="USER_ENTERED",body={"values":list_of_lists}).execute()





# f = open('To Ajesh/schedule_anomalies.json', encoding="utf8")
# data = json.load(f)
# df = pd.DataFrame(data)

# df["_id"]=df["_id"].str.get('$oid')
# df["_institution_id"]=df["_institution_id"].str.get('$oid')
# df["scheduleId"]=df["scheduleId"].str.get('$oid')
# df["studentId"]=df["studentId"].str.get('$oid')
# df["createdAt"]=df["createdAt"].str.get('$date')
# df["createdAt"]=df["createdAt"].str.get('$numberLong')
# df['createdAt'] = pd.to_datetime(df['createdAt'], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')
# df["updatedAt"]=df["updatedAt"].str.get('$date')
# df["updatedAt"]=df["updatedAt"].str.get('$numberLong')
# df['updatedAt'] = pd.to_datetime(df['updatedAt'], unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')



# df=df[["_id","isDeleted","_institution_id","scheduleId","studentId","createdAt","updatedAt","pure"]]


# list_of_lists = df.values.tolist()
# list_of_lists.insert(0, df.columns.tolist())
# print(len(list_of_lists))

# service.spreadsheets().values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='schedule_anomalies!A1:ZZ10000').execute()
# Update = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,range='schedule_anomalies!A1:ZZ10000',valueInputOption="USER_ENTERED",body={"values":list_of_lists}).execute()




# course_schedules_test

client = MongoClient("mongodb+srv://bharath:AfckYvszaOBqGdwL@digischeduler.ooq3z.mongodb.net/digischeduler")
db = client["digischeduler"]
collection = db["course_schedules"]
data1 = list(collection.find())


df = pd.json_normalize(data1)
# Function to extract values from dictionaries
def extract_values(item, key):
    if isinstance(item, dict) and key in item:
        return item[key]
    return None

# Iterate over unique keys in dictionaries and create new columns
unique_keys = set(key for row in df['students'].dropna() for item in row if item and isinstance(item, dict) for key in item.keys())
print("unique_keys",unique_keys)
for key in unique_keys:
    df[f"students_{key}"] = df['students'].apply(lambda x: [extract_values(item, key) for item in x] if isinstance(x, list) and x else None)





for col in df.columns:
    # print("col",col)
    if df[col].astype(str).apply(len).max() >= 5000:
        print("max",col,df[col].astype(str).apply(len).max())
        


# df=df.drop(['student_groups','subjects','staffs','students'], axis=1)
df = df.astype(str)

list_of_lists = df.values.tolist()
# print(df.columns)
list_of_lists.insert(0, df.columns.tolist())
print(len(list_of_lists))

service.spreadsheets().values().clear(spreadsheetId=SAMPLE_SPREADSHEET_ID, range='course_schedules_test!A1:ZZ10000').execute()
Update = sheet.values().update(spreadsheetId=SAMPLE_SPREADSHEET_ID,range='course_schedules_test!A1:ZZ10000',valueInputOption="USER_ENTERED",body={"values":list_of_lists}).execute()





# # To add a new sheet in the spreadsheets


# spreadsheet = service.spreadsheets().get(spreadsheetId=SAMPLE_SPREADSHEET_ID).execute()

# # Add a new sheet to the existing spreadsheet
# new_sheet_title = "Test"
# add_sheet_request = {
#     "addSheet": {
#         "properties": {
#             "title": new_sheet_title,
#         }
#     }
# }
# service.spreadsheets().batchUpdate(spreadsheetId=SAMPLE_SPREADSHEET_ID, body={"requests": [add_sheet_request]}).execute()

# print(f"New sheet '{new_sheet_title}' added to the spreadsheet.")
