from flask import Flask
from pymongo import MongoClient
import pandas as pd
import numpy as np
import json



app = Flask(__name__)


def Data_Converter():
        client = MongoClient("mongodb+srv://bharath:AfckYvszaOBqGdwL@digischeduler.ooq3z.mongodb.net/digischeduler")
        db = client["digischeduler"]
        collection = db["course_schedules"]
        data = list(collection.find())
        df = pd.DataFrame(data)
        print("Got the data")
        df["_id"]=df["_id"].astype(str)#.str.get('$oid')
        df['retake'] = df['sessionDetail'].apply(lambda x: x['retake'])
        df['attendance_mode'] = df['sessionDetail'].apply(lambda x: x['attendance_mode'])
        df['attendance_mode'] = df['attendance_mode'].replace('COMPLETED', "completed")
        df['_institution_id']=df["_institution_id"].str.get('$oid')
        df['_institution_calendar_id']=df["_institution_calendar_id"].str.get('$oid')
        df['_program_id']=df["_program_id"].str.get('$oid')
        df['_student_group_id']=df["_student_group_id"].str.get('$oid')
        df['_course_id']=df["_course_id"].str.get('$oid')
        df['_session_id']=df["session"].str.get('_session_id')
        df['_session_id']=df["_session_id"].str.get('$oid')
        df['s_no']=df["session"].str.get('s_no')
        df['delivery_symbol']=df["session"].str.get('delivery_symbol')
        df['delivery_no']=df["session"].str.get('delivery_no')
        df['session_type']=df["session"].str.get('session_type')
        df['session_topic']=df["session"].str.get('session_topic')
        df['student_groups_id'] = df['student_groups'].apply(lambda x: x[0].get('_id', None))
        df['student_groups_group_id'] = df['student_groups'].apply(lambda x: x[0].get('group_id', None))
        df['student_groups_gender'] = df['student_groups'].apply(lambda x: x[0].get('gender', None))
        df['student_groups_group_no'] = df['student_groups'].apply(lambda x: x[0].get('group_no', None))
        df['student_groups_group_name'] = df['student_groups'].apply(lambda x: x[0].get('group_name', None))
        df['student_groups_strudent'] = df['student_groups'].apply(lambda x: x[0].get('students', None))
        df['student_groups_Total_strudent'] = df['student_groups_strudent'].apply(len)
        df['student_groups_session_group'] = df['student_groups'].apply(lambda x: x[0].get('session_group', None))
        df['student_groups_len_session_group'] = df['student_groups_session_group'].apply(len)
        # df['schedule_date']=df["schedule_date"].str.get('$date')
        # df['schedule_date']=df["schedule_date"].str.get('$numberLong')
        df = df.dropna(subset=['schedule_date'])
        df['schedule_date'] = pd.to_datetime(df['schedule_date'], format='iso8601', errors='coerce')
        df['schedule_date'] = df['schedule_date'].dt.strftime('%Y-%m-%d')
        df['start'] = df['start'].apply(lambda x: f"{x['hour']}:{x['minute']} {x['format']}" if isinstance(x, dict) else None)
        df['end'] = df['end'].apply(lambda x: f"{x['hour']}:{x['minute']} {x['format']}" if isinstance(x, dict) else None)
        df['subject_name'] = df['subjects'].apply(lambda x: x[0]['subject_name'] if len(x) > 0 else None)
        df['staff_status'] = df['staffs'].apply(lambda x: x[0]['status'] if len(x) > 0 else None)
        df['staff_name'] = df['staffs'].apply(lambda x: f"{x[0]['staff_name']['first']} {x[0]['staff_name']['last']}" if len(x) > 0 else None)

        df=df[["_id","retake","attendance_mode","rotation","merge_status","status","isDeleted","isActive","_institution_id","program_name","type","term","year_no","course_name","course_code","delivery_symbol","delivery_no","session_type","session_topic","student_groups_gender","student_groups_group_no","student_groups_group_name","student_groups_Total_strudent","student_groups_len_session_group","schedule_date","start","end","mode","subject_name","staff_status","staff_name","infra_name"]]
        df=df.tail(200)
        output_data=df.replace(np.nan, 0)
        return output_data
      




@app.route('/')
# @cross_origin()
def POST_timetables():
        output_data=Data_Converter()
        data=output_data.to_json(orient="records", default_handler=str)
        data=json.loads(data)
        data={"Data":data}
        return data












if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000 ,debug=True)