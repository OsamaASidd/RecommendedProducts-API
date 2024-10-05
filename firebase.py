import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pandas as pd
import datetime
import pytz

app = credentials.Certificate("serviceAccountKey.json")
firebase_admin.initialize_app(app)
db = firestore.client()


def get_Users():
    users_ref = db.collection('users').stream()
    users_list = []
    for user in users_ref:
        user_dict = user.to_dict()
        user_dict['user_id'] = user.id
        logs_ref = db.collection('users').document(user.id).collection('logs').stream()
        logs_list = [log.to_dict() for log in logs_ref] 
        user_dict['logs'] = logs_list  
        activity_ref = db.collection('users').document(user.id).collection('userActivity').stream()
        activity_list = [activity.to_dict() for activity in activity_ref]  
        user_dict['userActivity'] = activity_list  
        users_list.append(user_dict)
    users_df = pd.DataFrame(users_list)
    return users_df

df = get_Users()

columns_to_keep = ['user_id', 'logs', 'userlogs', 'userActivity', 'favCategories', 'favBrands']
filtered_df = df[columns_to_keep]

def extract_category_subcat(df):
    extracted_data = {}

    for index, row in df.iterrows():
        user_id = row['user_id']
        
        if user_id not in extracted_data:
            extracted_data[user_id] = {'category_subcat_subsubcat': []}
        
        for log in row['logs']:
            if 'category' in log and 'subCategory' in log and 'subSubCategory' in log:
                category_info = f"{log['category']}_{log['subCategory']}_{log['subSubCategory']}"
                extracted_data[user_id]['category_subcat_subsubcat'].append(category_info)
        
        for activity in row['userActivity']:
            if 'category' in activity and 'subCategory' in activity and 'subSubCategory' in activity:
                category_info = f"{activity['category']}_{activity['subCategory']}_{activity['subSubCategory']}"
                extracted_data[user_id]['category_subcat_subsubcat'].append(category_info)
        
        if 'favCategories' in row and isinstance(row['favCategories'], list):
            extracted_data[user_id]['favCategories'] = row['favCategories']
        else:
            extracted_data[user_id]['favCategories'] = []

    extracted_df = pd.DataFrame.from_dict(extracted_data, orient='index').reset_index()
    extracted_df.columns = ['user_id', 'category_subcat_subsubcat', 'favCategories']
    
    return extracted_df

