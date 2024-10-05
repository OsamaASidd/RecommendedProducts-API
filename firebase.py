import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import requests
import pandas as pd
import datetime
import pytz
import json


def get_firebase_credentials():
    url = "https://ffedf1ce685eedad040d475f7d0785128b608f0befbdbe71393b731-apidata.googleusercontent.com/download/storage/v1/b/gcf-sources-247754598856-us-central1/o/serviceAccountKey.json?jk=AdvOr8sRxsIyHb2SnCNT1AvZOBLVyX3RgOcE1YgzD5zJXVTxcThA6FKw-hgHRuy4Tj1_L_dKMNIgAH7_bCm1Ol5BQmAjtzejxngU2bLETOQWJ0wqThMs93Vv17sIJ8gZ6vKnM2PbeSL0WkmKNXBXu39GDqOMK8jrnk2A8DBv6gSBpqYfvOIjt9RsjDC1FBpkeQed4Wj8KegKP47wSE1gkxOsGeS0QceYse3gT4Pps4wX0jGyrpqMuu-Yq6eIUeCeJG88GkdYzYZBP0Dscv8cwz-Zq2Q7gkI2tvrtka1YVuRpLJ5PhabXl6iyqpKgBiC4RnizHmx9uVCkQxqRw7otHhRnVWyXcNGkOH5CbVCp0FIkpdYIzdD3Rwtv5Fh5sY965BvJb4NKyCUKvJcJfD2B-4bxIkuUaE6DN8nWNa4BE2dAyWlwjvyEvr9TlHDwvcNtGyHJ8Q_-rsCH56fKERMpv_Vc4HZ33asZI7f_9Hcq410NxHaOf_zdcZbvXFSi3Q-mNMe0F9p0JKgqglVR_ZKMaGQe6vPUJp9CqE4cIt5mGxCyxhnGH2dtcEVsZPxt2pRs57T3A1JdKSc_-iiiWsS3pG2R8Q0DSV8xSA55CVycBkrTpVOZXKBIIXW4xlCzAd6AMOzcNIQTx50cANIT4p94_3RMCklgSLa81Ivt2JOe8_QjZyllg7vZyVN57_Cz6exL_U9NAW8_XM4mY2Nge6doK2BeG0jch-zlulOTnFE7edAxJpB_11hbIat4fN6hwe5toFGGuaAxDYHkcRS2OG3BtFU4I3HTEZc59fWVyPsQM8eLWJgAbfVgvYJLyIj1jA0ifRMUdZszE4Wf3ItZpKd3-GdR5kQONvzObYijmL5CJ2Vti3bUTyI9vqDnXZhb8gluL_q5NXt7L-gIPfL3cYs8xIaQcnn5XBgEq4QNYByIbC_fDTtcl1sNemv0yRgvJtoPhT4OLGcExK_WqdEjtriLz2bB9X1zVQhuDcNvH8rP_hEF8v65RU_OOM1Z7zsBFoR6ye31xDPnwHaPuxzcgIB2Zrfb7AmgrfGV3NbjlQ2uD71h8PsqplmTRN3Cravcz5S930jyTpjCvsQov1dtMQjG-st1BzOdx2GFuEu19sJxty77gkELeYK6308EUz0BC7G0J31SvJPRnrsNMvLuKrsDA7l9yObixuxi639WQj1ySoBJcIiJxJcIsXkSs6uOSBVYnpCCOcdmvpIEQe8Cfa2Y-_qM58VrRl9ykIBB9mdA2myjKOC4hjP99PBfRi0x68wOrk4Y5kFdHTHgvIXEtEXW1LYDX3zMFn4Bwl0WUsGNEFjPTBmpsuO5I8jI66eaQQWTZlZdpt7xTYP_a766V2NEd41_pm8L0uBRKGXak3ELxVRHSLa8JO7Miw&isca=1"
    response = requests.get(url)
    key_dict = json.loads(response.text)  
    return credentials.Certificate(key_dict)
cred = get_firebase_credentials()
firebase_admin.initialize_app(cred)
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


