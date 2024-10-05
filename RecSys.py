import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import MultiLabelBinarizer
import ast 
from scipy import stats  
from collections import Counter
from firebase import *
# Load the CSV file
# df = pd.read_csv('users_Categories.csv')


# Define the function to parse user preferences
def createSearchTerm(logMap):
    searchTerm = []

    # Convert logMap entries to search terms
    for log in logMap:
        category, sub_category, sub_sub_category = log.split('_')
        searchTerm.append({
            'category': category,
            'subCategory': sub_category,
            'subSubCategory': sub_sub_category
        })

    return searchTerm


def rec(user_data):
    if user_data.empty:
        print("User not found.")
        return

    search_terms = user_data.iloc[0]['searchTerms']

    if len(search_terms) == 0:
        return

    mlb = MultiLabelBinarizer()
    user_search_terms = []
    for term in search_terms:
            category = term.get('category', 'None')
            sub_category = term.get('subCategory', 'Unknown')  
            sub_sub_category = term.get('subSubCategory', 'Unknown')  
            user_search_terms.append((category, sub_category, sub_sub_category))

    mode = Counter(user_search_terms).most_common(1)[0][0]
    user_vector = mlb.fit_transform([mode]).sum(axis=0)
    print(mode)
    products_ref = db.collection('products').stream()  
    
    products = []
    for product in products_ref:
        product_data = product.to_dict()
        products.append({
            'id': product_data['id'],
            'category': product_data['category'],
            'subCategory': product_data['subCategory'],
            'brand': product_data['supplier'],
            'subSubCategory': product_data['subSubCategory'],
            'views': product_data['views']  
        })


    product_vectors = []
    for product in products:
        product_vector = mlb.transform([(product['category'], product['subCategory'], product['subSubCategory'])]).sum(axis=0)
        product_vectors.append((product['id'], product_vector, product['views']))

    product_vectors_array = np.array([vec for _, vec, _ in product_vectors])
    similarity = cosine_similarity([user_vector], product_vectors_array).flatten()
    top_indices = np.argsort(similarity)[::-1][:50]
    top_products = [(products[i]['id']) for i in top_indices]

    return top_products

