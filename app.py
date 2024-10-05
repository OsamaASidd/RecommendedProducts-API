from flask import Flask, request, jsonify
from firebase import get_Users, extract_category_subcat
from RecSys import rec, createSearchTerm
import pandas as pd
import ast 


app = Flask(__name__)


@app.route("/")
def hello() -> str:
    """Return a friendly HTTP greeting.

    Returns:
        A string with the words 'Hello World!'.
    """
    return "Hello World!"


@app.route('/getusers', methods=['GET'])
def get_users_data():
    users_df = get_Users()
    columns_to_keep = ['user_id', 'logs', 'userlogs', 'userActivity', 'favCategories', 'favBrands']
    filtered_df = users_df[columns_to_keep]
    new_df = extract_category_subcat(filtered_df)
    new_df = new_df.rename(columns={"user_id":"uID", "category_subcat_subsubcat": "logMap"})
    return new_df

@app.route('/getRecommendedProducts/<user_id>', methods=['GET'])
def recommend_products(user_id):
    try:
        # 'user_id' is now directly available as a function parameter
        if not user_id:
            return jsonify({"error": "Missing user_id in the URL path"}), 400
        df = get_users_data()
        df['searchTerms'] = df.apply(lambda row: createSearchTerm(row['logMap']), axis=1)
        user_data = df[df['uID'] == user_id]
        if user_data.empty:
            return jsonify({"error": "User not found"}), 404
        recommended_products = rec(user_data)
        return jsonify({"recommended_products": recommended_products}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500




if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8080, debug=True)
