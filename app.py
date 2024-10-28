from flask import Flask, request, jsonify
from firebase import get_Users, extract_category_subcat
from flask_cors import CORS, cross_origin
from RecSys import rec, createSearchTerm
import pandas as pd
import ast 
import google.generativeai as genai
import re

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route("/")
def hello() -> str:
    """Return a friendly HTTP greeting.

    Returns:
        A string with the words 'Hello World!'.
    """
    return "Hello World!"

""" 
    :description: returns a Dataframe for all users with their logMap (<Category> _ <SubCategory> _ <SubsubCategory>)
"""
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


@app.route('/evaluateText', methods=['GET'])
@cross_origin()
def evaluate_text():
    GOOGLE_API_KEY = "AIzaSyDLALmBwgf7xcEu5mY3pS6JL_FsN85LTxw"
    genai.configure(api_key=GOOGLE_API_KEY)
    model = genai.GenerativeModel('gemini-pro')

    content = request.args.get('text', default='')
    if not content:
        return jsonify({"error": "No text provided"}), 400

    prompt = f"""
    Determine if text is inappropriate, has sexually explicit content, hate speech, harassment, or dangerous content.
    If so, respond with 'Negative'. If the text is acceptable, return 'Positive'.
    Text: "{content}"
    """

    try:
        response = str(model.generate_content(prompt))
        match = re.search(r'"text":\s*"([^"]*)"', response)
        result = match.group(1)
        # safety_ratings = {rating.category: rating.probability for rating in response.result.candidates[0].safety_ratings}
        return jsonify({"evaluation": result}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8080, debug=True)
