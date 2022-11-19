import json
import requests
from flask import Flask, request, jsonify
from transformers import pipeline


app = app = Flask(__name__)



def get_model():
    model = pipeline("sentiment-analysis", model='pysentimiento/robertuito-sentiment-analysis')
    print("[+] model loaded")
    return model

model = get_model()

@app.route('/predict', methods=['POST'])
def predict():
    # receive the data
    req = request.get_data()
    req = json.loads(req)
    data = req['data']
    response = model(data)[0]
    return response

