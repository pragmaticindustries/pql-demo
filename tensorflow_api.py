import json

import requests
# https://www.tensorflow.org/tfx/serving/docker
# Download the TensorFlow Serving Docker image and repo
# docker pull tensorflow/serving
#
# git clone https://github.com/tensorflow/serving
# # Location of demo models
# TESTDATA="$(pwd)/serving/tensorflow_serving/servables/tensorflow/testdata"
#
# # Start TensorFlow Serving container and open the REST API port
#  Modell hlaf_plus_two 0.5 * 2 * X
# docker run -t --rm -p 8501:8501 \
#                            -v "$TESTDATA/saved_model_half_plus_two_cpu:/models/half_plus_two" \
#                               -e MODEL_NAME=half_plus_two \
#     tensorflow/serving &
#
# # Query the model using the predict API
# curl -d '{"instances": [1.0, 2.0, 5.0]}' \
#         -X POST http://localhost:8501/v1/models/half_plus_two:predict
#
# # Returns => { "predictions": [2.5, 3.0, 4.5] }

headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
}

data = '{"instances": [10, 20, 50]}'
response = requests.post('http://localhost:8501/v1/models/half_plus_two:predict', headers=headers, data=data)
json_:dict = json.loads(response.content)
print(json_.get("predictions"))

data = '{"signature_name": "tensorflow/serving/regress", "examples": [{"x": 1.0}, {"x": 2.0}]}'
url = "http://localhost:8501/v1/models/saved_model_half_plus_two:regress"
response= requests.post(url=url,headers=headers,data=data)
json_:dict = json.loads(response.content)
print(json_.get("results"))

response = requests.get(url="http://localhost:8501/v1/models/saved_model_half_plus_two",headers=headers)
print(response)

print("Neues Modell")
response = requests.post('http://localhost:8503/v1/models/half_plus_three:predict', headers=headers, data=data)
json_:dict = json.loads(response.content)
print(json_.get("predictions"))

data = '{"signature_name": "tensorflow/serving/regress", "examples": [{"x": 1.0}, {"x": 2.0}]}'
url = "http://localhost:8503/v1/models/saved_model_half_plus_three:regress"
response= requests.post(url=url,headers=headers,data=data)
json_:dict = json.loads(response.content)
print(json_.get("results"))

response = requests.get(url="http://localhost:8503/v1/models/saved_model_half_plus_three",headers=headers)
print(response)


