from flask import Flask
from flask import request, jsonify
from sql_server.sql_server import SqlServer, DB_NAME
from flask_cors import CORS, cross_origin
from parsing.parser import get_vacancy

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'
server = SqlServer(DB_NAME)

@app.route('/')
@cross_origin()
def hello_world():
    return 'hello world!'


@app.route('/get_positions')
@cross_origin()
def get_positions():
    try:
        positions = server.get_positions()
        return jsonify(isError=False,
                       data=positions,
                       message="Success",
                       statusCode=200), 200
    except:
        return jsonify(isError=True,
                       data={},
                       message="Error",
                       statusCode=200), 200


@app.route('/add_position', methods=['POST'])
@cross_origin()
def add_position():
    try:
        data = request.json
        index = server.add_position(**data)
        return jsonify(isError=False,
                       data={"index": index},
                       message="Success",
                       statusCode=200), 200
    except:
        return jsonify(isError=True,
                       index=-1,
                       message="Error",
                       statusCode=200), 200


@app.route('/change_position/<position_id>', methods=['POST'])
@cross_origin()
def change_position(position_id):
    try:
        data = request.json
        index = server.change_position(position_id, **data)
        return jsonify(isError=False,
                       data={"index": index},
                       message="Success",
                       statusCode=200), 200
    except:
        return jsonify(isError=True,
                       index=-1,
                       message="Error",
                       statusCode=200), 200


@app.route('/get_position_by_id/<position_id>')
@cross_origin()
def get_position_by_id(position_id):
    try:
        position = server.get_position_by_id(position_id)
        return jsonify(isError=False,
                       data=position,
                       message="Success",
                       statusCode=200), 200
    except:
        return jsonify(isError=True,
                       data={},
                       message="Error",
                       statusCode=200), 200


@app.route('/delete_position_by_id/<position_id>', methods=['DELETE'])
@cross_origin()
def delete_position_by_id(position_id):
    try:
        server.delete_position_by_id(position_id)
        return jsonify(isError=False,
                       data={},
                       message="Success",
                       statusCode=200), 200
    except:
        return jsonify(isError=True,
                       data={},
                       message="Error",
                       statusCode=200), 200


@app.route('/parse_vacancy', methods=['POST'])
@cross_origin()
def parse_vacancy():
    try:
        data = request.json
        print(data)
        vacancy = get_vacancy(data['url'])
        return jsonify(isError=False,
                       data=vacancy,
                       message="Success",
                       statusCode=200), 200
    except:
        return jsonify(isError=True,
                       message="Error",
                       statusCode=200), 200