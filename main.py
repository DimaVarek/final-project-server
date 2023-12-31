from flask import Flask
from flask import request, jsonify
from sql_server.sql_server import SqlServer, DB_NAME
from flask_cors import CORS, cross_origin
from parsing.parser import get_vacancy
from fake_stats.fake_stats import FAKE_STATS

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


@app.route('/positions', methods=['GET'])
@cross_origin()
def positions():
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
                       data={},
                       message="Error",
                       statusCode=200), 200


@app.route('/position/<position_id>', methods=['GET', 'PUT', "DELETE"])
@cross_origin()
def position(position_id):
    if request.method == 'GET':
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
    elif request.method == 'PUT':
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
    elif request.method == 'DELETE':
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
                       data={},
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
                       message="Error",
                       statusCode=200), 200


@app.route('/parse_vacancy', methods=['POST'])
@cross_origin()
def parse_vacancy():
    try:
        data = request.json
        vacancy = get_vacancy(data['url'])
        return jsonify(isError=False,
                       data=vacancy,
                       message="Success",
                       statusCode=200), 200
    except:
        return jsonify(isError=True,
                       message="Error",
                       statusCode=200), 200


@app.route('/get_stages_by_period', methods=['GET'])
@cross_origin()
def get_stages_by_period():
    try:
        start_interval = int(request.args['start_interval'])
        end_interval = int(request.args['end_interval'])
        stages = server.get_stages_from_range(start_interval, end_interval)
        return jsonify(isError=False,
                       data=stages,
                       message="Success",
                       statusCode=200), 200
    except:
        return jsonify(isError=True,
                       message="Error",
                       statusCode=200), 200


@app.route('/statistic', methods=['GET'])
@cross_origin()
def statistic():
    try:
        stat_type = request.args.get('stat_type', default='application_last_six_months')
        fake_stat = request.args.get('fake_stat', default='false')
        stat = []
        if fake_stat == 'true':
            stat = FAKE_STATS[stat_type]
        else:
            if stat_type == 'application_last_six_months':
                stat = server.statistic_applications_last_six_months()
            elif stat_type == 'application_last_four_week':
                stat = server.get_applications_made_last_month()
            elif stat_type == 'application_last_week':
                stat = server.get_applications_made_last_week()
            elif stat_type == 'total_positive_result_by_each_stage':
                stat = server.total_positive_result_by_each_stage()
        return jsonify(isError=False,
                       data=stat,
                       message="Success",
                       statusCode=200), 200
    except:
        return jsonify(isError=True,
                       message="Error",
                       statusCode=200), 200

