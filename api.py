import csv
from flask import Flask, render_template
from flask_restful import reqparse,Resource,Api

app = Flask(__name__)

def read_csv_data(file_path):
    data = []
    with open(file_path, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            data.append(row)
    return data

@app.route('/',methods=['GET' ])
def display_table():
    csv_file = 'data/expectation.csv'   
    table_data = read_csv_data(csv_file)
 
    return render_template('table.html', data=table_data)
api = Api(app)

 

class DataResource(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('dataset', type=str, required=True, help='Name field is required.')
        parser.add_argument('expectation', type=str, required=True, help='Age field is required.')
        parser.add_argument('column', type=str, required=True, help='Country field is required.')
        args = parser.parse_args()
        new_data = {
            'dataset': args['dataset'],
            'expectation': args['expectation'],
            'column': args['column']
        }
        try:
            with open('data/expectation.csv', 'a', newline='') as csvfile:
                fieldnames = ['dataset', 'expectation', 'column']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(new_data)
                return new_data, 201
        except Exception as e:
            return {'message': 'An error occurred while saving the data.', 'error': str(e)}, 500

api.add_resource(DataResource, '/post_data')
 


if __name__ == '__main__':
    app.run(debug=True)
