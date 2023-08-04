import great_expectations as ge
import boto3
import csv
from flask import Flask,redirect,url_for,jsonify,send_from_directory,request,render_template
 
 

app = Flask(__name__)
def read_csv_data(file_path):
    data = []
    with open(file_path, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            data.append(row)
    return data


#Getting the data in the csv file and posting required data
@app.route('/',methods=['GET', 'POST'])
def display_table():
    csv_file = 'data/expectation.csv'  
    table_data = read_csv_data(csv_file)
    if request.method == 'POST':
        dataset = request.form['dataset']
        expectation = request.form['expectation']
        column=request.form['column']
        data={
            'dataset':dataset,
            'expectation':expectation,
            'column':column
        }
        with open('data/expectation.csv', 'a', newline='') as csvfile:
                fieldnames = ['dataset', 'expectation', 'column']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(data)
        return redirect(url_for("gx",expectation=expectation,column=column,dataset=dataset) )
    return render_template('index.html', data=table_data)  


#running great_expectations with the data that user provided
@app.route('/execute/<expectation>/<column>/<dataset>')
def gx(expectation,column,dataset):
    data_context = ge.get_context()
    try:
            my_datasource = data_context.get_datasource("my_s3_datasource") 
            my_batching_regex = dataset+".csv"
            my_asset = my_datasource.add_csv_asset(
            name=dataset, batching_regex=my_batching_regex
        )
    except Exception as e:
         my_asset = data_context.get_datasource("my_s3_datasource").get_asset(dataset)
 
        
    
    my_batch_request = my_asset.build_batch_request( )
    suite = data_context.create_expectation_suite(
    "new_suite", overwrite_existing=True)

    validator = data_context.get_validator(
    batch_request=my_batch_request,
    expectation_suite_name="new_suite",)
    validator.head()

    if expectation == "is_exist":
            expectation_validation_result = validator.expect_column_to_exist(column=column)
 
    elif expectation == "not_null":
            expectation_validation_result = validator.expect_column_values_to_not_be_null(
            column=column)
 
    elif expectation == "in_set":
         expectation_validation_result = validator.expect_column_values_to_be_in_set(column , ["male", "female"])
 
    elif expectation =="unique":
         expectation_validation_result = validator.expect_column_values_to_be_unique(column=column)
 
    elif expectation =="minimum":
         expectation_validation_result = validator.expect_column_min_to_be_between(column=column, min_value=0, max_value=10)
 
    elif expectation =="minimum":
         expectation_validation_result = validator.expect_column_max_to_be_between(column= column, min_value=40, max_value=100)
    else :
         return f"{expectation} is not in the list"
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(expectation_validation_result)   
    checkpoint = data_context.add_or_update_checkpoint(
    name="new_checkpoint",
    validations=[
        {
            "batch_request": my_batch_request,
            "expectation_suite_name": "new_suite",
        },
    ],
)
    
    result = data_context.run_checkpoint(
       checkpoint_name="new_checkpoint",
       run_name="First",
)
 
    data_context.build_data_docs()
    validation_results_html = data_context.view_validation_result(result)
    return send_from_directory(".", validation_results_html)

#getting the list of datasets in the s3 bucket in json format
@app.route('/list_of_files',methods=['GET'])
def get_files_from_s3():
     ACCESS_key=" You access key"
     SECRET_ACCESS_KEY= "Your secret Access key"
     Bucket_name="bucket_name"
 
     s3 = boto3.client('s3', aws_access_key_id=ACCESS_key, aws_secret_access_key=SECRET_ACCESS_KEY)

     try:
        response = s3.list_objects_v2(Bucket=Bucket_name)

        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents']]
            return jsonify({'files': files})

        return jsonify({'message': 'No files found in the bucket.'}), 404

     except Exception as e:
        return jsonify({'error': str(e)}), 500  


if __name__ == "__main__":
    app.run(debug=True)