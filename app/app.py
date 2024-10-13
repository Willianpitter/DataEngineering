from flask import Flask, request, jsonify
from data_ingestion import ingest_trips
from queries import weekly_average_region, weekly_average_lat_long
from sqlalchemy import func, and_
import threading
import os
from datetime import datetime


app = Flask(__name__)
# Store status updates in-memory for simplicity (could use Redis for scalability)
status_updates = {}
UPLOAD_FOLDER = './temp/save/uploads'
now = datetime.now() # current date and time

@app.route('/upload_trips', methods=['POST'])
def upload_trips():
    file = request.files['file']
    # Ensure the upload folder exists
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)    
    
    # Define the file path where the file will be saved
    file_name = file.filename + now.strftime("%m_%d_%Y_%H_%M_%S") + '.csv'
    file_path = os.path.join(UPLOAD_FOLDER, file_name)
    
    # Save the file
    file.save(file_path)
    
    # Use thread ID as job identifier
    job_id = threading.get_ident()  
    status_updates[job_id] = 'In Progress'
    #ingest_trips(file,2)
    # Run ingestion in a separate thread
    thread = threading.Thread(target=ingest_trips, args=(file_path, job_id))
    thread.start()
    
    return jsonify({'message': 'Data ingestion started', 'job_id': job_id})

@app.route('/status/<int:job_id>', methods=['GET'])
def get_status(job_id):
    status = status_updates.get(job_id, 'Unknown Job ID')
    return jsonify({'status': status})


@app.route('/weekly_average', methods=['GET'])
def weekly_average():
    region = request.args.get('region')
    lat_min, lat_max = request.args.get('lat_min'), request.args.get('lat_max')
    long_min, long_max = request.args.get('long_min'), request.args.get('long_max')

    if region:
        weekly_avg = weekly_average_region(region)
    elif lat_min and lat_max and long_min and long_max:
        weekly_avg = weekly_average_lat_long(lat_min,long_min, lat_max, long_max)
    else:
        return jsonify({'error': 'Please provide region or bounding box coordinates'}), 400

    return jsonify({'weekly_average': weekly_avg.to_json()})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
