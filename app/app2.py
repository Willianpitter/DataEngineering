from flask import Flask, request, jsonify
from data_ingestion import ingest_trips
from queries import weekly_average_region, weekly_average_lat_long
from sqlalchemy import func, and_
import threading
import os
from datetime import datetime
import asyncio
import websockets

app = Flask(__name__)

# Store status updates in-memory for simplicity (could use Redis for scalability)
status_updates = {}
UPLOAD_FOLDER = './temp/save/uploads'
now = datetime.now()  # current date and time

# WebSocket server for real-time updates
async def notify_ingestion_status(websocket, path):
    while True:
        message = await websocket.recv()  # Receive the job ID from the WebSocket client
        if message in status_updates:
            await websocket.send(status_updates[message])  # Send the job status back to the client

async def update_status(job_id, status):
    status_updates[job_id] = status
    await asyncio.sleep(1)  # Simulate task progression for real-time updates

# Flask API endpoint for uploading trips
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
    
    # Run ingestion in a separate thread and update status
    thread = threading.Thread(target=ingest_trips_with_status, args=(file_path, job_id))
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
        weekly_avg = weekly_average_lat_long(lat_min, long_min, lat_max, long_max)
    else:
        return jsonify({'error': 'Please provide region or bounding box coordinates'}), 400

    return jsonify({'weekly_average': weekly_avg.to_json()})

# Helper function to manage ingestion with real-time status updates
def ingest_trips_with_status(file_path, job_id):
    try:
        ingest_trips(file_path, job_id)  # Perform the actual data ingestion
        status_updates[job_id] = 'Completed'
    except Exception as e:
        status_updates[job_id] = f'Error: {str(e)}'

# Start both Flask API and WebSocket server concurrently
def run_flask_and_websocket():
    # Run Flask app in a thread
    flask_thread = threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000, use_reloader=False))
    flask_thread.start()

    # Run WebSocket server in asyncio event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    start_server = websockets.serve(notify_ingestion_status, "localhost", 8765)
    loop.run_until_complete(start_server)
    loop.run_forever()

if __name__ == '__main__':
    run_flask_and_websocket()
