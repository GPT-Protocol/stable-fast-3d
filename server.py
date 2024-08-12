from flask import Flask, request, send_file
import requests
import os
from werkzeug.utils import secure_filename
import uuid
from flask_cors import CORS
import base64
from minio_client import minio_client, minio_bucket_name

app = Flask(__name__)

# This is a very permissive setting; you should limit origins in a production app
CORS(app, resources={r"/process-sf3d": {"origins": "*"}})


@app.route('/process-sf3d', methods=['POST'])
def process_image():
    # Generate a unique ID for this request
    unique_id = str(uuid.uuid4())

    # Extract the Base64 encoded image data from the request
    image_data = request.json['image']
    if image_data.startswith('data:image'):  # Strip the prefix if it's present
        image_data = image_data.split(',')[1]

    # Decode the Base64 string
    image_bytes = base64.b64decode(image_data)

    # Create directories for inputs and outputs based on unique_id
    input_dir = os.path.join('inputs', unique_id)
    output_dir_base = os.path.join('output', f"output-{unique_id}")
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir_base, exist_ok=True)

    # Save the decoded image as a file
    input_path = os.path.join(input_dir, f"{unique_id}.png")
    with open(input_path, 'wb') as f:
        f.write(image_bytes)

    # Process the image
    # Modify your command to reflect the correct processing script and parameters
    os.system(f"/usr/bin/python3.9 /app/stable-fast-3d/run.py {input_path} --output-dir {output_dir_base}")

    # Define the expected output .obj file path
    obj_file_path = os.path.join(output_dir_base, "0", "mesh.glb")

    obj_name = unique_id + ".obj"
    # Store the generated object on minio
    minio_client.fput_object(minio_bucket_name, obj_name, obj_file_path)
    
    # Check if the .obj file exists, and return it
    if os.path.exists(obj_file_path):
        response = send_file(obj_file_path, as_attachment=True,
                             attachment_filename="output.obj", mimetype='application/octet-stream')
        return response
    else:
        # Handle the case where the .obj file does not exist
        return "Output file not found.", 404


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
