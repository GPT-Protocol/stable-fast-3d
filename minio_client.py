from minio import Minio
from os import environ
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()

# Parse the MINIO_ENDPOINT to extract host
parsed_endpoint = urlparse(environ["MINIO_ENDPOINT"])
host = parsed_endpoint.hostname or parsed_endpoint.path

# Determine if it's localhost and set the protocol accordingly
is_localhost = host in ('localhost', '127.0.0.1')
protocol = 'http' if is_localhost else 'https'

# Get the port from MINIO_PORT environment variable or use default 9000
port = int(environ.get("MINIO_PORT", 9000))

# Construct the endpoint string
endpoint = f"{host}:{port}"

minio_client = Minio(
    endpoint,
    access_key=environ["MINIO_ACCESS_KEY"],
    secret_key=environ["MINIO_SECRET_KEY"],
    secure=not is_localhost  # Set to True if using HTTPS (non-localhost)
)

minio_bucket_name = "test1"
minio_generated_3d_assets_bucket = "generated-3d-assets"