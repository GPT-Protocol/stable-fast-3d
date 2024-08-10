import pika
import os
import uuid
import base64
import json
from minio_client import minio_client, minio_bucket_name, minio_generated_3d_assets_bucket
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
AMQP_URL = os.environ["AMQP_URL"]
DATABASE_URL = os.environ["DATABASE_URL"]

def update_3d_generation_status(uuid, status) -> None:
    try:
        engine = create_engine(DATABASE_URL)

        with engine.connect() as connection:
            query = text('UPDATE "Generation" SET status = :status WHERE uuid = :uuid AND "fileType" = :fileType')
            
            result = connection.execute(query, {
                "status": status,
                "uuid": uuid,
                "fileType": "MODEL_3D"
            })

            connection.commit()
            print("Updated the status on database.")
        engine.dispose()
    except Exception as e:
        print(f"Error occured while writing to db", e)



def process_image(image_name, channel, method) -> None:
    try:
        image_response = minio_client.get_object(bucket_name=minio_bucket_name, object_name=image_name)
        image_data = base64.b64encode(image_response.read()).decode('utf-8')
        image_bytes = base64.b64decode(image_data)

        image_uuid = image_name.split(".")[0]
        # Create directories for inputs and outputs based on unique_id
        input_dir = os.path.join('inputs', image_uuid)
        output_dir_base = os.path.join('outputs', image_uuid)

        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir_base, exist_ok=True)

        input_path = os.path.join(input_dir, f"{image_name}")
        with open(input_path, 'wb') as f:
            f.write(image_bytes)

        # Process the image
        os.system(f"python run.py {input_path} --output-dir {output_dir_base}")

        # Define the expected output .obj file path
        obj_file_path = os.path.join(output_dir_base, "0", "mesh.obj")

        obj_file_name = image_name.split(".")[0] + ".obj"
        # Store the generated object on minio
        minio_client.fput_object(bucket_name=minio_generated_3d_assets_bucket, object_name=obj_file_name, file_path=obj_file_path)
        print("Obj file uploaded to minio", obj_file_name)
        update_3d_generation_status(image_name.split(".")[0], "DONE")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Failed to fetch image {image_name} from bucket: {str(e)}")
        update_3d_generation_status(image_name.split(".")[0], "FAILED")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return


def callback(ch, method, properties, body):
    # Extract the Base64 encoded image data from the message
    image_data = json.loads(body.decode("utf-8"))
    print(f"Message recvd => {image_data}")
    process_image(image_name=image_data["image"], channel=ch, method=method)


def main():
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()

    channel.queue_declare(queue='test_queue')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='test_queue', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    main()
