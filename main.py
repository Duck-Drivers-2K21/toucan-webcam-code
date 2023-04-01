import time
from io import BytesIO
import uuid

import cv2
import boto3
from botocore.exceptions import NoCredentialsError

BUCKET_NAME = 'toucan-data'


def upload_image_to_s3(frame, bucket_name, s3_key):
    """
    Uploads an image to an S3 bucket.

    :param frame: The image to upload.
    :param bucket_name: The name of the S3 bucket to upload the image to.
    :param s3_key: The key (path) where the image will be stored in the S3 bucket.
    :return: A dictionary with the result of the operation, including success status and error messages if any.
    """

    s3 = boto3.client('s3')
    # Encode the frame as a JPEG image
    retval, buffer = cv2.imencode('.jpg', frame)

    if not retval:
        return {'success': False, 'message': "Error encoding frame as JPEG image."}

    # Upload the encoded image to S3
    byte_data = BytesIO(buffer)
    s3.upload_fileobj(byte_data, bucket_name, s3_key)
    return {'success': True, 'message': f"Image uploaded to S3 successfully."}


def get_frame():
    vc = cv2.VideoCapture(0)

    try:
        if vc.isOpened():  # try to get the frame
            rval, frame = vc.read()
        else:
            raise Exception("Failed to get frame")
    finally:
        pass

    return frame


def main():
    while True:
        try:
            frame = get_frame()
        except:
            continue

        print(upload_image_to_s3(frame, BUCKET_NAME, str(uuid.uuid4())+'.jpg'))


if __name__ == '__main__':
    main()
