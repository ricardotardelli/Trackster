import json
import os
import tempfile
import boto3
import numpy as np

from asammdf import MDF, Signal


s3 = boto3.client("s3")


def lambda_handler(event, context):
    body = parse_body(event)

    input_bucket = body["inputBucketName"]
    output_bucket = body["outputBucketName"]
    client_id = body["clientId"]
    input_keys = body["inputKeys"]

    if not isinstance(input_keys, list) or len(input_keys) == 0:
        raise ValueError("inputKeys must be a non-empty list")

    results = []

    for input_key in input_keys:
        if not input_key.startswith(f"{client_id}/"):
            raise ValueError(f"inputKey is outside clientId folder: {input_key}")

        output_key = input_key.rsplit(".", 1)[0] + ".mf4"

        if not output_key.startswith(f"{client_id}/"):
            raise ValueError(f"outputKey is outside clientId folder: {output_key}")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_mf4 = os.path.join(tmpdir, "output.mf4")

            timestamps = np.array([0.0, 0.1, 0.2, 0.3], dtype=float)
            samples = np.array([10, 20, 30, 40], dtype=np.float64)

            signal = Signal(
                samples=samples,
                timestamps=timestamps,
                name="Trackster_Test_Channel",
                unit="raw"
            )

            mdf = MDF(version="4.10")
            mdf.append(signal)
            mdf.save(local_mf4, overwrite=True)

            s3.upload_file(local_mf4, output_bucket, output_key)

        results.append({
            "inputKey": input_key,
            "outputKey": output_key,
            "status": "created"
        })

    return response(200, {
        "message": "MF4 export completed",
        "results": results
    })


def parse_body(event):
    if isinstance(event, dict) and "body" in event:
        body = event["body"]
        if isinstance(body, str):
            return json.loads(body)
        return body

    return event


def response(status_code, payload):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(payload)
    }