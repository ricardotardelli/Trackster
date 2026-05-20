import base64
import json
import os
import struct

import boto3
import can

from can.io.blf import BLFWriter


s3 = boto3.client("s3")

TRACKSTER_MAGIC = b"TRKS"

FRAME_FLAG_CAN_FD = 0x01
FRAME_FLAG_EXTENDED_ID = 0x02

CAN_FD_DLC_TO_BYTES = {
    9: 12,
    10: 16,
    11: 20,
    12: 24,
    13: 32,
    14: 48,
    15: 64
}


def can_dlc_to_payload_length(dlc):
    if not isinstance(dlc, int) or dlc < 0 or dlc > 15:
        return 0

    if dlc <= 8:
        return dlc

    return CAN_FD_DLC_TO_BYTES.get(dlc, 0)


def get_payload(event):
    if "body" not in event:
        return event

    body = event["body"]

    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    if isinstance(body, str):
        return json.loads(body)

    return body


def validate_client_path(client_id, input_key):
    expected_prefix = f"{client_id}/"

    if not input_key.startswith(expected_prefix):
        raise Exception(
            f"Invalid inputKey. Expected key to start with '{expected_prefix}'."
        )


def build_output_key(input_key):
    directory = os.path.dirname(input_key)
    file_name = os.path.basename(input_key)

    if not file_name.lower().endswith(".bin"):
        raise Exception("Input file must have .bin extension.")

    output_file_name = f"{file_name[:-4]}.blf"

    if directory:
        return f"{directory}/{output_file_name}"

    return output_file_name


def parse_trackster_bin(buffer):
    if len(buffer) < 40:
        raise Exception(
            f"Invalid Trackster BIN. Expected at least 40 bytes, received {len(buffer)}."
        )

    if buffer[0:4] != TRACKSTER_MAGIC:
        raise Exception("Invalid Trackster BIN magic. Expected TRKS.")

    header_bytes = struct.unpack_from("<H", buffer, 8)[0]
    block_header_bytes = struct.unpack_from("<H", buffer, 10)[0]
    frame_fixed_header_bytes = struct.unpack_from("<H", buffer, 12)[0]
    block_count = struct.unpack_from("<I", buffer, 16)[0]

    if len(buffer) < header_bytes:
        raise Exception(
            f"Invalid Trackster BIN. Header declares {header_bytes} bytes, "
            f"but file has only {len(buffer)} bytes."
        )

    frames = []
    offset = header_bytes

    for _ in range(block_count):
        if offset + block_header_bytes > len(buffer):
            break

        block_offset = offset

        frame_count = struct.unpack_from("<I", buffer, block_offset + 16)[0]
        payload_bytes = struct.unpack_from("<I", buffer, block_offset + 20)[0]
        block_size_bytes = struct.unpack_from("<I", buffer, block_offset + 24)[0]

        if block_size_bytes <= 0:
            break

        frame_offset = block_offset + block_header_bytes
        end_offset = frame_offset + payload_bytes

        for _ in range(frame_count):
            if frame_offset + frame_fixed_header_bytes > len(buffer):
                break

            if frame_offset + frame_fixed_header_bytes > end_offset:
                break

            can_id = struct.unpack_from("<I", buffer, frame_offset)[0]
            timestamp_delta_ns = struct.unpack_from("<I", buffer, frame_offset + 4)[0]

            bus = buffer[frame_offset + 8]
            dlc_code = buffer[frame_offset + 9]
            flags = buffer[frame_offset + 10]

            payload_length = can_dlc_to_payload_length(dlc_code)
            payload_offset = frame_offset + frame_fixed_header_bytes
            payload_end = payload_offset + payload_length

            if payload_end > len(buffer):
                break

            if payload_end > end_offset:
                break

            payload = buffer[payload_offset:payload_end]

            frames.append({
                "timestamp": timestamp_delta_ns / 1_000_000_000.0,
                "can_id": can_id,
                "bus": bus,
                "payload": payload,
                "is_can_fd": (flags & FRAME_FLAG_CAN_FD) != 0,
                "is_extended_id": (flags & FRAME_FLAG_EXTENDED_ID) != 0
            })

            frame_offset += frame_fixed_header_bytes + payload_length

            if frame_offset > end_offset:
                break

        offset += block_size_bytes

    return frames


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "content-type,authorization",
            "Access-Control-Allow-Methods": "POST,OPTIONS"
        },
        "body": json.dumps(body)
    }


def export_single_blf(input_bucket_name, output_bucket_name, client_id, input_key, index):
    validate_client_path(client_id, input_key)

    output_key = build_output_key(input_key)

    input_path = f"/tmp/trackster-input-{index}.bin"
    output_path = f"/tmp/trackster-output-{index}.blf"

    s3.download_file(input_bucket_name, input_key, input_path)

    with open(input_path, "rb") as file:
        buffer = file.read()

    frames = parse_trackster_bin(buffer)

    with BLFWriter(output_path) as writer:
        for frame in frames:
            message = can.Message(
                timestamp=frame["timestamp"],
                arbitration_id=frame["can_id"],
                is_extended_id=frame["is_extended_id"],
                is_fd=frame["is_can_fd"],
                data=frame["payload"],
                channel=frame["bus"]
            )

            writer.on_message_received(message)

    output_file_size = os.path.getsize(output_path)

    s3.upload_file(
        output_path,
        output_bucket_name,
        output_key,
        ExtraArgs={
            "ContentType": "application/octet-stream"
        }
    )

    return {
        "inputKey": input_key,
        "outputKey": output_key,
        "frameCount": len(frames),
        "fileSize": output_file_size,
        "status": "exported"
    }


def lambda_handler(event, context):
    try:
        if event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
            return response(200, {"message": "OK"})

        payload = get_payload(event)

        input_bucket_name = payload["inputBucketName"]
        output_bucket_name = payload["outputBucketName"]
        client_id = payload.get("clientId", "00000000")
        input_keys = payload["inputKeys"]

        if not isinstance(input_keys, list) or len(input_keys) == 0:
            raise Exception("inputKeys must be a non-empty array.")

        results = []
        errors = []

        for index, input_key in enumerate(input_keys):
            try:
                result = export_single_blf(
                    input_bucket_name,
                    output_bucket_name,
                    client_id,
                    input_key,
                    index
                )

                results.append(result)

            except Exception as error:
                errors.append({
                    "inputKey": input_key,
                    "status": "failed",
                    "error": str(error)
                })

        if errors:
            return response(207, {
                "message": "BLF export completed with errors",
                "inputBucketName": input_bucket_name,
                "outputBucketName": output_bucket_name,
                "clientId": client_id,
                "exportedCount": len(results),
                "failedCount": len(errors),
                "results": results,
                "errors": errors
            })

        return response(200, {
            "message": "BLF export completed successfully",
            "inputBucketName": input_bucket_name,
            "outputBucketName": output_bucket_name,
            "clientId": client_id,
            "exportedCount": len(results),
            "failedCount": 0,
            "results": results,
            "errors": []
        })

    except Exception as error:
        return response(500, {
            "message": "BLF export failed",
            "error": str(error)
        })