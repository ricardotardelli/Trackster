import base64
import csv
import io
import json
import os
import struct

import boto3


s3 = boto3.client("s3")

TRACKSTER_MAGIC = b"TRKS"

FRAME_FLAG_CAN_FD = 0x01
FRAME_FLAG_EXTENDED_ID = 0x02

PREVIEW_LIMIT = 2000

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

    output_file_name = f"{file_name[:-4]}.csv"

    if directory:
        return f"{directory}/{output_file_name}"

    return output_file_name


def build_manifest_key(output_key):
    return f"{output_key}.json"


def format_can_id(can_id):
    return f"{can_id:X}"


def format_payload(payload):
    return " ".join(
        f"{byte:02X}"
        for byte in payload
    )


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
    first_timestamp_ns = None
    last_timestamp_ns = None

    offset = header_bytes

    for _ in range(block_count):
        if offset + block_header_bytes > len(buffer):
            break

        block_offset = offset

        block_timestamp_ns = struct.unpack_from("<Q", buffer, block_offset + 8)[0]
        frame_count = struct.unpack_from("<I", buffer, block_offset + 16)[0]
        payload_bytes = struct.unpack_from("<I", buffer, block_offset + 20)[0]
        block_size_bytes = struct.unpack_from("<I", buffer, block_offset + 24)[0]

        if first_timestamp_ns is None:
            first_timestamp_ns = block_timestamp_ns

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

            absolute_timestamp_ns = block_timestamp_ns + timestamp_delta_ns
            last_timestamp_ns = absolute_timestamp_ns

            relative_timestamp_ns = absolute_timestamp_ns - first_timestamp_ns

            frames.append({
                "timestamp": relative_timestamp_ns / 1_000_000_000.0,
                "channel": str(bus),
                "can_id": can_id,
                "direction": "Rx",
                "frame_type": "CAN FD" if (flags & FRAME_FLAG_CAN_FD) != 0 else "CAN",
                "dlc": dlc_code,
                "payload": payload,
                "is_extended_id": (flags & FRAME_FLAG_EXTENDED_ID) != 0
            })

            frame_offset += frame_fixed_header_bytes + payload_length

            if frame_offset > end_offset:
                break

        offset += block_size_bytes

    duration_seconds = 0.0

    if first_timestamp_ns is not None and last_timestamp_ns is not None:
        duration_seconds = (
            last_timestamp_ns - first_timestamp_ns
        ) / 1_000_000_000.0

    return {
        "frames": frames,
        "durationSeconds": duration_seconds
    }


def build_csv_text(frames):
    output = io.StringIO()

    writer = csv.DictWriter(
        output,
        fieldnames=[
            "timestamp",
            "channel",
            "canId",
            "direction",
            "frameType",
            "dlc",
            "payload"
        ],
        lineterminator="\n"
    )

    writer.writeheader()

    for frame in frames:
        writer.writerow({
            "timestamp": f"{frame['timestamp']:.6f}",
            "channel": frame["channel"],
            "canId": format_can_id(frame["can_id"]),
            "direction": frame["direction"],
            "frameType": frame["frame_type"],
            "dlc": str(frame["dlc"]),
            "payload": format_payload(frame["payload"])
        })

    return output.getvalue()


def build_rows_preview(frames):
    rows = []

    for frame in frames[:PREVIEW_LIMIT]:
        rows.append({
            "timestamp": f"{frame['timestamp']:.6f}",
            "channel": frame["channel"],
            "canId": format_can_id(frame["can_id"]),
            "direction": frame["direction"],
            "frameType": frame["frame_type"],
            "dlc": str(frame["dlc"]),
            "payload": format_payload(frame["payload"])
        })

    return rows


def build_manifest(
    input_key,
    output_key,
    manifest_key,
    input_file_size,
    output_file_size,
    frames,
    duration_seconds
):
    unique_can_ids = {
        frame["can_id"]
        for frame in frames
    }

    channels = {
        frame["channel"]
        for frame in frames
    }

    return {
        "manifestVersion": "1",
        "format": "csv",
        "inputKey": input_key,
        "outputKey": output_key,
        "manifestKey": manifest_key,
        "inputFileSize": input_file_size,
        "outputFileSize": output_file_size,
        "summary": {
            "frameCount": len(frames),
            "previewCount": min(len(frames), PREVIEW_LIMIT),
            "previewLimit": PREVIEW_LIMIT,
            "uniqueCanIdCount": len(unique_can_ids),
            "channelCount": len(channels),
            "durationSeconds": duration_seconds
        },
        "columns": [
            "timestamp",
            "channel",
            "canId",
            "direction",
            "frameType",
            "dlc",
            "payload"
        ],
        "rowsPreview": build_rows_preview(frames)
    }


def upload_text(bucket_name, key, text, content_type):
    encoded = text.encode("utf-8")

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=encoded,
        ContentType=content_type
    )

    return len(encoded)


def export_single_csv(input_bucket_name, output_bucket_name, client_id, input_key):
    validate_client_path(client_id, input_key)

    output_key = build_output_key(input_key)
    manifest_key = build_manifest_key(output_key)

    s3_object = s3.get_object(
        Bucket=input_bucket_name,
        Key=input_key
    )

    buffer = s3_object["Body"].read()
    input_file_size = len(buffer)

    parsed = parse_trackster_bin(buffer)

    frames = parsed["frames"]
    duration_seconds = parsed["durationSeconds"]

    csv_text = build_csv_text(frames)

    output_file_size = upload_text(
        output_bucket_name,
        output_key,
        csv_text,
        "text/csv; charset=utf-8"
    )

    manifest = build_manifest(
        input_key,
        output_key,
        manifest_key,
        input_file_size,
        output_file_size,
        frames,
        duration_seconds
    )

    manifest_text = json.dumps(
        manifest,
        ensure_ascii=False,
        separators=(",", ":")
    )

    manifest_file_size = upload_text(
        output_bucket_name,
        manifest_key,
        manifest_text,
        "application/json; charset=utf-8"
    )

    return {
        "inputKey": input_key,
        "outputKey": output_key,
        "manifestKey": manifest_key,
        "frameCount": len(frames),
        "fileSize": output_file_size,
        "manifestFileSize": manifest_file_size,
        "status": "exported"
    }


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


def lambda_handler(event, context):
    try:
        if event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
            return response(200, {
                "message": "OK"
            })

        payload = get_payload(event)

        input_bucket_name = payload["inputBucketName"]
        output_bucket_name = payload["outputBucketName"]
        client_id = payload.get("clientId", "00000000")
        input_keys = payload["inputKeys"]

        if not isinstance(input_keys, list) or len(input_keys) == 0:
            raise Exception("inputKeys must be a non-empty array.")

        results = []
        errors = []

        for input_key in input_keys:
            try:
                result = export_single_csv(
                    input_bucket_name,
                    output_bucket_name,
                    client_id,
                    input_key
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
                "message": "CSV export completed with errors",
                "inputBucketName": input_bucket_name,
                "outputBucketName": output_bucket_name,
                "clientId": client_id,
                "exportedCount": len(results),
                "failedCount": len(errors),
                "results": results,
                "errors": errors
            })

        return response(200, {
            "message": "CSV export completed successfully",
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
            "message": "CSV export failed",
            "error": str(error)
        })