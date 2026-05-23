import base64
import json
import os
import struct
from datetime import datetime

import boto3


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

MANIFEST_PREVIEW_LIMIT = 500


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

    output_file_name = f"{file_name[:-4]}.asc"

    if directory:
        return f"{directory}/{output_file_name}"

    return output_file_name


def build_manifest_key(output_key):
    return f"{output_key}.json"


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

    for block_index in range(block_count):
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

        for frame_index in range(frame_count):
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
                "index": len(frames),
                "blockIndex": block_index,
                "frameIndex": frame_index,
                "timestamp": timestamp_delta_ns / 1_000_000_000.0,
                "timestamp_ns": timestamp_delta_ns,
                "can_id": can_id,
                "bus": bus,
                "dlc": dlc_code,
                "payload_length": payload_length,
                "payload": payload,
                "is_can_fd": (flags & FRAME_FLAG_CAN_FD) != 0,
                "is_extended_id": (flags & FRAME_FLAG_EXTENDED_ID) != 0
            })

            frame_offset += frame_fixed_header_bytes + payload_length

            if frame_offset > end_offset:
                break

        offset += block_size_bytes

    return {
        "frames": frames,
        "tracksterHeader": {
            "magic": "TRKS",
            "headerBytes": header_bytes,
            "blockHeaderBytes": block_header_bytes,
            "frameFixedHeaderBytes": frame_fixed_header_bytes,
            "blockCount": block_count
        }
    }


def build_vector_asc(frames):
    lines = []

    lines.append(f"date {build_asc_date_line()}")
    lines.append("base hex  timestamps absolute")
    lines.append("internal events logged")
    lines.append("Begin Triggerblock")

    for frame in frames:
        lines.append(
            build_asc_frame_line(frame)
        )

    lines.append("End TriggerBlock")

    return "\n".join(lines)


def build_asc_date_line():
    now = datetime.now()

    return now.strftime(
        "%a %b %d %H:%M:%S %Y"
    )


def build_asc_frame_line(frame):
    timestamp = frame["timestamp"]
    channel = frame["bus"]

    can_id = format_can_id(
        frame["can_id"],
        frame["is_extended_id"]
    )

    direction = "Rx"

    data = bytes_to_hex(
        frame["payload"]
    )

    payload_length = frame["payload_length"]

    if frame["is_can_fd"]:

        return (
            f"{timestamp:12.6f} "
            f"{channel} "
            f"CANFD "
            f"{can_id} "
            f"{direction} "
            f"d "
            f"{frame['dlc']} "
            f"{payload_length}"
            f"{(' ' + data) if data else ''}"
        )

    return (
        f"{timestamp:12.6f} "
        f"{channel} "
        f"{can_id} "
        f"{direction} "
        f"d "
        f"{frame['dlc']}"
        f"{(' ' + data) if data else ''}"
    )


def build_manifest(
    input_bucket_name,
    output_bucket_name,
    client_id,
    input_key,
    output_key,
    manifest_key,
    input_file_size,
    output_file_size,
    frames,
    trackster_header
):
    can_count = sum(
        1 for frame in frames
        if not frame["is_can_fd"]
    )

    can_fd_count = sum(
        1 for frame in frames
        if frame["is_can_fd"]
    )

    extended_count = sum(
        1 for frame in frames
        if frame["is_extended_id"]
    )

    buses = sorted(
        set(frame["bus"] for frame in frames)
    )

    can_ids = sorted(
        set(frame["can_id"] for frame in frames)
    )

    duration_seconds = 0.0

    if frames:
        duration_seconds = (
            max(frame["timestamp"] for frame in frames) -
            min(frame["timestamp"] for frame in frames)
        )

    preview_messages = [
        build_manifest_message(frame)
        for frame in frames[:MANIFEST_PREVIEW_LIMIT]
    ]

    return {
        "format": "TRACKSTER_VECTOR_ASC_MANIFEST",
        "manifestVersion": 1,
        "inputBucketName": input_bucket_name,
        "outputBucketName": output_bucket_name,
        "clientId": client_id,
        "inputKey": input_key,
        "outputKey": output_key,
        "manifestKey": manifest_key,
        "inputFileSize": input_file_size,
        "outputFileSize": output_file_size,
        "tracksterHeader": trackster_header,
        "summary": {
            "frameCount": len(frames),
            "canMessageCount": can_count,
            "canFdMessageCount": can_fd_count,
            "extendedIdCount": extended_count,
            "standardIdCount": len(frames) - extended_count,
            "busCount": len(buses),
            "uniqueCanIdCount": len(can_ids),
            "durationSeconds": duration_seconds,
            "previewCount": len(preview_messages),
            "previewLimit": MANIFEST_PREVIEW_LIMIT
        },
        "messagesPreview": preview_messages
    }


def build_manifest_message(frame):
    return {
        "index": frame["index"],
        "time": f'{frame["timestamp"]:.6f} s',
        "timestampSeconds": frame["timestamp"],
        "timestampDeltaNs": frame["timestamp_ns"],
        "type": "CAN FD" if frame["is_can_fd"] else "CAN",
        "bus": frame["bus"],
        "canId": format_can_id(
            frame["can_id"],
            frame["is_extended_id"]
        ),
        "canIdDecimal": frame["can_id"],
        "dlc": frame["dlc"],
        "payloadLength": frame["payload_length"],
        "payload": bytes_to_hex(frame["payload"]),
        "flags": format_flags(frame)
    }


def bytes_to_hex(payload):
    return " ".join(
        byte.to_bytes(1, "little").hex().upper()
        for byte in payload
    )


def format_can_id(can_id, is_extended=False):
    width = 8 if is_extended else 3

    value = f"{can_id:0{width}X}"

    if is_extended:
        return f"{value}x"

    return value


def format_flags(frame):
    values = []

    if frame["is_extended_id"]:
        values.append("EXT")

    if frame["is_can_fd"]:
        values.append("FD")

    return " · ".join(values) if values else "-"


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


def export_single_vectorasc(
    input_bucket_name,
    output_bucket_name,
    client_id,
    input_key,
    index
):
    validate_client_path(
        client_id,
        input_key
    )

    output_key = build_output_key(
        input_key
    )

    manifest_key = build_manifest_key(
        output_key
    )

    input_path = f"/tmp/trackster-input-{index}.bin"
    output_path = f"/tmp/trackster-output-{index}.asc"

    s3.download_file(
        input_bucket_name,
        input_key,
        input_path
    )

    input_file_size = os.path.getsize(
        input_path
    )

    with open(input_path, "rb") as file:
        buffer = file.read()

    parsed = parse_trackster_bin(
        buffer
    )

    frames = parsed["frames"]

    trackster_header = parsed["tracksterHeader"]

    asc_text = build_vector_asc(
        frames
    )

    with open(output_path, "w", encoding="utf-8") as file:
        file.write(asc_text)

    output_file_size = os.path.getsize(
        output_path
    )

    manifest = build_manifest(
        input_bucket_name=input_bucket_name,
        output_bucket_name=output_bucket_name,
        client_id=client_id,
        input_key=input_key,
        output_key=output_key,
        manifest_key=manifest_key,
        input_file_size=input_file_size,
        output_file_size=output_file_size,
        frames=frames,
        trackster_header=trackster_header
    )

    manifest_bytes = json.dumps(
        manifest,
        indent=2
    ).encode("utf-8")

    s3.upload_file(
        output_path,
        output_bucket_name,
        output_key,
        ExtraArgs={
            "ContentType": "text/plain"
        }
    )

    s3.put_object(
        Bucket=output_bucket_name,
        Key=manifest_key,
        Body=manifest_bytes,
        ContentType="application/json"
    )

    return {
        "inputKey": input_key,
        "outputKey": output_key,
        "manifestKey": manifest_key,
        "frameCount": len(frames),
        "fileSize": output_file_size,
        "manifestFileSize": len(manifest_bytes),
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
                result = export_single_vectorasc(
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
                "message": "Vector ASC export completed with errors",
                "inputBucketName": input_bucket_name,
                "outputBucketName": output_bucket_name,
                "clientId": client_id,
                "exportedCount": len(results),
                "failedCount": len(errors),
                "results": results,
                "errors": errors
            })

        return response(200, {
            "message": "Vector ASC export completed successfully",
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
            "message": "Vector ASC export failed",
            "error": str(error)
        })