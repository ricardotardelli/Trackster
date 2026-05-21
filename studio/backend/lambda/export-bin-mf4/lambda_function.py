import base64
import json
import os
import struct

import boto3
import numpy as np
from asammdf import MDF, Signal


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

    output_file_name = f"{file_name[:-4]}.mf4"

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


def create_mf4_file(frames, output_path):
    if not frames:
        raise Exception("No CAN frames were found in the Trackster BIN.")

    timestamps = np.array(
        [frame["timestamp"] for frame in frames],
        dtype=np.float64
    )

    can_ids = np.array(
        [frame["can_id"] for frame in frames],
        dtype=np.uint32
    )

    buses = np.array(
        [frame["bus"] for frame in frames],
        dtype=np.uint8
    )

    dlcs = np.array(
        [frame["dlc"] for frame in frames],
        dtype=np.uint8
    )

    payload_lengths = np.array(
        [frame["payload_length"] for frame in frames],
        dtype=np.uint8
    )

    is_can_fd = np.array(
        [1 if frame["is_can_fd"] else 0 for frame in frames],
        dtype=np.uint8
    )

    is_extended_id = np.array(
        [1 if frame["is_extended_id"] else 0 for frame in frames],
        dtype=np.uint8
    )

    timestamp_ns = np.array(
        [frame["timestamp_ns"] for frame in frames],
        dtype=np.uint32
    )

    payload_matrix = np.zeros((len(frames), 64), dtype=np.uint8)

    for row_index, frame in enumerate(frames):
        payload = frame["payload"]
        payload_matrix[row_index, 0:len(payload)] = np.frombuffer(payload, dtype=np.uint8)

    signals = [
        Signal(samples=timestamp_ns, timestamps=timestamps, name="timestamp_delta_ns", unit="ns"),
        Signal(samples=buses, timestamps=timestamps, name="bus", unit=""),
        Signal(samples=can_ids, timestamps=timestamps, name="can_id", unit=""),
        Signal(samples=dlcs, timestamps=timestamps, name="dlc", unit=""),
        Signal(samples=payload_lengths, timestamps=timestamps, name="payload_length", unit="byte"),
        Signal(samples=is_can_fd, timestamps=timestamps, name="is_can_fd", unit=""),
        Signal(samples=is_extended_id, timestamps=timestamps, name="is_extended_id", unit="")
    ]

    for byte_index in range(64):
        signals.append(
            Signal(
                samples=payload_matrix[:, byte_index],
                timestamps=timestamps,
                name=f"data_{byte_index:02d}",
                unit="byte"
            )
        )

    mdf = MDF(version="4.10")
    mdf.append(signals, common_timebase=True)
    mdf.save(output_path, overwrite=True)


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
    can_count = sum(1 for frame in frames if not frame["is_can_fd"])
    can_fd_count = sum(1 for frame in frames if frame["is_can_fd"])
    extended_count = sum(1 for frame in frames if frame["is_extended_id"])

    buses = sorted(set(frame["bus"] for frame in frames))
    can_ids = sorted(set(frame["can_id"] for frame in frames))

    duration_seconds = 0.0

    if frames:
        duration_seconds = max(frame["timestamp"] for frame in frames) - min(frame["timestamp"] for frame in frames)

    preview_messages = [
        build_manifest_message(frame)
        for frame in frames[:MANIFEST_PREVIEW_LIMIT]
    ]

    channels = [
        {"name": "timestamp_delta_ns", "type": "uint32", "unit": "ns"},
        {"name": "bus", "type": "uint8", "unit": ""},
        {"name": "can_id", "type": "uint32", "unit": ""},
        {"name": "dlc", "type": "uint8", "unit": ""},
        {"name": "payload_length", "type": "uint8", "unit": "byte"},
        {"name": "is_can_fd", "type": "uint8", "unit": ""},
        {"name": "is_extended_id", "type": "uint8", "unit": ""}
    ]

    for byte_index in range(64):
        channels.append({
            "name": f"data_{byte_index:02d}",
            "type": "uint8",
            "unit": "byte"
        })

    return {
        "format": "TRACKSTER_MF4_MANIFEST",
        "manifestVersion": 1,
        "mf4Version": "4.10",
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
        "buses": [
            {
                "bus": bus,
                "frameCount": sum(1 for frame in frames if frame["bus"] == bus)
            }
            for bus in buses
        ],
        "channels": channels,
        "canIds": [
            {
                "canId": format_can_id(can_id),
                "decimal": can_id,
                "frameCount": sum(1 for frame in frames if frame["can_id"] == can_id)
            }
            for can_id in can_ids[:500]
        ],
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
        "canId": format_can_id(frame["can_id"], frame["is_extended_id"]),
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
    return f"0x{can_id:0{width}X}"


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


def export_single_mf4(input_bucket_name, output_bucket_name, client_id, input_key, index):
    validate_client_path(client_id, input_key)

    output_key = build_output_key(input_key)
    manifest_key = build_manifest_key(output_key)

    input_path = f"/tmp/trackster-input-{index}.bin"
    output_path = f"/tmp/trackster-output-{index}.mf4"

    s3.download_file(input_bucket_name, input_key, input_path)

    input_file_size = os.path.getsize(input_path)

    with open(input_path, "rb") as file:
        buffer = file.read()

    parsed = parse_trackster_bin(buffer)

    frames = parsed["frames"]
    trackster_header = parsed["tracksterHeader"]

    create_mf4_file(frames, output_path)

    output_file_size = os.path.getsize(output_path)

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
            "ContentType": "application/octet-stream"
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
                result = export_single_mf4(
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
                "message": "MF4 export completed with errors",
                "inputBucketName": input_bucket_name,
                "outputBucketName": output_bucket_name,
                "clientId": client_id,
                "exportedCount": len(results),
                "failedCount": len(errors),
                "results": results,
                "errors": errors
            })

        return response(200, {
            "message": "MF4 export completed successfully",
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
            "message": "MF4 export failed",
            "error": str(error)
        })