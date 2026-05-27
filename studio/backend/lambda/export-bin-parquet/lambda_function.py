import base64
import io
import json
import os
import struct

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


s3 = boto3.client("s3")

TRACKSTER_MAGIC = b"TRKS"

FRAME_FLAG_CAN_FD = 0x01
FRAME_FLAG_EXTENDED_ID = 0x02

PREVIEW_LIMIT = 2000
PREVIEW_SCHEMA_VERSION = "2"

CAN_FD_DLC_TO_BYTES = {
    9: 12,
    10: 16,
    11: 20,
    12: 24,
    13: 32,
    14: 48,
    15: 64
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
        "body": json.dumps(body, ensure_ascii=False)
    }


def get_payload(event):
    if "body" not in event:
        return event

    body = event["body"]

    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")

    if isinstance(body, str):
        return json.loads(body)

    return body


def object_exists(bucket_name, key):
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except Exception:
        return False


def read_json_from_s3(bucket_name, key):
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def upload_bytes(bucket_name, key, content, content_type):
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=content,
        ContentType=content_type
    )
    return len(content)


def upload_json(bucket_name, key, value):
    content = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":")
    ).encode("utf-8")

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=content,
        ContentType="application/json; charset=utf-8"
    )

    return len(content)


def validate_client_path(client_id, input_key):
    expected_prefix = f"{client_id}/"

    if not input_key.startswith(expected_prefix):
        raise Exception(
            f"Invalid inputKey. Expected key to start with '{expected_prefix}'."
        )


def can_dlc_to_payload_length(dlc):
    if not isinstance(dlc, int) or dlc < 0 or dlc > 15:
        return 0

    if dlc <= 8:
        return dlc

    return CAN_FD_DLC_TO_BYTES.get(dlc, 0)


def build_output_key(input_key):
    directory = os.path.dirname(input_key)
    file_name = os.path.basename(input_key)

    if not file_name.lower().endswith(".bin"):
        raise Exception("Input file must have .bin extension.")

    output_name = f"{file_name[:-4]}.parquet"

    if directory:
        return f"{directory}/{output_name}"

    return output_name


def build_preview_key(output_key):
    return f"{output_key}.json"


def build_run_manifest_key(input_key):
    directory = os.path.dirname(input_key)

    if directory:
        return f"{directory}/run-manifest.json"

    return "run-manifest.json"


def format_can_id(can_id):
    return f"{can_id:X}"


def normalize_can_id_key(can_id):
    if isinstance(can_id, int):
        return f"0x{can_id:X}".lower()

    value = str(can_id).strip()

    if value.startswith("0x") or value.startswith("0X"):
        return f"0x{int(value, 16):X}".lower()

    return f"0x{int(value, 16):X}".lower()


def format_payload(payload):
    return " ".join(f"{byte:02X}" for byte in payload)


def is_valid_cached_preview(preview):
    summary = preview.get("summary", {})

    return (
        preview.get("previewSchemaVersion") == PREVIEW_SCHEMA_VERSION
        and int(summary.get("decodedRowCount", 0)) > 0
        and int(summary.get("decoderCanIdCount", 0)) > 0
    )


def unsigned_payload_value(payload):
    value = 0

    for index, byte in enumerate(payload):
        value |= byte << (8 * index)

    return value


def extract_little_endian_raw(payload, start_bit, bit_length):
    source = unsigned_payload_value(payload)
    mask = (1 << bit_length) - 1
    return (source >> start_bit) & mask


def extract_big_endian_raw(payload, start_bit, bit_length):
    bit_string = "".join(f"{byte:08b}" for byte in payload)

    positions = []
    current = start_bit

    for _ in range(bit_length):
        byte_index = current // 8
        bit_index = current % 8
        network_index = byte_index * 8 + (7 - bit_index)

        positions.append(network_index)

        if bit_index == 0:
            current += 15
        else:
            current -= 1

    raw_bits = ""

    for position in positions:
        if 0 <= position < len(bit_string):
            raw_bits += bit_string[position]
        else:
            raw_bits += "0"

    if not raw_bits:
        return 0

    return int(raw_bits, 2)


def apply_signed(raw_value, bit_length):
    sign_bit = 1 << (bit_length - 1)

    if raw_value & sign_bit:
        return raw_value - (1 << bit_length)

    return raw_value


def field_value(signal, fields, name, default_value=None):
    index = fields.get(name)

    if index is None:
        return default_value

    if not isinstance(signal, list) or index >= len(signal):
        return default_value

    value = signal[index]

    if value is None:
        return default_value

    return value


def decode_signal_value(payload, signal, fields):
    start_bit = int(field_value(signal, fields, "sb", 0))
    bit_length = int(field_value(signal, fields, "bl", 0))
    byte_order = int(field_value(signal, fields, "bo", 0))
    signed = int(field_value(signal, fields, "sg", 0))
    factor = float(field_value(signal, fields, "f", 1))
    offset = float(field_value(signal, fields, "o", 0))

    if bit_length <= 0:
        return None

    if byte_order == 1:
        raw_value = extract_big_endian_raw(payload, start_bit, bit_length)
    else:
        raw_value = extract_little_endian_raw(payload, start_bit, bit_length)

    if signed:
        raw_value = apply_signed(raw_value, bit_length)

    physical_value = raw_value * factor + offset

    if physical_value == int(physical_value):
        return str(int(physical_value))

    return str(round(physical_value, 6))


def build_decoder_from_run_manifest(run_manifest):
    compiled_dbc = run_manifest.get("dbc", {}).get("compiledDbc", {})
    field_names = compiled_dbc.get("f", [])
    messages = compiled_dbc.get("m", {})

    if not isinstance(field_names, list) or not isinstance(messages, dict):
        return {}

    fields = {
        field_name: index
        for index, field_name in enumerate(field_names)
    }

    decoder = {}

    for can_id_key, entries in messages.items():
        normalized_can_id = normalize_can_id_key(can_id_key)

        if isinstance(entries, dict):
            entries = [entries]

        if not isinstance(entries, list):
            continue

        for entry in entries:
            if not isinstance(entry, dict):
                continue

            message_name = str(
                entry.get("messageName")
                or entry.get("frame", {}).get("n")
                or f"CAN_{can_id_key}"
            )

            frame = entry.get("frame", {})

            if not isinstance(frame, dict):
                continue

            signals = frame.get("s", [])

            if not isinstance(signals, list):
                continue

            for signal in signals:
                signal_name = field_value(signal, fields, "n", "")

                if not signal_name:
                    continue

                decoder.setdefault(normalized_can_id, []).append({
                    "messageName": message_name,
                    "signalName": str(signal_name),
                    "signal": signal,
                    "fields": fields
                })

    return decoder


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


def decode_frame(frame, decoder):
    can_id_key = normalize_can_id_key(frame["can_id"])
    signal_definitions = decoder.get(can_id_key, [])

    decoded = []

    for signal_definition in signal_definitions:
        value = decode_signal_value(
            frame["payload"],
            signal_definition["signal"],
            signal_definition["fields"]
        )

        if value is None:
            continue

        decoded.append({
            "message": signal_definition["messageName"],
            "signal": signal_definition["signalName"],
            "value": value
        })

    return decoded


def build_rows(frames, decoder):
    rows = []

    for frame in frames:
        decoded_signals = decode_frame(frame, decoder)

        base_row = {
            "timestamp": float(f"{frame['timestamp']:.6f}"),
            "channel": frame["channel"],
            "canId": format_can_id(frame["can_id"]),
            "direction": frame["direction"],
            "frameType": frame["frame_type"],
            "dlc": int(frame["dlc"]),
            "payload": format_payload(frame["payload"]),
            "isExtendedId": bool(frame["is_extended_id"])
        }

        if not decoded_signals:
            row = dict(base_row)
            row["message"] = f"CAN_{base_row['canId']}"
            row["name"] = row["message"]
            row["data"] = row["payload"]
            row["signal"] = ""
            row["value"] = ""
            rows.append(row)
            continue

        for decoded_signal in decoded_signals:
            row = dict(base_row)
            row["message"] = decoded_signal["message"]
            row["name"] = decoded_signal["message"]
            row["data"] = row["payload"]
            row["signal"] = decoded_signal["signal"]
            row["value"] = decoded_signal["value"]
            rows.append(row)

    return rows


def build_rows_preview(rows):
    preview = []

    for row in rows[:PREVIEW_LIMIT]:
        preview.append({
            "timestamp": f"{float(row['timestamp']):.6f}",
            "channel": row["channel"],
            "canId": row["canId"],
            "direction": row["direction"],
            "frameType": row["frameType"],
            "dlc": str(row["dlc"]),
            "payload": row["payload"],
            "isExtendedId": bool(row["isExtendedId"]),
            "message": row["message"],
            "name": row["name"],
            "data": row["data"],
            "signal": row["signal"],
            "value": row["value"]
        })

    return preview


def build_preview_json(
    input_key,
    output_key,
    preview_key,
    run_manifest_key,
    input_file_size,
    output_file_size,
    frames,
    rows,
    duration_seconds,
    decoder
):
    unique_can_ids = {
        frame["can_id"]
        for frame in frames
    }

    channels = {
        frame["channel"]
        for frame in frames
    }

    decoded_rows = [
        row
        for row in rows
        if row.get("signal")
    ]

    decoder_signal_count = sum(
        len(signals)
        for signals in decoder.values()
    )

    return {
        "manifestVersion": "1",
        "previewSchemaVersion": PREVIEW_SCHEMA_VERSION,
        "format": "parquet",
        "inputKey": input_key,
        "outputKey": output_key,
        "manifestKey": preview_key,
        "previewKey": preview_key,
        "runManifestKey": run_manifest_key,
        "inputFileSize": input_file_size,
        "outputFileSize": output_file_size,
        "summary": {
            "frameCount": len(frames),
            "rowCount": len(rows),
            "decodedRowCount": len(decoded_rows),
            "previewCount": min(len(rows), PREVIEW_LIMIT),
            "previewLimit": PREVIEW_LIMIT,
            "uniqueCanIdCount": len(unique_can_ids),
            "channelCount": len(channels),
            "durationSeconds": duration_seconds,
            "decoderCanIdCount": len(decoder),
            "decoderSignalCount": decoder_signal_count
        },
        "columns": [
            "timestamp",
            "channel",
            "canId",
            "direction",
            "frameType",
            "dlc",
            "payload",
            "isExtendedId",
            "message",
            "signal",
            "value"
        ],
        "rowsPreview": build_rows_preview(rows)
    }


def build_parquet_bytes(rows):
    schema = pa.schema([
        ("timestamp", pa.float64()),
        ("channel", pa.string()),
        ("canId", pa.string()),
        ("direction", pa.string()),
        ("frameType", pa.string()),
        ("dlc", pa.int32()),
        ("payload", pa.string()),
        ("isExtendedId", pa.bool_()),
        ("message", pa.string()),
        ("name", pa.string()),
        ("data", pa.string()),
        ("signal", pa.string()),
        ("value", pa.string())
    ])

    table = pa.Table.from_pylist(rows, schema=schema)

    output = io.BytesIO()

    pq.write_table(
        table,
        output,
        compression="snappy"
    )

    return output.getvalue()


def export_single_parquet(
    input_bucket_name,
    output_bucket_name,
    manifest_bucket_name,
    client_id,
    input_key,
    explicit_run_manifest_key
):
    validate_client_path(client_id, input_key)

    output_key = build_output_key(input_key)
    preview_key = build_preview_key(output_key)

    parquet_exists = object_exists(output_bucket_name, output_key)
    preview_exists = object_exists(output_bucket_name, preview_key)

    if parquet_exists and preview_exists:
        cached_preview = read_json_from_s3(output_bucket_name, preview_key)

        if is_valid_cached_preview(cached_preview):
            return {
                "inputKey": input_key,
                "outputKey": output_key,
                "manifestKey": preview_key,
                "previewKey": preview_key,
                "frameCount": int(cached_preview.get("summary", {}).get("frameCount", 0)),
                "rowCount": int(cached_preview.get("summary", {}).get("rowCount", 0)),
                "decodedRowCount": int(cached_preview.get("summary", {}).get("decodedRowCount", 0)),
                "fileSize": int(cached_preview.get("outputFileSize", 0)),
                "manifestFileSize": 0,
                "preview": cached_preview,
                "status": "cached"
            }

    s3_object = s3.get_object(
        Bucket=input_bucket_name,
        Key=input_key
    )

    buffer = s3_object["Body"].read()
    input_file_size = len(buffer)

    parsed = parse_trackster_bin(buffer)

    frames = parsed["frames"]
    duration_seconds = parsed["durationSeconds"]

    run_manifest_key = explicit_run_manifest_key or build_run_manifest_key(input_key)

    if not object_exists(manifest_bucket_name, run_manifest_key):
        raise Exception(
            f"Run manifest not found. Expected: s3://{manifest_bucket_name}/{run_manifest_key}"
        )

    run_manifest = read_json_from_s3(
        manifest_bucket_name,
        run_manifest_key
    )

    decoder = build_decoder_from_run_manifest(run_manifest)

    if not decoder:
        raise Exception(
            "Run manifest was found, but dbc.compiledDbc did not produce a decoder."
        )

    rows = build_rows(frames, decoder)

    preview = build_preview_json(
        input_key,
        output_key,
        preview_key,
        run_manifest_key,
        input_file_size,
        0,
        frames,
        rows,
        duration_seconds,
        decoder
    )

    if int(preview["summary"]["decodedRowCount"]) <= 0:
        raise Exception(
            "Decoder was built, but no frame was decoded. Check CAN IDs between BIN and run-manifest."
        )

    parquet_bytes = build_parquet_bytes(rows)

    output_file_size = upload_bytes(
        output_bucket_name,
        output_key,
        parquet_bytes,
        "application/octet-stream"
    )

    preview["outputFileSize"] = output_file_size

    preview_file_size = upload_json(
        output_bucket_name,
        preview_key,
        preview
    )

    return {
        "inputKey": input_key,
        "outputKey": output_key,
        "manifestKey": preview_key,
        "previewKey": preview_key,
        "runManifestKey": run_manifest_key,
        "frameCount": len(frames),
        "rowCount": len(rows),
        "decodedRowCount": int(preview["summary"]["decodedRowCount"]),
        "decoderCanIdCount": int(preview["summary"]["decoderCanIdCount"]),
        "decoderSignalCount": int(preview["summary"]["decoderSignalCount"]),
        "fileSize": output_file_size,
        "manifestFileSize": preview_file_size,
        "preview": preview,
        "status": "exported"
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

        manifest_bucket_name = payload.get(
            "manifestBucketName",
            input_bucket_name
        )

        client_id = payload.get(
            "clientId",
            "00000000"
        )

        input_keys = payload["inputKeys"]
        run_manifest_key = payload.get("runManifestKey")

        if not isinstance(input_keys, list) or len(input_keys) == 0:
            raise Exception("inputKeys must be a non-empty array.")

        results = []
        errors = []

        for input_key in input_keys:
            try:
                result = export_single_parquet(
                    input_bucket_name,
                    output_bucket_name,
                    manifest_bucket_name,
                    client_id,
                    input_key,
                    run_manifest_key
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
                "message": "Parquet export completed with errors",
                "inputBucketName": input_bucket_name,
                "outputBucketName": output_bucket_name,
                "manifestBucketName": manifest_bucket_name,
                "clientId": client_id,
                "exportedCount": len(results),
                "failedCount": len(errors),
                "results": results,
                "errors": errors
            })

        return response(200, {
            "message": "Parquet export completed successfully",
            "inputBucketName": input_bucket_name,
            "outputBucketName": output_bucket_name,
            "manifestBucketName": manifest_bucket_name,
            "clientId": client_id,
            "exportedCount": len(results),
            "failedCount": 0,
            "results": results,
            "errors": []
        })

    except Exception as error:
        return response(500, {
            "message": "Parquet export failed",
            "error": str(error)
        })