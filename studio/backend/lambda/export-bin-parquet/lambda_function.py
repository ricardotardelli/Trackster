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

    output_file_name = f"{file_name[:-4]}.parquet"

    if directory:
        return f"{directory}/{output_file_name}"

    return output_file_name


def build_manifest_key(output_key):
    return f"{output_key}.json"


def format_can_id(can_id):
    return f"{can_id:X}"


def normalize_can_id_key(can_id):
    if isinstance(can_id, int):
        return f"0x{can_id:X}"

    value = str(can_id).strip()

    if value.startswith("0x") or value.startswith("0X"):
        return f"0x{int(value, 16):X}"

    return f"0x{int(value, 16):X}"


def format_payload(payload):
    return " ".join(f"{byte:02X}" for byte in payload)


def read_json_from_s3(bucket_name, key):
    s3_object = s3.get_object(
        Bucket=bucket_name,
        Key=key
    )

    content = s3_object["Body"].read().decode("utf-8")
    return json.loads(content)


def resolve_run_manifest_key(input_key):
    directory = os.path.dirname(input_key)
    file_name = os.path.basename(input_key)

    base_name = file_name[:-4] if file_name.lower().endswith(".bin") else file_name

    candidates = [
        f"{directory}/{base_name}.json" if directory else f"{base_name}.json",
        f"{directory}/{base_name}.manifest.json" if directory else f"{base_name}.manifest.json",
        f"{directory}/{base_name}.run-manifest.json" if directory else f"{base_name}.run-manifest.json",
        f"{directory}/run-manifest.json" if directory else "run-manifest.json"
    ]

    return candidates


def find_existing_manifest_key(bucket_name, input_key, explicit_key):
    if explicit_key:
        return explicit_key

    for candidate in resolve_run_manifest_key(input_key):
        try:
            s3.head_object(
                Bucket=bucket_name,
                Key=candidate
            )
            return candidate
        except Exception:
            pass

    return None


def collect_compiled_dbc_keys(value):
    keys = []

    if isinstance(value, dict):
        for key, nested_value in value.items():
            lower_key = str(key).lower()

            if lower_key in [
                "compiledkey",
                "compiledjsonkey",
                "compileddbcjsonkey",
                "compileddbcpath",
                "compiledpath",
                "jsonkey",
                "key"
            ]:
                if isinstance(nested_value, str) and nested_value.lower().endswith(".json"):
                    keys.append(nested_value)

            keys.extend(collect_compiled_dbc_keys(nested_value))

    elif isinstance(value, list):
        for item in value:
            keys.extend(collect_compiled_dbc_keys(item))

    elif isinstance(value, str):
        lower_value = value.lower()

        if (
            lower_value.endswith(".json")
            and (
                "dbc" in lower_value
                or "compiled" in lower_value
            )
        ):
            keys.append(value)

    return list(dict.fromkeys(keys))


def get_compiled_field_map(compiled_json):
    fields = compiled_json.get("f")

    if not isinstance(fields, list):
        return {}

    return {
        field_name: index
        for index, field_name in enumerate(fields)
    }


def get_signal_property(signal, field_map, name, default_value=None):
    if isinstance(signal, dict):
        return signal.get(name, default_value)

    if isinstance(signal, list):
        index = field_map.get(name)

        if index is not None and index < len(signal):
            return signal[index]

    return default_value


def extract_signal_name(signal, field_map, fallback_name):
    for key in ["name", "n", "signal", "sg"]:
        value = get_signal_property(signal, field_map, key)

        if value is not None and value != "":
            return str(value)

    return fallback_name


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

    return int(raw_bits, 2) if raw_bits else 0


def apply_signed(raw_value, bit_length):
    sign_bit = 1 << (bit_length - 1)

    if raw_value & sign_bit:
        return raw_value - (1 << bit_length)

    return raw_value


def decode_signal(payload, signal, field_map):
    start_bit = int(get_signal_property(signal, field_map, "sb", 0))
    bit_length = int(get_signal_property(signal, field_map, "bl", 0))
    byte_order = int(get_signal_property(signal, field_map, "bo", 0))
    signed = int(get_signal_property(signal, field_map, "sg", 0))
    factor = float(get_signal_property(signal, field_map, "f", 1))
    offset = float(get_signal_property(signal, field_map, "o", 0))

    if bit_length <= 0:
        return None

    if byte_order == 1:
        raw_value = extract_big_endian_raw(payload, start_bit, bit_length)
    else:
        raw_value = extract_little_endian_raw(payload, start_bit, bit_length)

    if signed:
        raw_value = apply_signed(raw_value, bit_length)

    return raw_value * factor + offset


def load_decoder_from_compiled_jsons(bucket_name, compiled_keys):
    decoder = {}

    for compiled_key in compiled_keys:
        compiled_json = read_json_from_s3(bucket_name, compiled_key)
        field_map = get_compiled_field_map(compiled_json)

        messages = compiled_json.get("m", {})

        if not isinstance(messages, dict):
            continue

        for can_id_key, message in messages.items():
            normalized_can_id = normalize_can_id_key(can_id_key)

            if not isinstance(message, dict):
                continue

            signals = message.get("s", [])

            if not isinstance(signals, list):
                continue

            decoder.setdefault(normalized_can_id, [])

            for index, signal in enumerate(signals):
                signal_name = extract_signal_name(
                    signal,
                    field_map,
                    f"Signal_{index + 1}"
                )

                decoder[normalized_can_id].append({
                    "name": signal_name,
                    "definition": signal,
                    "fieldMap": field_map,
                    "sourceCompiledKey": compiled_key
                })

    return decoder


def decode_frame_signals(frame, decoder):
    can_id_key = normalize_can_id_key(frame["can_id"])
    signal_definitions = decoder.get(can_id_key, [])

    decoded = []

    for signal_definition in signal_definitions:
        value = decode_signal(
            frame["payload"],
            signal_definition["definition"],
            signal_definition["fieldMap"]
        )

        if value is None:
            continue

        decoded.append({
            "signal": signal_definition["name"],
            "value": value,
            "sourceCompiledKey": signal_definition["sourceCompiledKey"]
        })

    return decoded


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


def build_enriched_rows(frames, decoder):
    rows = []

    for frame in frames:
        decoded_signals = decode_frame_signals(frame, decoder)

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
            row["signal"] = ""
            row["value"] = ""
            row["sourceCompiledKey"] = ""
            rows.append(row)
            continue

        for decoded_signal in decoded_signals:
            row = dict(base_row)
            row["signal"] = decoded_signal["signal"]
            row["value"] = str(decoded_signal["value"])
            row["sourceCompiledKey"] = decoded_signal["sourceCompiledKey"]
            rows.append(row)

    return rows


def build_rows_preview(rows):
    preview_rows = []

    for row in rows[:PREVIEW_LIMIT]:
        preview_rows.append({
            "timestamp": f"{row['timestamp']:.6f}",
            "channel": row["channel"],
            "canId": row["canId"],
            "direction": row["direction"],
            "frameType": row["frameType"],
            "dlc": str(row["dlc"]),
            "payload": row["payload"],
            "signal": row["signal"],
            "value": row["value"]
        })

    return preview_rows


def build_manifest(
    input_key,
    output_key,
    manifest_key,
    run_manifest_key,
    compiled_dbc_keys,
    input_file_size,
    output_file_size,
    frames,
    rows,
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

    decoded_rows = [
        row
        for row in rows
        if row.get("signal")
    ]

    return {
        "manifestVersion": "1",
        "format": "parquet",
        "inputKey": input_key,
        "outputKey": output_key,
        "manifestKey": manifest_key,
        "runManifestKey": run_manifest_key,
        "compiledDbcKeys": compiled_dbc_keys,
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
            "durationSeconds": duration_seconds
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
            "signal",
            "value",
            "sourceCompiledKey"
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
        ("signal", pa.string()),
        ("value", pa.string()),
        ("sourceCompiledKey", pa.string())
    ])

    table = pa.Table.from_pylist(
        rows,
        schema=schema
    )

    output = io.BytesIO()

    pq.write_table(
        table,
        output,
        compression="snappy"
    )

    return output.getvalue()


def upload_bytes(bucket_name, key, content, content_type):
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=content,
        ContentType=content_type
    )

    return len(content)


def upload_text(bucket_name, key, text, content_type):
    encoded = text.encode("utf-8")

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=encoded,
        ContentType=content_type
    )

    return len(encoded)


def export_single_parquet(
    input_bucket_name,
    output_bucket_name,
    manifest_bucket_name,
    dbc_bucket_name,
    client_id,
    input_key,
    explicit_run_manifest_key
):
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

    run_manifest_key = find_existing_manifest_key(
        manifest_bucket_name,
        input_key,
        explicit_run_manifest_key
    )

    compiled_dbc_keys = []
    decoder = {}

    if run_manifest_key:
        run_manifest = read_json_from_s3(
            manifest_bucket_name,
            run_manifest_key
        )

        compiled_dbc_keys = collect_compiled_dbc_keys(run_manifest)

        if compiled_dbc_keys:
            decoder = load_decoder_from_compiled_jsons(
                dbc_bucket_name,
                compiled_dbc_keys
            )

    rows = build_enriched_rows(frames, decoder)

    parquet_bytes = build_parquet_bytes(rows)

    output_file_size = upload_bytes(
        output_bucket_name,
        output_key,
        parquet_bytes,
        "application/octet-stream"
    )

    manifest = build_manifest(
        input_key,
        output_key,
        manifest_key,
        run_manifest_key,
        compiled_dbc_keys,
        input_file_size,
        output_file_size,
        frames,
        rows,
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
        "runManifestKey": run_manifest_key,
        "compiledDbcKeys": compiled_dbc_keys,
        "frameCount": len(frames),
        "rowCount": len(rows),
        "decodedRowCount": len([
            row
            for row in rows
            if row.get("signal")
        ]),
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

        manifest_bucket_name = payload.get(
            "manifestBucketName",
            input_bucket_name
        )

        dbc_bucket_name = payload.get(
            "dbcBucketName",
            manifest_bucket_name
        )

        client_id = payload.get("clientId", "00000000")
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
                    dbc_bucket_name,
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
                "dbcBucketName": dbc_bucket_name,
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
            "dbcBucketName": dbc_bucket_name,
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