"use strict";

/*============================================================================
Trackster Telemetry Worker (SQS)
- Trigger: SQS
- Contract (current): record.body JSON with:
  { vin, type, intervalSec, durationSec  }
- S3 key: <VIN>/can_data_<TIMESTAMP>_<FILE SEQUENCE>.bin
============================================================================*/
const fs = require("fs");
const path = require("path");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const REGION = process.env.AWS_REGION || "us-east-1";
const s3 = new S3Client({ region: REGION });

const BASE_DIR = __dirname;
const DBC_DIR = path.join(BASE_DIR, "dbc");

const GENERIC_HEADER_SIZE = 0x26;         // 38 bytes (keep as-is per your current working validator expectations)
const NUM_CAN_BLOCKS_OFFSET = 0x23;       // 3 bytes (0x23..0x25)

const BLOCK_HEADER_SIZE = 29;
const BLOCK_DATA_SIZE = 0x1E00;
const GPS_BYTES = Buffer.from("77114820178BE135", "hex");

const FD_PAYLOAD_SIZES = [
  0, 1, 2, 3, 4, 5, 6, 7, 8,
  12, 16, 20, 24, 32, 48, 64
];
const specCache = new Map();

async function putObjectNoOverwrite({ Bucket, Key, Body, ContentType }) {
  return s3.send(new PutObjectCommand({
    Bucket,
    Key,
    Body,
    ContentType,
    IfNoneMatch: "*" // only create if object does NOT exist
  }));
}

function isAlreadyExists(err) {
  return err?.$metadata?.httpStatusCode === 412 ||
         err?.name === "PreconditionFailed";
}

function mapToFdSize(minBytes) {
  for (const s of FD_PAYLOAD_SIZES) if (s >= minBytes) return s;
  throw new Error("Payload too large");
}

function fdDlcFromSize(payloadSize) {
  if (payloadSize <= 8) return payloadSize;
  const idx = FD_PAYLOAD_SIZES.indexOf(payloadSize);
  if (idx === -1) throw new Error("Invalid CAN-FD payload size: " + payloadSize);
  return idx; // 12->9, 16->10, 20->11, 24->12, 32->13, 48->14, 64->15
}

function makeRng(seed) {
  let x = (seed >>> 0) || 1;
  return () => {
    x = (x * 16807) % 2147483647;
    return x / 2147483647;
  };
}

function hashStringToU32(str) {
  let h = 2166136261 >>> 0;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619) >>> 0;
  }
  return h >>> 0;
}

function parseNumber(x) {
  if (x === undefined || x === null) return Number.NaN;
  const s = String(x).trim().replace(",", ".");
  if (!s) return Number.NaN;
  const n = Number.parseFloat(s);
  return Number.isFinite(n) ? n : Number.NaN;
}

function writeIntel(payload, startBit, length, rawUnsigned) {
  for (let i = 0; i < length; i++) {
    const bitIndex = startBit + i;
    const byte = Math.floor(bitIndex / 8);
    const bit = bitIndex % 8;
    if (byte < 0 || byte >= payload.length) continue;
    payload[byte] &= ~(1 << bit);
  }

  for (let i = 0; i < length; i++) {
    const bitVal = (rawUnsigned >> i) & 1;
    if (!bitVal) continue;

    const bitIndex = startBit + i;
    const byte = Math.floor(bitIndex / 8);
    const bit = bitIndex % 8;

    if (byte < 0 || byte >= payload.length) continue;
    payload[byte] |= (1 << bit);
  }
}

function writeMotorola(payload, startBit, length, rawUnsigned) {
  let byte = Math.floor(startBit / 8);
  let bitInByte = startBit % 8;

  for (let i = 0; i < length; i++) {
    const actualBit = 7 - bitInByte;
    if (byte >= 0 && byte < payload.length) {
      payload[byte] &= ~(1 << actualBit);
    }
    if (bitInByte === 7) { byte += 1; bitInByte = 0; }
    else bitInByte += 1;
  }

  byte = Math.floor(startBit / 8);
  bitInByte = startBit % 8;

  for (let i = 0; i < length; i++) {
    const bitVal = (rawUnsigned >> (length - 1 - i)) & 1;
    if (bitVal) {
      const actualBit = 7 - bitInByte;
      if (byte >= 0 && byte < payload.length) {
        payload[byte] |= (1 << actualBit);
      }
    }

    if (bitInByte === 7) { byte += 1; bitInByte = 0; }
    else bitInByte += 1;
  }
}

function readIntel(payload, startBit, length) {
  let v = 0;
  for (let i = 0; i < length; i++) {
    const bitIndex = startBit + i;
    const byte = Math.floor(bitIndex / 8);
    const bit = bitIndex % 8;
    if (byte < 0 || byte >= payload.length) continue;
    const bitVal = (payload[byte] >> bit) & 1;
    v |= (bitVal << i);
  }
  return v >>> 0;
}

function readMotorola(payload, startBit, length) {
  let v = 0;
  let byte = Math.floor(startBit / 8);
  let bitInByte = startBit % 8; // 0=MSB..7=LSB

  for (let i = 0; i < length; i++) {
    const actualBit = 7 - bitInByte;
    let bitVal = 0;
    if (byte >= 0 && byte < payload.length) {
      bitVal = (payload[byte] >> actualBit) & 1;
    }
    v = (v << 1) | bitVal;

    if (bitInByte === 7) { byte += 1; bitInByte = 0; }
    else bitInByte += 1;
  }
  return v >>> 0;
}

function toTwosComplementUnsigned(rawSigned, length) {
  const L = BigInt(length);
  const mod = 1n << L;
  const x = BigInt(rawSigned);
  const u = ((x % mod) + mod) % mod;
  return Number(u); // length here is small in DBC; safe to cast for bit ops
}

function parseDbc(text) {
  const lines = text.split(/\r?\n/);
  const messages = {};
  let current = null;

  for (const line of lines) {
    const t = line.trimStart();

    if (t.startsWith("BO_ ")) {
      const m = t.match(/^BO_\s+(\d+)\s+\w+\s*:\s+(\d+)/);
      if (!m) continue;

      current = {
        id: Number(m[1]),
        dlcBytes: Number(m[2]),
        signals: []
      };
      messages[current.id] = current;
      continue;
    }

    if (t.startsWith("SG_ ") && current) {
      const m = t.match(
        /^SG_\s+(\w+)(?:\s+[mM]\d+|\s+M)?\s*:\s*(\d+)\|(\d+)@(\d)([+-])\s+\(([^,]+),([^)]+)\)\s+\[([^|]+)\|([^\]]+)\]/
      );
      if (!m) continue;

      const sigName = m[1];

      current.signals.push({
        name: sigName,
        startBit: Number(m[2]),
        length: Number(m[3]),
        endian: Number(m[4]),
        signed: m[5] === "-",
        scale: parseNumber(m[6]),
        offset: parseNumber(m[7]),
        min: parseNumber(m[8]),
        max: parseNumber(m[9])
      });
    }
  }

  return messages;
}

function chooseRawForSignal(s, rnd) {
  const L = Number(s.length);

  const scale  = Number(s.scale);
  const offset = Number(s.offset);

  const hasPhysRange =
    Number.isFinite(s.min) && Number.isFinite(s.max) &&
    Number.isFinite(scale) && scale !== 0 &&
    Number.isFinite(offset);

  let rawLo, rawHi;
  if (s.signed) {
    rawLo = -(2 ** (L - 1));
    rawHi =  (2 ** (L - 1)) - 1;
  } else {
    rawLo = 0;
    rawHi = (2 ** L) - 1;
  }

  const clamp = (x, lo, hi) => Math.max(lo, Math.min(hi, x));

  if (!hasPhysRange) return 0;

  const physMin = Math.min(s.min, s.max);
  const physMax = Math.max(s.min, s.max);

  // raw range implied by physical range
  let rMin = (physMin - offset) / scale;
  let rMax = (physMax - offset) / scale;

  // FIX #1: handle negative scale
  if (scale < 0) {
    const tmp = rMin; rMin = rMax; rMax = tmp;
  }

  rMin = Math.ceil(rMin);
  rMax = Math.floor(rMax);

  // FIX #2: clamp to representable raw range
  rMin = clamp(rMin, rawLo, rawHi);
  rMax = clamp(rMax, rawLo, rawHi);

  // Normalize if still inverted after clamping
  if (rMax < rMin) {
    const r0 = clamp(Math.round((0 - offset) / scale), rawLo, rawHi);
    return r0;
  }

  // Pull away from edges a bit
  if (rMax - rMin >= 4) { rMin += 1; rMax -= 1; }

  for (let t = 0; t < 10; t++) {
    const pick = rMin + Math.floor(rnd() * (rMax - rMin + 1));
    const phys = pick * scale + offset;
    if (phys >= physMin && phys <= physMax) {
      return pick; // already within [rMin,rMax]
    }
  }

  return Math.round((rMin + rMax) / 2);
}

function buildPayload(msg, rnd) {
  let minBytes = msg.dlcBytes;

  for (const s of msg.signals) {
    const startByte = Math.floor(s.startBit / 8);
    const bytesForSignal = startByte + Math.ceil(s.length / 8);
    if (bytesForSignal > minBytes) minBytes = bytesForSignal;
  }

  const payloadSize = mapToFdSize(minBytes);
  const payload = Buffer.alloc(payloadSize, 0x00);

  for (const s of msg.signals) {
    const raw = chooseRawForSignal(s, rnd);

    let rawUnsigned;
    if (s.signed) rawUnsigned = toTwosComplementUnsigned(raw, s.length);
    else rawUnsigned = raw >>> 0;

    if (s.endian === 1) writeIntel(payload, s.startBit, s.length, rawUnsigned);
    else writeMotorola(payload, s.startBit, s.length, rawUnsigned);
  }

  if (msg.id === 0x3AC || msg.id === 0x390 || msg.id === 0x448) {
    console.log("DIAG CAN", "0x" + msg.id.toString(16).toUpperCase(), "payloadHex", payload.toString("hex"));

    if (msg.id === 0x3AC) {
      const intel = readIntel(payload, 25, 2);
      const moto = readMotorola(payload, 25, 2);
      console.log("DIAG 0x3AC GW_POWER_RES start=25 len=2 -> Intel:", intel, "Motorola:", moto);
    }

    if (msg.id === 0x448) {
      const intel = readIntel(payload, 32, 2);
      const moto = readMotorola(payload, 32, 2);
      console.log("DIAG 0x448 EVSECONFPAYOPT start=32 len=2 -> Intel:", intel, "Motorola:", moto);
    }

    if (msg.id === 0x390) {
      const destIntel = readIntel(payload, 19, 4);
      const destMoto = readMotorola(payload, 19, 4);
      const tempRawIntel = readIntel(payload, 32, 8);
      const tempRawMoto = readMotorola(payload, 32, 8);

      const physIntel = tempRawIntel * 0.5 - 40;
      const physMoto = tempRawMoto * 0.5 - 40;

      console.log("DIAG 0x390 DESTINATION start=19 len=4 -> Intel:", destIntel, "Motorola:", destMoto);
      console.log(
        "DIAG 0x390 OUTTEMP raw start=32 len=8 -> IntelRaw:",
        tempRawIntel,
        "MotorolaRaw:",
        tempRawMoto,
        "IntelPhys:",
        physIntel,
        "MotorolaPhys:",
        physMoto
      );
    }
  }

  return payload;
}

function makeTimestampAbsBytesFromDate(d) {
  const b = Buffer.alloc(8, 0x00);

  const sec   = d.getUTCSeconds();          // 0..59  (valid 0..89)
  const min   = d.getUTCMinutes();          // 0..59  (valid 0..89)
  const hour  = d.getUTCHours();            // 0..23  (valid 0..35)
  const day   = d.getUTCDate();             // 1..31  (valid 1..49)
  const month = d.getUTCMonth() + 1;        // 1..12  (valid 1..18)
  const year  = d.getUTCFullYear() - 2000;  // e.g. 26 (valid 0..153)

  // Layout expected by decoder for 0x661: bytes are fields (not epoch)
  // [0]=SEC, [1]=MIN, [2]=HOUR, [3]=DAY, [4]=MONTH, [5]=YEAR, [6]=0, [7]=0
  b[0] = sec & 0xff;
  b[1] = min & 0xff;
  b[2] = hour & 0xff;
  b[3] = day & 0xff;
  b[4] = month & 0xff;
  b[5] = year & 0xff;

  return b;
}

function makeTimestampRelBytesMs(ms) {
  const b = Buffer.alloc(8, 0x00);
  b.writeBigUInt64BE(BigInt(ms), 0);
  return b;
}

function normalizeDbcName(name) {
  return String(name || "").trim().replace(/\.dbc$/i, "");
}

function resolveDbcPath(name) {
  return path.join(DBC_DIR, `${normalizeDbcName(name)}.dbc`);
}

function resolveDbcFiles(payload) {
  if (!Array.isArray(payload?.dbcFiles)) return [];

  return Array.from(new Set(
    payload.dbcFiles
      .map(normalizeDbcName)
      .filter(Boolean)
  )).sort();
}

function normalizeCanId(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.trunc(value);
  }

  const text = String(value || "").trim();
  if (!text) return Number.NaN;
  if (/^0x/i.test(text)) return Number.parseInt(text, 16);
  return Number.parseInt(text, 10);
}

function resolveCanWhitelist(payload) {
  if (!Array.isArray(payload?.canFrames)) return new Set();

  const canIds = payload.canFrames
    .map(normalizeCanId)
    .filter(Number.isFinite);

  return new Set(canIds);
}

function loadSpec(selectedDbcFiles, whitelist) {
  if (!whitelist.size) throw new Error("canFrames empty in message");

  const dbcNames = selectedDbcFiles;
  if (!dbcNames.length) throw new Error("dbcFiles empty in message");

  const cacheKey = `${dbcNames.join("|")}::${Array.from(whitelist).sort((a, b) => a - b).join("|")}`;
  const cached = specCache.get(cacheKey);
  if (cached) return cached;

  let messages = {};
  for (const name of dbcNames) {
    const fp = resolveDbcPath(name);
    const dbcText = fs.readFileSync(fp, "utf8");
    Object.assign(messages, parseDbc(dbcText));
  }

  const msgList = Object.values(messages)
    .filter(m => whitelist.has(m.id))
    .filter(m => m.id !== 0x335)
    .sort((a, b) => a.id - b.id);

  const spec = { msgList };
  specCache.set(cacheKey, spec);
  return spec;
}

function resolveS3Bucket(payload) {
  const raw = String(
    payload?.s3Bucket ||
    payload?.bucket ||
    payload?.s3_bucket ||
    ""
  ).trim();

  if (!raw) return "";

  if (raw.startsWith("arn:aws:s3:::")) {
    return raw.slice("arn:aws:s3:::".length);
  }

  return raw;

}

function buildGenericHeader(numBlocks, epochMs) {
  const h = Buffer.alloc(GENERIC_HEADER_SIZE, 0x00);

  h.writeUInt16BE(0x0001, 0x00);
  h.writeUInt16BE(0x0002, 0x02);
  h.writeUInt16BE(0x0003, 0x04);

  const tsRel = BigInt(epochMs);
  h.writeBigUInt64BE(tsRel, 0x06);

  h.writeUInt8(0x01, 0x0E);

  const controlLen = numBlocks * (BLOCK_HEADER_SIZE + BLOCK_DATA_SIZE);
  h.writeUInt32BE(controlLen >>> 0, 0x0F);

  h[NUM_CAN_BLOCKS_OFFSET + 0] = (numBlocks >> 16) & 0xff;
  h[NUM_CAN_BLOCKS_OFFSET + 1] = (numBlocks >> 8) & 0xff;
  h[NUM_CAN_BLOCKS_OFFSET + 2] = numBlocks & 0xff;

  return h;
}

function generateBin(msgList, cycles, seed, intervalSec, epochMs) {
  const rnd = makeRng(seed);

  const FRAMES_PER_BLOCK = 150;
  const blocksPerCycle = Math.ceil(msgList.length / FRAMES_PER_BLOCK);
  const totalBlocks = cycles * blocksPerCycle;

  const genericHeader = buildGenericHeader(totalBlocks, epochMs);
  const blocks = [];
  const baseDate = new Date(epochMs);

  let blockNo = 1;

  for (let i = 1; i <= cycles; i++) {
    const simMs = (i - 1) * intervalSec * 1000;
    const simDate = new Date(baseDate.getTime() + simMs);

    for (let page = 0; page < blocksPerCycle; page++) {
      let used = 0;
      const frames = [];

      const start = page * FRAMES_PER_BLOCK;
      const end = Math.min(start + FRAMES_PER_BLOCK, msgList.length);

      for (let k = start; k < end; k++) {
        const m = msgList[k];
        const payload = buildPayload(m, rnd);

        const frame = Buffer.alloc(8 + payload.length);
        frame.writeUInt32BE(m.id >>> 0, 0);

        frame.writeUInt32BE(fdDlcFromSize(payload.length) >>> 0, 4);

        payload.copy(frame, 8);

        if (used + frame.length > BLOCK_DATA_SIZE) break;
        frames.push(frame);
        used += frame.length;
      }

      const block = Buffer.alloc(BLOCK_HEADER_SIZE + BLOCK_DATA_SIZE, 0x00);

      block[0] = (blockNo >> 16) & 0xff;
      block[1] = (blockNo >> 8) & 0xff;
      block[2] = blockNo & 0xff;

      makeTimestampAbsBytesFromDate(simDate).copy(block, 0x03);

      makeTimestampRelBytesMs(simMs).copy(block, 0x0b);

      GPS_BYTES.copy(block, 0x13);

      block[0x1b] = (BLOCK_DATA_SIZE >> 8) & 0xff;
      block[0x1c] = BLOCK_DATA_SIZE & 0xff;

      let off = BLOCK_HEADER_SIZE;
      for (const f of frames) {
        f.copy(block, off);
        off += f.length;
        if (off >= block.length) break;
      }

      blocks.push(block);
      blockNo++;
    }
  }

  return Buffer.concat([genericHeader, ...blocks]);
}

exports.handler = async (event) => {
  console.log("### WORKER RUNNING ###");
  console.log("EVENT RAW:", JSON.stringify(event));

  if (!event || !Array.isArray(event.Records) || event.Records.length === 0) {
    console.log("No SQS records.");
    return;
  }

  for (const rec of event.Records) {
    let msg;
    try {
      msg = JSON.parse(rec.body);
    } catch {
      console.log("Skipping record: invalid JSON body");
      continue;
    }

    const epochMs  = Number(msg.epochMs || 0);
    const epochSec = Number(msg.epochSec || 0);

    if (!epochMs || !epochSec) {
      console.log("Skipping record: missing epochMs/epochSec");
      continue;
    }

    const intervalSec = Number(msg.intervalSec || 0);
    const durationSec = Number(msg.durationSec || 0);
    if (!intervalSec || !durationSec) {
      console.log("Skipping record: missing intervalSec/durationSec");
      continue;
    }

    const cycles = Math.floor(durationSec / intervalSec);
    if (cycles <= 0) {
      console.log("Skipping record: cycles <= 0");
      continue;
    }

    const vin = String(msg.vin || "").trim();
    if (!vin) {
      console.log("Skipping record: missing vin");
      continue;
    }

    const dbcFiles = resolveDbcFiles(msg);
    if (!dbcFiles.length) {
      console.log("Skipping record: missing dbcFiles");
      continue;
    }

    const whitelist = resolveCanWhitelist(msg);
    if (!whitelist.size) {
      console.log("Skipping record: missing canFrames");
      continue;
    }

    const bucketName = resolveS3Bucket(msg);
    if (!bucketName) {
      console.log("Skipping record: missing s3Bucket/bucket in payload");
      continue;
    }

    const { msgList } = loadSpec(dbcFiles, whitelist);

    const seed = (epochMs ^ hashStringToU32(vin)) >>> 0;
    const bin = generateBin(msgList, cycles, seed, intervalSec, epochMs);

    let n = 1;
    while (true) {
      const key = `${vin}/can_data_${epochSec}_${n}.bin`;

      try {
        await putObjectNoOverwrite({
          Bucket: bucketName,
          Key: key,
          Body: bin,
          ContentType: "application/octet-stream"
        });

        console.log("PUT OK", { Bucket: bucketName, Key: key, bytes: bin.length });
        break;
      } catch (err) {
        if (isAlreadyExists(err)) { n++; continue; }
        throw err;
      }
    }
  }
};
