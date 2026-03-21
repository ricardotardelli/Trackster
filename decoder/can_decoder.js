"use strict";

/*
=============================================================================
Trackster BIN Decoder → JSON output
=============================================================================
Usage:
  node decoder.js file.bin

Output:
  ./output/<same_name>.json
=============================================================================
*/

const fs = require("fs");
const path = require("path");

const GENERIC_HEADER_SIZE = 0x26; // 38 bytes
const BLOCK_HEADER_SIZE = 29;

const FD_PAYLOAD_SIZES = [
  0, 1, 2, 3, 4, 5, 6, 7, 8,
  12, 16, 20, 24, 32, 48, 64
];

function readUInt24BE(buf, offset) {
  return (buf[offset] << 16) | (buf[offset + 1] << 8) | buf[offset + 2];
}

function dlcToSize(dlc) {
  if (dlc <= 8) return dlc;
  return FD_PAYLOAD_SIZES[dlc] ?? 0;
}

function readTimestampAbs(buf) {
  return {
    sec: buf[0],
    min: buf[1],
    hour: buf[2],
    day: buf[3],
    month: buf[4],
    year: 2000 + buf[5]
  };
}

function readTimestampRel(buf) {
  return Number(buf.readBigUInt64BE(0));
}

function loadDbcFiles() {
  const dbcDir = path.join(__dirname, "dbc");
  if (!fs.existsSync(dbcDir)) return new Set();

  const files = fs.readdirSync(dbcDir).filter(f => f.endsWith(".dbc"));

  const ids = new Set();

  for (const file of files) {
    const content = fs.readFileSync(path.join(dbcDir, file), "utf8");
    const lines = content.split(/\r?\n/);

    for (const line of lines) {
      const m = line.match(/^BO_\s+(\d+)/);
      if (m) {
        ids.add(Number(m[1]));
      }
    }
  }

  return ids;
}

function decodeBin(filePath) {
  const buffer = fs.readFileSync(filePath);

  const header = buffer.slice(0, GENERIC_HEADER_SIZE);

  const numBlocks = readUInt24BE(header, 0x23);
  const epochMs = Number(header.readBigUInt64BE(0x06));

  let offset = GENERIC_HEADER_SIZE;

  const blocks = [];
  let blockDataSize = null;

  // =======================
  // NEW: validation state
  // =======================
  const dbcIds = loadDbcFiles();

  const validation = {
    totalFrames: 0,
    validFrames: 0,
    invalidFrames: 0,
    errors: {
      unknownCanId: 0
    },
    invalidIds: {}
  };

  for (let b = 0; b < numBlocks; b++) {
    const blockHeader = buffer.slice(offset, offset + BLOCK_HEADER_SIZE);
    offset += BLOCK_HEADER_SIZE;

    const blockNumber = readUInt24BE(blockHeader, 0);

    const absTime = readTimestampAbs(blockHeader.slice(0x03, 0x0B));
    const relTime = readTimestampRel(blockHeader.slice(0x0B, 0x13));
    const gps = blockHeader.slice(0x13, 0x1B).toString("hex");

    const dataSize = (blockHeader[0x1B] << 8) | blockHeader[0x1C];
    if (blockDataSize === null) {
      blockDataSize = dataSize;
    }

    const data = buffer.slice(offset, offset + dataSize);
    offset += dataSize;

    const frames = [];
    let pos = 0;

    while (pos < data.length) {
      if (pos + 8 > data.length) break;

      const canId = data.readUInt32BE(pos);
      const dlc = data.readUInt32BE(pos + 4);

      const payloadSize = dlcToSize(dlc);

      if (payloadSize === 0 || pos + 8 + payloadSize > data.length) break;

      const payload = data.slice(pos + 8, pos + 8 + payloadSize);

      validation.totalFrames++;

      if (!dbcIds.has(canId)) {
        validation.invalidFrames++;
        validation.errors.unknownCanId++;

        const idHex = "0x" + canId.toString(16).toUpperCase();

        if (!validation.invalidIds[idHex]) {
          validation.invalidIds[idHex] = 0;
        }

        validation.invalidIds[idHex]++;
      } else {
        validation.validFrames++;
      }

      frames.push({
        id: "0x" + canId.toString(16).toUpperCase(),
        dlc,
        payloadHex: payload.toString("hex")
      });

      pos += 8 + payloadSize;
    }

    blocks.push({
      blockNumber,
      absTime,
      relTimeMs: relTime,
      gps,
      frameCount: frames.length,
      frames
    });
  }

  return {
    header: {
      numBlocks,
      epochMs,
      blockDataSize,
      dbcValidation: validation
    },
    blocks
  };
}

function writeOutputJson(inputPath, data) {
  const baseName = path.basename(inputPath, path.extname(inputPath));
  const outputDir = path.join(process.cwd(), "output");

  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  const outputFile = path.join(outputDir, `${baseName}.json`);

  fs.writeFileSync(outputFile, JSON.stringify(data, null, 2));

  console.log(`\nJSON file generated at: ${outputFile}`);
}

if (require.main === module) {
  const file = process.argv[2];

  if (!file) {
    console.error("Usage: node decoder.js <file.bin>");
    process.exit(1);
  }

  const result = decodeBin(file);

  console.log("Summary:");
  console.log("===============");
  console.log("Blocks:", result.header.numBlocks);
  console.log("CAN frames:", result.blocks[0]?.frameCount);
  console.log("Block Data Size:", result.header.blockDataSize);
  writeOutputJson(file, result);
  console.log(" ");
}