export interface ParsedTracksterBinHeaderField {
  offset: number;
  size: number;
  name: string;
  type: string;
  value: string | number;
}

export interface ParsedTracksterBinBlockField {
  offset: number;
  size: number;
  name: string;
  type: string;
  value: string | number;
}

export interface ParsedTracksterBinFrameField {
  offset: number;
  size: number;
  name: string;
  type: string;
  value: string | number;
}

export interface ParsedTracksterBinSignal {
  name: string;
  raw: string;
  value: string;
  unit: string;
  startBit: number;
  bitLength: number;
  byteOrder: 'little' | 'big';
  isSigned: boolean;
}

export interface ParsedTracksterBinFrame {
  offset: number;
  rawFrameBytes: string;

  canId: number;
  canIdHex: string;
  timestampDeltaNs: number;
  bus: number;
  dlcCode: number;
  payloadLength: number;
  flags: number;

  isCanFd: boolean;
  isExtendedId: boolean;

  payloadOffset: number;
  payloadBytes: string;
  payloadRaw: Uint8Array;

  recordSizeBytes: number;

  messageName: string;
  decodedSignals: number;
  signals: ParsedTracksterBinSignal[];

  fields: ParsedTracksterBinFrameField[];
}

export interface ParsedTracksterBinBlock {
  offset: number;
  magic: string;
  blockIndex: number;
  timestampNs: string;
  frameCount: number;
  payloadBytes: number;
  blockSizeBytes: number;
  reserved: number;

  firstFrameOffset: number;
  firstFrame?: ParsedTracksterBinFrame;
  frames: ParsedTracksterBinFrame[];

  fields: ParsedTracksterBinBlockField[];
}

export interface ParsedTracksterBin {
  magic: string;

  version: number;
  versionMajor: number;
  versionMinor: number;
  patchVersion: number;

  totalBytes: number;

  headerBytes: number;
  blockHeaderBytes: number;
  frameFixedHeaderBytes: number;
  reservedHeader14: number;

  blockCount: number;
  totalFrameCount: number;
  createdAtMs: string;
  totalPayloadBytes: number;
  reservedHeader36: number;

  isValidTracksterBin: boolean;

  headerFields: ParsedTracksterBinHeaderField[];
  blocks: ParsedTracksterBinBlock[];
}

interface RuntimeMessage {
  canId: number;
  name: string;
  dlc: number;
  bus: number;
  signals: RuntimeSignal[];
}

interface RuntimeSignal {
  name: string;
  startBit: number;
  bitLength: number;
  byteOrder: 'little' | 'big';
  isSigned: boolean;
  factor: number;
  offset: number;
  unit: string;
  min?: unknown;
  max?: unknown;
  muxType: string | null;
  muxValue: number | null;
}

const TRACKSTER_MAGIC = 'TRKS';

const FRAME_FLAG_CAN_FD = 0x01;
const FRAME_FLAG_EXTENDED_ID = 0x02;

const CAN_FD_DLC_TO_BYTES: Record<number, number> = {
  9: 12,
  10: 16,
  11: 20,
  12: 24,
  13: 32,
  14: 48,
  15: 64
};

export function parseTracksterBin(
  buffer: ArrayBuffer,
  runtime?: unknown
): ParsedTracksterBin {
  if (buffer.byteLength < 40) {
    throw new Error(
      `Invalid Trackster BIN. Expected at least 40 bytes, received ${buffer.byteLength}.`
    );
  }

  const runtimeIndex = buildRuntimeIndex(runtime);

  const view = new DataView(buffer);

  const magic = readAscii(view, 0, 4);

  const formatVersion = view.getUint16(4, true);
  const versionMajor = view.getUint8(6);
  const versionMinor = view.getUint8(7);

  const headerBytes = view.getUint16(8, true);
  const blockHeaderBytes = view.getUint16(10, true);
  const frameFixedHeaderBytes = view.getUint16(12, true);
  const reservedHeader14 = view.getUint16(14, true);

  const blockCount = view.getUint32(16, true);
  const totalFrameCount = view.getUint32(20, true);
  const createdAtMs = readUint64AsString(view, 24);
  const totalPayloadBytes = view.getUint32(32, true);
  const reservedHeader36 = view.getUint32(36, true);

  if (buffer.byteLength < headerBytes) {
    throw new Error(
      `Invalid Trackster BIN. Header declares ${headerBytes} bytes, but file has only ${buffer.byteLength} bytes.`
    );
  }

  const headerFields = parseTracksterHeader(view);

  const blocks = parseTracksterBlocks(
    view,
    headerBytes,
    blockHeaderBytes,
    frameFixedHeaderBytes,
    blockCount,
    runtimeIndex
  );

  return {
    magic,

    version: formatVersion,
    versionMajor,
    versionMinor,
    patchVersion: versionMinor,

    totalBytes: buffer.byteLength,

    headerBytes,
    blockHeaderBytes,
    frameFixedHeaderBytes,
    reservedHeader14,

    blockCount,
    totalFrameCount,
    createdAtMs,
    totalPayloadBytes,
    reservedHeader36,

    isValidTracksterBin: magic === TRACKSTER_MAGIC,

    headerFields,
    blocks
  };
}

function parseTracksterHeader(
  view: DataView
): ParsedTracksterBinHeaderField[] {
  return [
    {
      offset: 0,
      size: 4,
      name: 'magic',
      type: 'ascii',
      value: readAscii(view, 0, 4)
    },
    {
      offset: 4,
      size: 2,
      name: 'formatVersion',
      type: 'uint16_le',
      value: view.getUint16(4, true)
    },
    {
      offset: 6,
      size: 1,
      name: 'versionMajor',
      type: 'uint8',
      value: view.getUint8(6)
    },
    {
      offset: 7,
      size: 1,
      name: 'versionMinor',
      type: 'uint8',
      value: view.getUint8(7)
    },
    {
      offset: 8,
      size: 2,
      name: 'headerBytes',
      type: 'uint16_le',
      value: view.getUint16(8, true)
    },
    {
      offset: 10,
      size: 2,
      name: 'blockHeaderBytes',
      type: 'uint16_le',
      value: view.getUint16(10, true)
    },
    {
      offset: 12,
      size: 2,
      name: 'frameFixedHeaderBytes',
      type: 'uint16_le',
      value: view.getUint16(12, true)
    },
    {
      offset: 14,
      size: 2,
      name: 'reservedHeader14',
      type: 'uint16_le',
      value: view.getUint16(14, true)
    },
    {
      offset: 16,
      size: 4,
      name: 'blockCount',
      type: 'uint32_le',
      value: view.getUint32(16, true)
    },
    {
      offset: 20,
      size: 4,
      name: 'totalFrameCount',
      type: 'uint32_le',
      value: view.getUint32(20, true)
    },
    {
      offset: 24,
      size: 8,
      name: 'createdAtMs',
      type: 'uint64_le',
      value: readUint64AsString(view, 24)
    },
    {
      offset: 32,
      size: 4,
      name: 'totalPayloadBytes',
      type: 'uint32_le',
      value: view.getUint32(32, true)
    },
    {
      offset: 36,
      size: 4,
      name: 'reservedHeader36',
      type: 'uint32_le',
      value: view.getUint32(36, true)
    }
  ];
}

function parseTracksterBlocks(
  view: DataView,
  headerBytes: number,
  blockHeaderBytes: number,
  frameFixedHeaderBytes: number,
  blockCount: number,
  runtimeIndex: Map<number, RuntimeMessage>
): ParsedTracksterBinBlock[] {
  const blocks: ParsedTracksterBinBlock[] = [];
  let offset = headerBytes;

  for (let blockIndex = 0; blockIndex < blockCount; blockIndex += 1) {
    if (offset + blockHeaderBytes > view.byteLength) {
      break;
    }

    const block = parseTracksterBlock(
      view,
      offset,
      blockHeaderBytes,
      frameFixedHeaderBytes,
      runtimeIndex
    );

    blocks.push(block);

    if (block.blockSizeBytes <= 0) {
      break;
    }

    offset += block.blockSizeBytes;
  }

  return blocks;
}

function parseTracksterBlock(
  view: DataView,
  offset: number,
  blockHeaderBytes: number,
  frameFixedHeaderBytes: number,
  runtimeIndex: Map<number, RuntimeMessage>
): ParsedTracksterBinBlock {
  const magic = readAscii(view, offset, 4);

  const blockIndex = view.getUint32(offset + 4, true);
  const timestampNs = readUint64AsString(view, offset + 8);
  const frameCount = view.getUint32(offset + 16, true);
  const payloadBytes = view.getUint32(offset + 20, true);
  const blockSizeBytes = view.getUint32(offset + 24, true);
  const reserved = view.getUint32(offset + 28, true);

  const firstFrameOffset = offset + blockHeaderBytes;

  const frames = parseTracksterFrames(
    view,
    firstFrameOffset,
    frameCount,
    payloadBytes,
    frameFixedHeaderBytes,
    runtimeIndex
  );

  const firstFrame = frames.length > 0 ? frames[0] : undefined;

  const fields: ParsedTracksterBinBlockField[] = [
    {
      offset,
      size: 4,
      name: 'blockMagic',
      type: 'ascii',
      value: magic
    },
    {
      offset: offset + 4,
      size: 4,
      name: 'blockIndex',
      type: 'uint32_le',
      value: blockIndex
    },
    {
      offset: offset + 8,
      size: 8,
      name: 'timestampNs',
      type: 'uint64_le',
      value: timestampNs
    },
    {
      offset: offset + 16,
      size: 4,
      name: 'frameCount',
      type: 'uint32_le',
      value: frameCount
    },
    {
      offset: offset + 20,
      size: 4,
      name: 'payloadBytes',
      type: 'uint32_le',
      value: payloadBytes
    },
    {
      offset: offset + 24,
      size: 4,
      name: 'blockSizeBytes',
      type: 'uint32_le',
      value: blockSizeBytes
    },
    {
      offset: offset + 28,
      size: 4,
      name: 'reserved',
      type: 'uint32_le',
      value: reserved
    }
  ];

  return {
    offset,
    magic,
    blockIndex,
    timestampNs,
    frameCount,
    payloadBytes,
    blockSizeBytes,
    reserved,

    firstFrameOffset,
    firstFrame,
    frames,

    fields
  };
}

function parseTracksterFrames(
  view: DataView,
  startOffset: number,
  frameCount: number,
  payloadBytes: number,
  frameFixedHeaderBytes: number,
  runtimeIndex: Map<number, RuntimeMessage>
): ParsedTracksterBinFrame[] {
  const frames: ParsedTracksterBinFrame[] = [];

  let offset = startOffset;
  const endOffset = startOffset + payloadBytes;

  for (let frameIndex = 0; frameIndex < frameCount; frameIndex += 1) {
    if (offset + frameFixedHeaderBytes > view.byteLength) {
      break;
    }

    if (offset + frameFixedHeaderBytes > endOffset) {
      break;
    }

    const frame = parseTracksterFrame(
      view,
      offset,
      frameFixedHeaderBytes,
      runtimeIndex
    );

    frames.push(frame);

    if (frame.recordSizeBytes <= 0) {
      break;
    }

    offset += frame.recordSizeBytes;

    if (offset > endOffset) {
      break;
    }
  }

  return frames;
}

function parseTracksterFrame(
  view: DataView,
  offset: number,
  frameFixedHeaderBytes: number,
  runtimeIndex: Map<number, RuntimeMessage>
): ParsedTracksterBinFrame {
  const canId = view.getUint32(offset, true);
  const timestampDeltaNs = view.getUint32(offset + 4, true);
  const bus = view.getUint8(offset + 8);
  const dlcCode = view.getUint8(offset + 9);
  const flags = view.getUint8(offset + 10);

  const payloadLength = canDlcToPayloadLength(dlcCode);

  const payloadOffset = offset + frameFixedHeaderBytes;
  const payloadRaw = readRawBytes(view, payloadOffset, payloadLength);
  const payloadBytes = formatHexBytes(payloadRaw);

  const recordSizeBytes = frameFixedHeaderBytes + payloadLength;

  const rawFrameBytes = readHexBytes(
    view,
    offset,
    recordSizeBytes
  );

  const runtimeMessage = runtimeIndex.get(canId);
  const messageName = runtimeMessage?.name ?? `CAN_${toCanIdHex(canId)}`;
  const signals = runtimeMessage
    ? decodeSignals(payloadRaw, runtimeMessage)
    : [];

  const fields: ParsedTracksterBinFrameField[] = [
    {
      offset,
      size: 4,
      name: 'canId',
      type: 'uint32_le',
      value: canId
    },
    {
      offset: offset + 4,
      size: 4,
      name: 'timestampDeltaNs',
      type: 'uint32_le',
      value: timestampDeltaNs
    },
    {
      offset: offset + 8,
      size: 1,
      name: 'bus',
      type: 'uint8',
      value: bus
    },
    {
      offset: offset + 9,
      size: 1,
      name: 'dlcCode',
      type: 'uint8',
      value: dlcCode
    },
    {
      offset: offset + 10,
      size: 1,
      name: 'flags',
      type: 'uint8',
      value: flags
    },
    {
      offset: payloadOffset,
      size: payloadLength,
      name: 'payload',
      type: 'hex',
      value: payloadBytes
    }
  ];

  return {
    offset,
    rawFrameBytes,

    canId,
    canIdHex: toCanIdHex(canId),
    timestampDeltaNs,
    bus,
    dlcCode,
    payloadLength,
    flags,

    isCanFd: (flags & FRAME_FLAG_CAN_FD) !== 0,
    isExtendedId: (flags & FRAME_FLAG_EXTENDED_ID) !== 0,

    payloadOffset,
    payloadBytes,
    payloadRaw,

    recordSizeBytes,

    messageName,
    decodedSignals: signals.length,
    signals,

    fields
  };
}

function buildRuntimeIndex(runtime: unknown): Map<number, RuntimeMessage> {
  const messages = normalizeRuntimeMessages(runtime);
  const index = new Map<number, RuntimeMessage>();

  for (const message of messages) {
    index.set(message.canId, message);
  }

  return index;
}

function normalizeRuntimeMessages(runtime: unknown): RuntimeMessage[] {
  if (!runtime || typeof runtime !== 'object') {
    return [];
  }

  const value = runtime as any;
  const result: RuntimeMessage[] = [];

  if (value.dbc?.compiledDbc) {
    return normalizeRuntimeMessages(value.dbc.compiledDbc);
  }

  if (value.dbc?.resolvedCanFrames) {
    return normalizeRuntimeMessages({
      messages: value.dbc.resolvedCanFrames
    });
  }

  if (value.compiledDbc) {
    return normalizeRuntimeMessages(value.compiledDbc);
  }

  if (value.runtimeCompiledDbc) {
    return normalizeRuntimeMessages(value.runtimeCompiledDbc);
  }

  if (value.dbcRuntime) {
    return normalizeRuntimeMessages(value.dbcRuntime);
  }

  if (value.runtime) {
    return normalizeRuntimeMessages(value.runtime);
  }

  if (value.m && typeof value.m === 'object' && !Array.isArray(value.m)) {
    for (const [canIdKey, entries] of Object.entries(value.m)) {
      const list = Array.isArray(entries) ? entries : [entries];

      for (const entry of list) {
        const normalized = normalizeRuntimeMessage(canIdKey, entry);

        if (normalized) {
          result.push(normalized);
        }
      }
    }

    return result;
  }

  if (Array.isArray(value.messages)) {
    for (const entry of value.messages) {
      const normalized = normalizeRuntimeMessage(
        entry?.canId ?? entry?.id,
        entry
      );

      if (normalized) {
        result.push(normalized);
      }
    }

    return result;
  }

  if (Array.isArray(value.m)) {
    for (const entry of value.m) {
      const normalized = normalizeRuntimeMessage(
        entry?.canId ?? entry?.id,
        entry
      );

      if (normalized) {
        result.push(normalized);
      }
    }

    return result;
  }

  return [];
}

function normalizeRuntimeMessage(
  canIdKey: unknown,
  entry: unknown
): RuntimeMessage | null {
  if (!entry || typeof entry !== 'object') {
    return null;
  }

  const data = entry as any;
  const frame = data.frame || data;

  const canId = parseCanId(
    data.canId ??
    frame.canId ??
    frame.id ??
    frame.address ??
    canIdKey
  );

  if (!Number.isInteger(canId) || canId < 0) {
    return null;
  }

  const name = String(
    data.messageName ||
    frame.messageName ||
    frame.name ||
    frame.n ||
    `CAN_${toCanIdHex(canId)}`
  );

  const dlc = Number(frame.dlc ?? frame.length ?? frame.l ?? 8);
  const bus = Number(frame.bus ?? frame.src ?? 0);

  const rawSignals =
    frame.signals ||
    frame.s ||
    frame.signalList ||
    [];

  const signals = Array.isArray(rawSignals)
    ? rawSignals
        .map((signal, index) => normalizeRuntimeSignal(signal, index))
        .filter((signal): signal is RuntimeSignal => !!signal)
    : [];

  return {
    canId,
    name,
    dlc,
    bus,
    signals
  };
}

function normalizeRuntimeSignal(
  raw: unknown,
  index: number
): RuntimeSignal | null {
  if (!raw) {
    return null;
  }

  if (Array.isArray(raw)) {
    return {
      name: raw[8] ? String(raw[8]) : `signal_${index}`,
      startBit: Number(raw[0]),
      bitLength: Number(raw[1]),
      byteOrder: Number(raw[2]) === 0 ? 'big' : 'little',
      isSigned: Boolean(raw[3]),
      factor: Number(raw[4] ?? 1),
      offset: Number(raw[5] ?? 0),
      min: raw[6],
      max: raw[7],
      unit: raw[11] ? String(raw[11]) : '',
      muxType: normalizeMuxType(raw[9]),
      muxValue: normalizeMuxValue(raw[10])
    };
  }

  if (typeof raw !== 'object') {
    return null;
  }

  const data = raw as any;

  return {
    name: data.name ? String(data.name) : `signal_${index}`,
    startBit: Number(data.startBit ?? data.sb),
    bitLength: Number(data.bitLength ?? data.sizeBits ?? data.bl),
    byteOrder: normalizeByteOrder(data.byteOrder ?? data.endianness ?? data.bo),
    isSigned: Boolean(data.isSigned ?? data.signed ?? data.sg),
    factor: Number(data.factor ?? data.f ?? 1),
    offset: Number(data.offset ?? data.o ?? 0),
    min: data.min ?? data.minRaw,
    max: data.max ?? data.maxRaw,
    unit: data.unit ? String(data.unit) : '',
    muxType: normalizeMuxType(data.mx ?? data.muxType),
    muxValue: normalizeMuxValue(data.mv ?? data.muxValue)
  };
}

function decodeSignals(
  payload: Uint8Array,
  message: RuntimeMessage
): ParsedTracksterBinSignal[] {
  const result: ParsedTracksterBinSignal[] = [];

  const muxSignal = message.signals.find(signal => signal.muxType === 'multiplexor');
  let muxRawValue: bigint | null = null;

  if (muxSignal) {
    muxRawValue = readSignalRaw(payload, muxSignal);
  }

  for (const signal of message.signals) {
    if (!isSignalUsable(signal, payload)) {
      continue;
    }

    if (signal.muxType === 'multiplexed') {
      if (!muxSignal || muxRawValue === null) {
        continue;
      }

      if (signal.muxValue === null || signal.muxValue === undefined) {
        continue;
      }

      if (BigInt(signal.muxValue) !== muxRawValue) {
        continue;
      }
    }

    const rawValue = readSignalRaw(payload, signal);
    const signedRawValue = applySignedValue(rawValue, signal);
    const physicalValue =
      Number(signedRawValue) * signal.factor + signal.offset;

    result.push({
      name: signal.name,
      raw: signedRawValue.toString(),
      value: formatPhysicalValue(physicalValue),
      unit: signal.unit,
      startBit: signal.startBit,
      bitLength: signal.bitLength,
      byteOrder: signal.byteOrder,
      isSigned: signal.isSigned
    });
  }

  return result;
}

function isSignalUsable(
  signal: RuntimeSignal,
  payload: Uint8Array
): boolean {
  if (!Number.isInteger(signal.startBit) || signal.startBit < 0) {
    return false;
  }

  if (!Number.isInteger(signal.bitLength) || signal.bitLength <= 0 || signal.bitLength > 64) {
    return false;
  }

  const payloadBits = payload.length * 8;

  if (signal.startBit >= payloadBits) {
    return false;
  }

  if (signal.byteOrder === 'little') {
    return signal.startBit + signal.bitLength <= payloadBits;
  }

  const positions = getMotorolaBitPositions(signal.startBit, signal.bitLength);

  return positions.every(bit => bit >= 0 && bit < payloadBits);
}

function readSignalRaw(
  payload: Uint8Array,
  signal: RuntimeSignal
): bigint {
  if (signal.byteOrder === 'big') {
    return readMotorolaRaw(payload, signal.startBit, signal.bitLength);
  }

  return readIntelRaw(payload, signal.startBit, signal.bitLength);
}

function readIntelRaw(
  payload: Uint8Array,
  startBit: number,
  bitLength: number
): bigint {
  let value = 0n;

  for (let index = 0; index < bitLength; index += 1) {
    const absoluteBit = startBit + index;
    const bit = getPayloadBit(payload, absoluteBit);

    if (bit) {
      value |= 1n << BigInt(index);
    }
  }

  return value;
}

function readMotorolaRaw(
  payload: Uint8Array,
  startBit: number,
  bitLength: number
): bigint {
  let value = 0n;
  const positions = getMotorolaBitPositions(startBit, bitLength);

  for (let index = 0; index < positions.length; index += 1) {
    const bit = getPayloadBit(payload, positions[index]);

    if (bit) {
      value |= 1n << BigInt(bitLength - 1 - index);
    }
  }

  return value;
}

function getPayloadBit(
  payload: Uint8Array,
  absoluteBit: number
): number {
  const byteIndex = Math.floor(absoluteBit / 8);
  const bitIndex = absoluteBit % 8;

  if (byteIndex < 0 || byteIndex >= payload.length) {
    return 0;
  }

  return (payload[byteIndex] >> bitIndex) & 1;
}

function getMotorolaBitPositions(
  startBit: number,
  bitLength: number
): number[] {
  const positions: number[] = [];
  let bit = Number(startBit);

  for (let index = 0; index < bitLength; index += 1) {
    positions.push(bit);

    if (bit % 8 === 0) {
      bit += 15;
    } else {
      bit -= 1;
    }
  }

  return positions;
}

function applySignedValue(
  rawValue: bigint,
  signal: RuntimeSignal
): bigint {
  if (!signal.isSigned) {
    return rawValue;
  }

  const signBit = 1n << BigInt(signal.bitLength - 1);

  if ((rawValue & signBit) === 0n) {
    return rawValue;
  }

  return rawValue - (1n << BigInt(signal.bitLength));
}

function formatPhysicalValue(value: number): string {
  if (!Number.isFinite(value)) {
    return '';
  }

  if (Number.isInteger(value)) {
    return String(value);
  }

  return value.toFixed(6).replace(/\.?0+$/, '');
}

function normalizeMuxType(value: unknown): string | null {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const numeric = Number(value);

  if (Number.isFinite(numeric)) {
    if (numeric === 1) return 'multiplexor';
    if (numeric === 0) return 'multiplexed';
  }

  const text = String(value).trim().toLowerCase();

  if (text === 'm' || text === 'mux' || text === 'multiplexor' || text === 'multiplexer') {
    return 'multiplexor';
  }

  if (text.startsWith('m') && text.length > 1) {
    return 'multiplexed';
  }

  if (text === 'multiplexed') {
    return 'multiplexed';
  }

  return null;
}

function normalizeMuxValue(value: unknown): number | null {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const numeric = Number(value);

  if (Number.isFinite(numeric)) {
    return numeric;
  }

  const text = String(value).trim().toLowerCase();

  if (/^m\d+$/.test(text)) {
    return Number(text.slice(1));
  }

  return null;
}

function normalizeByteOrder(value: unknown): 'little' | 'big' {
  if (value === 0) return 'big';
  if (value === 1) return 'little';

  const text = String(value || '').toLowerCase();

  if (text.includes('big') || text.includes('motorola')) {
    return 'big';
  }

  return 'little';
}

function parseCanId(value: unknown): number {
  if (typeof value === 'number') {
    return value;
  }

  const text = String(value || '').trim();

  if (text.startsWith('0x') || text.startsWith('0X')) {
    return parseInt(text, 16);
  }

  return parseInt(text, 10);
}

function readAscii(
  view: DataView,
  offset: number,
  length: number
): string {
  let value = '';

  for (let index = 0; index < length; index += 1) {
    value += String.fromCharCode(view.getUint8(offset + index));
  }

  return value;
}

function readUint64AsString(
  view: DataView,
  offset: number
): string {
  return view.getBigUint64(offset, true).toString();
}

function readRawBytes(
  view: DataView,
  offset: number,
  length: number
): Uint8Array {
  const end = Math.min(offset + length, view.byteLength);
  const bytes = new Uint8Array(end - offset);

  for (let index = offset; index < end; index += 1) {
    bytes[index - offset] = view.getUint8(index);
  }

  return bytes;
}

function readHexBytes(
  view: DataView,
  offset: number,
  length: number
): string {
  return formatHexBytes(
    readRawBytes(view, offset, length)
  );
}

function formatHexBytes(bytes: Uint8Array): string {
  return Array.from(bytes)
    .map(byte => byte.toString(16).padStart(2, '0').toUpperCase())
    .join(' ');
}

function canDlcToPayloadLength(dlc: number): number {
  if (!Number.isInteger(dlc) || dlc < 0 || dlc > 15) {
    return 0;
  }

  if (dlc <= 8) {
    return dlc;
  }

  return CAN_FD_DLC_TO_BYTES[dlc] ?? 0;
}

function toCanIdHex(canId: number): string {
  const width = canId > 0x7ff ? 8 : 3;

  return `0x${canId.toString(16).toUpperCase().padStart(width, '0')}`;
}