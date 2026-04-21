export type ValidationErrorType =
  | 'SYNTAX_ERROR'
  | 'OVERLAP_ERROR'
  | 'OUT_OF_BOUNDS'
  | 'DUPLICATE_ERROR'
  | 'INVALID_VALUE';

export type ValidationSeverity = 'error' | 'warning';

export interface ValidationError {
  line: number;
  column?: number;
  endColumn?: number;
  type: ValidationErrorType;
  severity: ValidationSeverity;
  message: string;
  messageCode?: string;
}

export interface DbcSignal {
  sourceLine: number;
  name: string;
  startBit: number;
  sizeBits: number;
  endianness: 'Little Endian' | 'Big Endian';
  isSigned: boolean;
  factor: number;
  offset: number;
  range: { min: number; max: number };
  unit: string;
  receivers: string[];
  bitRange: { lsb: number; msb: number };
  rawBitPositions: number[];
  multiplex?: {
    isMultiplexor: boolean;
    selector?: string;
  };
}

export interface DbcMessage {
  sourceLine: number;
  id: number;
  hexId: string;
  name: string;
  sizeBytes: number;
  signals: DbcSignal[];
}

export interface DbcFullReport {
  isValid: boolean;
  errors: ValidationError[];
  stats: {
    messages: { total: number; valid: number; invalid: number };
    signals: { total: number; valid: number; invalid: number };
  };
  data: DbcMessage[];
}

export class DbcParser {
  private static readonly BO_RE = /^BO_\s+(\w+)\s+(\w+)\s*:\s*(\w+)\s+(\w+)$/;

  private static readonly SG_RE =
    /^SG_\s+(\w+)\s*(?:(M|m\d+)\s*)?(?::)\s*(\d+)\|(\d+)@(\d)([+-])\s+\(([0-9.+\-eE]+),([0-9.+\-eE]+)\)\s+\[([0-9.+\-eE]+)\|([0-9.+\-eE]+)\]\s+"(.*)"\s+(.*)$/;

  private static readonly BE_BITS = Array.from({ length: 64 }, (_, index) => {
    const byteIndex = Math.floor(index / 8);
    const bitInByte = 7 - (index % 8);
    return byteIndex * 8 + bitInByte;
  });

  public static parse(content: string): DbcFullReport {
    const lines = content.split(/\r?\n/);
    const errors: ValidationError[] = [];
    const messagesMap = new Map<number, DbcMessage>();

    const invalidMessageAddrs = new Set<number>();
    const invalidSignalKeys = new Set<string>();
    const allSignalKeys = new Set<string>();

    const messageNameToAddress = new Map<string, number>();

    let currentMsg: DbcMessage | null = null;

    lines.forEach((line, index) => {
      const trimmed = line.trim();
      const lineNum = index + 1;

      if (!trimmed || this.isIgnorableLine(trimmed)) {
        return;
      }

      if (trimmed.startsWith('BO_ ')) {
        const match = trimmed.match(this.BO_RE);

        if (!match) {
          errors.push(
            this.buildError(
              lineNum,
              'SYNTAX_ERROR',
              `Invalid message syntax: ${trimmed}`,
              'INVALID_BO_SYNTAX'
            )
          );
          currentMsg = null;
          return;
        }

        const rawId = match[1];
        const name = match[2];
        const rawSize = match[3];

        const id = this.parseDbcInteger(rawId);
        const sizeBytes = this.parseDbcInteger(rawSize);

        let hasMessageError = false;

        if (id === null || id < 0) {
          errors.push(
            this.buildError(
              lineNum,
              'INVALID_VALUE',
              `Invalid message ID: ${rawId}`,
              'INVALID_MESSAGE_ID'
            )
          );
          hasMessageError = true;
        }

        if (sizeBytes === null || sizeBytes <= 0) {
          errors.push(
            this.buildError(
              lineNum,
              'INVALID_VALUE',
              `Invalid message size: ${rawSize}`,
              'INVALID_MESSAGE_SIZE'
            )
          );
          hasMessageError = true;
        }

        if (id !== null && messagesMap.has(id)) {
          errors.push(
            this.buildError(
              lineNum,
              'DUPLICATE_ERROR',
              `Duplicate message ID: ${id}`,
              'DUPLICATE_MESSAGE_ID'
            )
          );
          invalidMessageAddrs.add(id);
          hasMessageError = true;
        }

        if (messageNameToAddress.has(name)) {
          errors.push(
            this.buildError(
              lineNum,
              'DUPLICATE_ERROR',
              `Duplicate message name: ${name}`,
              'DUPLICATE_MESSAGE_NAME'
            )
          );
          hasMessageError = true;
        }

        if (id === null || sizeBytes === null || sizeBytes <= 0) {
          currentMsg = null;
          return;
        }

        currentMsg = {
          sourceLine: lineNum,
          id,
          hexId: `0x${id.toString(16).toUpperCase()}`,
          name,
          sizeBytes,
          signals: []
        };

        messagesMap.set(id, currentMsg);
        messageNameToAddress.set(name, id);

        if (hasMessageError) {
          invalidMessageAddrs.add(id);
        }

        return;
      }

      if (trimmed.startsWith('SG_ ')) {
        if (!currentMsg) {
          errors.push(
            this.buildError(
              lineNum,
              'SYNTAX_ERROR',
              'Signal defined without a corresponding BO_ message.',
              'ORPHAN_SIGNAL'
            )
          );
          return;
        }

        const match = trimmed.match(this.SG_RE);

        if (!match) {
          errors.push(
            this.buildError(
              lineNum,
              'SYNTAX_ERROR',
              `Invalid signal syntax: ${trimmed}`,
              'INVALID_SG_SYNTAX'
            )
          );
          invalidMessageAddrs.add(currentMsg.id);
          return;
        }

        const name = match[1];
        const multiplexToken = match[2] || '';
        const rawStartBit = match[3];
        const rawSizeBits = match[4];
        const rawEndian = match[5];
        const rawSign = match[6];
        const rawFactor = match[7];
        const rawOffset = match[8];
        const rawMin = match[9];
        const rawMax = match[10];
        const rawUnit = match[11] ?? '';
        const rawReceivers = match[12] ?? '';

        const startBit = Number.parseInt(rawStartBit, 10);
        const sizeBits = Number.parseInt(rawSizeBits, 10);
        const isLittleEndian = rawEndian === '1';
        const factor = Number.parseFloat(rawFactor);
        const offset = Number.parseFloat(rawOffset);
        const min = Number.parseFloat(rawMin);
        const max = Number.parseFloat(rawMax);

        const sigKey = `${currentMsg.id}:${name}`;
        allSignalKeys.add(sigKey);

        let hasSignalError = false;

        if (!Number.isInteger(startBit) || startBit < 0) {
          errors.push(
            this.buildError(
              lineNum,
              'INVALID_VALUE',
              `Invalid start bit for signal '${name}': ${rawStartBit}`,
              'INVALID_SIGNAL_START_BIT'
            )
          );
          hasSignalError = true;
        }

        if (!Number.isInteger(sizeBits) || sizeBits <= 0) {
          errors.push(
            this.buildError(
              lineNum,
              'INVALID_VALUE',
              `Invalid size for signal '${name}': ${rawSizeBits}`,
              'INVALID_SIGNAL_SIZE'
            )
          );
          hasSignalError = true;
        }

        if (!Number.isFinite(factor)) {
          errors.push(
            this.buildError(
              lineNum,
              'INVALID_VALUE',
              `Invalid factor for signal '${name}': ${rawFactor}`,
              'INVALID_SIGNAL_FACTOR'
            )
          );
          hasSignalError = true;
        }

        if (!Number.isFinite(offset)) {
          errors.push(
            this.buildError(
              lineNum,
              'INVALID_VALUE',
              `Invalid offset for signal '${name}': ${rawOffset}`,
              'INVALID_SIGNAL_OFFSET'
            )
          );
          hasSignalError = true;
        }

        if (!Number.isFinite(min) || !Number.isFinite(max)) {
          errors.push(
            this.buildError(
              lineNum,
              'INVALID_VALUE',
              `Invalid range for signal '${name}': [${rawMin}|${rawMax}]`,
              'INVALID_SIGNAL_RANGE'
            )
          );
          hasSignalError = true;
        }

        if (Number.isFinite(min) && Number.isFinite(max) && min > max) {
          errors.push(
            this.buildError(
              lineNum,
              'INVALID_VALUE',
              `Signal '${name}' has min greater than max.`,
              'INVALID_SIGNAL_MIN_MAX'
            )
          );
          hasSignalError = true;
        }

        if (currentMsg.signals.some(signal => signal.name === name)) {
          errors.push(
            this.buildError(
              lineNum,
              'DUPLICATE_ERROR',
              `Duplicate signal name '${name}' in message '${currentMsg.name}'.`,
              'DUPLICATE_SIGNAL_NAME'
            )
          );
          hasSignalError = true;
        }

        let bitPositions: number[] = [];
        let bitRange = { lsb: -1, msb: -1 };

        if (!hasSignalError) {
          bitPositions = this.calculateBitPositions(
            startBit,
            sizeBits,
            isLittleEndian
          );

          if (bitPositions.length !== sizeBits) {
            errors.push(
              this.buildError(
                lineNum,
                'OUT_OF_BOUNDS',
                `Signal '${name}' exceeds the supported bit mapping range.`,
                'SIGNAL_MAPPING_OUT_OF_RANGE'
              )
            );
            hasSignalError = true;
          } else {
            bitRange = {
              lsb: Math.min(...bitPositions),
              msb: Math.max(...bitPositions)
            };

            const messageMaxBit = currentMsg.sizeBytes * 8 - 1;

            if (bitPositions.some(bit => bit < 0 || bit > messageMaxBit)) {
              errors.push(
                this.buildError(
                  lineNum,
                  'OUT_OF_BOUNDS',
                  `Signal '${name}' exceeds the ${currentMsg.sizeBytes} bytes of message '${currentMsg.name}'.`,
                  'SIGNAL_EXCEEDS_MESSAGE_SIZE'
                )
              );
              hasSignalError = true;
            }
          }
        }

        if (!hasSignalError) {
          for (const existingSignal of currentMsg.signals) {
            if (
              this.hasBitOverlap(
                bitPositions,
                existingSignal.rawBitPositions
              )
            ) {
              errors.push(
                this.buildError(
                  lineNum,
                  'OVERLAP_ERROR',
                  `Signal '${name}' overlaps with '${existingSignal.name}'.`,
                  'SIGNAL_OVERLAP'
                )
              );
              hasSignalError = true;
              break;
            }
          }
        }

        if (hasSignalError) {
          invalidSignalKeys.add(sigKey);
          invalidMessageAddrs.add(currentMsg.id);
        }

        currentMsg.signals.push({
          sourceLine: lineNum,
          name,
          startBit,
          sizeBits,
          endianness: isLittleEndian ? 'Little Endian' : 'Big Endian',
          isSigned: rawSign === '-',
          factor,
          offset,
          range: { min, max },
          unit: rawUnit,
          receivers: this.parseReceivers(rawReceivers),
          bitRange,
          rawBitPositions: bitPositions,
          multiplex: this.parseMultiplexToken(multiplexToken)
        });

        return;
      }
    });

    return {
      isValid: errors.length === 0,
      errors,
      stats: {
        messages: {
          total: messagesMap.size,
          invalid: invalidMessageAddrs.size,
          valid: messagesMap.size - invalidMessageAddrs.size
        },
        signals: {
          total: allSignalKeys.size,
          invalid: invalidSignalKeys.size,
          valid: allSignalKeys.size - invalidSignalKeys.size
        }
      },
      data: Array.from(messagesMap.values())
    };
  }

  private static buildError(
    line: number,
    type: ValidationErrorType,
    message: string,
    messageCode?: string,
    column = 1,
    endColumn = 999
  ): ValidationError {
    return {
      line,
      column,
      endColumn,
      type,
      severity: 'error',
      message,
      messageCode
    };
  }

  private static isIgnorableLine(line: string): boolean {
    return (
      line.startsWith('CM_ ') ||
      line.startsWith('BA_ ') ||
      line.startsWith('BA_DEF_ ') ||
      line.startsWith('BA_DEF_DEF_ ') ||
      line.startsWith('BU_:') ||
      line.startsWith('VAL_ ') ||
      line.startsWith('EV_ ') ||
      line.startsWith('NS_') ||
      line.startsWith('BS_:') ||
      line.startsWith('VERSION ')
    );
  }

  private static parseDbcInteger(value: string): number | null {
    if (/^0x/i.test(value)) {
      const parsedHex = Number.parseInt(value, 16);
      return Number.isNaN(parsedHex) ? null : parsedHex;
    }

    const parsedDec = Number.parseInt(value, 10);
    return Number.isNaN(parsedDec) ? null : parsedDec;
  }

  private static parseReceivers(receiversRaw: string): string[] {
    return receiversRaw
      .split(',')
      .map(receiver => receiver.trim())
      .filter(Boolean);
  }

  private static parseMultiplexToken(token: string): DbcSignal['multiplex'] {
    if (!token) {
      return undefined;
    }

    if (token === 'M') {
      return {
        isMultiplexor: true
      };
    }

    return {
      isMultiplexor: false,
      selector: token
    };
  }

  private static calculateBitPositions(
    startBit: number,
    sizeBits: number,
    isLittleEndian: boolean
  ): number[] {
    if (sizeBits <= 0 || startBit < 0) {
      return [];
    }

    if (isLittleEndian) {
      return Array.from({ length: sizeBits }, (_, index) => startBit + index);
    }

    const startIndex = this.BE_BITS.indexOf(startBit);

    if (startIndex === -1) {
      return [];
    }

    const selected = this.BE_BITS.slice(startIndex, startIndex + sizeBits);

    if (selected.length !== sizeBits) {
      return [];
    }

    return selected;
  }

  private static hasBitOverlap(bitsA: number[], bitsB: number[]): boolean {
    if (!bitsA.length || !bitsB.length) {
      return false;
    }

    const bitSet = new Set(bitsA);

    for (const bit of bitsB) {
      if (bitSet.has(bit)) {
        return true;
      }
    }

    return false;
  }
}