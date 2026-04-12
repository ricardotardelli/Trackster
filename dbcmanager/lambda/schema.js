schema = {
  "v": 1,
  "f": ["sb","bl","bo","sg","f","o","min","max"],
  "m": {
    "0x100": {
      "l": 8,
      "s": [
        [0,16,0,0,0.01,0,0,65535],
        [16,16,0,0,0.25,0,0,65535]
      ]
    }
  }
}

const SB  = 0; // startBit
const BL  = 1; // bitLength
const BO  = 2; // byteOrder (0=intel, 1=motorola)
const SG  = 3; // signed
const F   = 4; // factor
const O   = 5; // offset
const MIN = 6; // minRaw
const MAX = 7; // maxRaw

function normalizeSignals(msg) {
  return msg.s.map(s => ({
    sb: s[0],
    bl: s[1],
    endian: s[2] === 0,
    signed: s[3] === 1,
    factor: s[4],
    offset: s[5],
    min: s[6],
    max: s[7]
  }));
}