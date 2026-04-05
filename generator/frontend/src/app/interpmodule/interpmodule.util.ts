export function interpolateGpsPerBlock(
  hexInputCoord: string[],
  speed: number,
  unity: 'Km' | 'Mi',
  latency: number,
  numberOfBlocks: number
): string[] {
  if (!hexInputCoord || hexInputCoord.length < 2) {
    throw new Error('At least 2 coordinates are required');
  }

  if (!Number.isFinite(speed) || speed <= 0) {
    throw new Error('Speed must be greater than zero');
  }

  if (!Number.isFinite(latency) || latency <= 0) {
    throw new Error('Latency must be greater than zero');
  }

  if (!Number.isFinite(numberOfBlocks) || numberOfBlocks <= 0) {
    throw new Error('Number of blocks must be greater than zero');
  }

  const speedKmH = unity === 'Mi' ? speed * 1.60934 : speed;
  const distancePerBlockKm = speedKmH * (latency / 3600);

  const points = hexInputCoord.map(decodeHexGps);

  const segments: {
    start: { lat: number; lng: number };
    end: { lat: number; lng: number };
    distance: number;
    cumulativeStart: number;
    cumulativeEnd: number;
  }[] = [];

  let cumulative = 0;

  for (let i = 0; i < points.length - 1; i++) {
    const dist = haversine(points[i], points[i + 1]);

    segments.push({
      start: points[i],
      end: points[i + 1],
      distance: dist,
      cumulativeStart: cumulative,
      cumulativeEnd: cumulative + dist
    });

    cumulative += dist;
  }

  const totalDistance = cumulative;
  const result: string[] = [];

  for (let block = 0; block < numberOfBlocks; block++) {
    const targetDistance = block * distancePerBlockKm;

    if (targetDistance >= totalDistance) {
      const last = points[points.length - 1];
      result.push(encodeHexGps(last.lat, last.lng));
      continue;
    }

    const segment = segments.find(
      (s) =>
        targetDistance >= s.cumulativeStart &&
        targetDistance <= s.cumulativeEnd
    );

    if (!segment) {
      const last = points[points.length - 1];
      result.push(encodeHexGps(last.lat, last.lng));
      continue;
    }

    const progress =
      segment.distance === 0
        ? 0
        : (targetDistance - segment.cumulativeStart) / segment.distance;

    const lat =
      segment.start.lat +
      (segment.end.lat - segment.start.lat) * progress;

    const lng =
      segment.start.lng +
      (segment.end.lng - segment.start.lng) * progress;

    result.push(encodeHexGps(lat, lng));
  }

  return result;
}

function decodeHexGps(hex: string): { lat: number; lng: number } {
  if (hex.length !== 16) {
    throw new Error('Invalid hex length. Expected 16 characters.');
  }

  const latHex = hex.substring(0, 8);
  const lngHex = hex.substring(8, 16);

  const latInt = signedInt32FromHex(latHex);
  const lngInt = signedInt32FromHex(lngHex);

  return {
    lat: latInt / 1_000_000,
    lng: lngInt / 1_000_000
  };
}

function encodeHexGps(lat: number, lng: number): string {
  const latInt = Math.round(lat * 1_000_000);
  const lngInt = Math.round(lng * 1_000_000);

  const latHex = int32ToHex(latInt);
  const lngHex = int32ToHex(lngInt);

  return (latHex + lngHex).toUpperCase();
}

function signedInt32FromHex(hex: string): number {
  const value = parseInt(hex, 16);
  return value > 0x7fffffff ? value - 0x100000000 : value;
}

function int32ToHex(value: number): string {
  const normalized = value < 0 ? value + 0x100000000 : value;
  return normalized.toString(16).padStart(8, '0');
}

function haversine(
  a: { lat: number; lng: number },
  b: { lat: number; lng: number }
): number {
  const R = 6371;

  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);

  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);

  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) *
      Math.cos(lat2) *
      Math.sin(dLng / 2) ** 2;

  return 2 * R * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
}

function toRad(deg: number): number {
  return deg * (Math.PI / 180);
}