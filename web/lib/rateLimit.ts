// In-memory per-IP limit. Per serverless instance only, so it's a coarse guard;
// back it with Redis if you need a strict global limit.
type Bucket = { count: number; reset: number };

const buckets = new Map<string, Bucket>();
const WINDOW_MS = 60_000;
const MAX_PER_WINDOW = 5;

export function rateLimit(key: string): boolean {
  const now = Date.now();
  const bucket = buckets.get(key);

  if (!bucket || now > bucket.reset) {
    buckets.set(key, { count: 1, reset: now + WINDOW_MS });
    return true;
  }
  if (bucket.count >= MAX_PER_WINDOW) {
    return false;
  }
  bucket.count += 1;
  return true;
}
