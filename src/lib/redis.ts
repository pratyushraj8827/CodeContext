import { Redis } from "@upstash/redis";

let redis: Redis | null = null;

if (
  process.env.UPSTASH_REDIS_REST_URL &&
  process.env.UPSTASH_REDIS_REST_TOKEN
) {
  try {
    redis = new Redis({
      url: process.env.UPSTASH_REDIS_REST_URL,
      token: process.env.UPSTASH_REDIS_REST_TOKEN,
    });
  } catch (error) {
    console.error("[Redis] Failed to initialize Upstash Redis client:", error);
    redis = null;
  }
}

export { redis };

export function getRedisClient(): Redis | null {
  return redis;
}

export function isRedisAvailable(): boolean {
  return redis !== null;
}

// In-memory fallback lock store for development or environments without Upstash Redis
interface MemoryLock {
  lockId: string;
  expiresAt: number;
}

const memoryLockStore = new Map<string, MemoryLock>();

function cleanupExpiredMemoryLocks() {
  const now = Date.now();
  for (const [key, lock] of memoryLockStore.entries()) {
    if (now > lock.expiresAt) {
      memoryLockStore.delete(key);
    }
  }
}

/**
 * Acquire an atomic lock for a given resource key.
 *
 * @param key Unique key for the lock (e.g. `lock:indexing:project_id`)
 * @param ttlSeconds Lock expiration time in seconds (default: 60)
 * @param customLockId Optional identifier; generates a UUID if not provided
 * @returns Object with `acquired: boolean` and `lockId: string` if successful, or null if lock not acquired
 */
export async function acquireLock(
  key: string,
  ttlSeconds: number = 60,
  customLockId?: string
): Promise<{ acquired: boolean; lockId: string } | null> {
  const lockId = customLockId || (typeof crypto !== "undefined" && crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).substring(2));
  const lockKey = key.startsWith("lock:") ? key : `lock:${key}`;

  if (redis) {
    try {
      // Redis SET key value NX EX ttlSeconds
      const result = await redis.set(lockKey, lockId, {
        nx: true,
        ex: ttlSeconds,
      });

      if (result === "OK" || result === true) {
        return { acquired: true, lockId };
      }
      return null;
    } catch (error) {
      console.warn("[Redis] Failed to acquire distributed lock via Redis, attempting in-memory fallback:", error);
    }
  }

  // In-memory fallback lock
  cleanupExpiredMemoryLocks();
  const existing = memoryLockStore.get(lockKey);
  const now = Date.now();

  if (existing && now < existing.expiresAt) {
    return null;
  }

  memoryLockStore.set(lockKey, {
    lockId,
    expiresAt: now + ttlSeconds * 1000,
  });

  return { acquired: true, lockId };
}

/**
 * Release an atomic lock safely (ensures only the lock holder can release it).
 *
 * @param key Unique key for the lock
 * @param lockId The lock identifier returned when acquiring the lock
 * @returns boolean indicating if release succeeded
 */
export async function releaseLock(
  key: string,
  lockId: string
): Promise<boolean> {
  const lockKey = key.startsWith("lock:") ? key : `lock:${key}`;

  if (redis) {
    try {
      // Lua script to atomically compare and release lock
      const luaScript = `
        if redis.call("get", KEYS[1]) == ARGV[1] then
          return redis.call("del", KEYS[1])
        else
          return 0
        end
      `;
      const result = await redis.eval(luaScript, [lockKey], [lockId]);
      return result === 1;
    } catch (error) {
      console.warn("[Redis] Failed to release lock via Redis Lua script:", error);
      // Fallback check
      try {
        const val = await redis.get<string>(lockKey);
        if (val === lockId) {
          await redis.del(lockKey);
          return true;
        }
      } catch {
        // ignore
      }
    }
  }

  // In-memory fallback release
  const existing = memoryLockStore.get(lockKey);
  if (existing && existing.lockId === lockId) {
    memoryLockStore.delete(lockKey);
    return true;
  }

  return false;
}

/**
 * Extend an existing lock's TTL if currently owned by `lockId`.
 *
 * @param key Unique key for the lock
 * @param lockId The lock identifier
 * @param extensionSeconds Additional seconds to extend the lock
 */
export async function extendLock(
  key: string,
  lockId: string,
  extensionSeconds: number = 60
): Promise<boolean> {
  const lockKey = key.startsWith("lock:") ? key : `lock:${key}`;

  if (redis) {
    try {
      const luaScript = `
        if redis.call("get", KEYS[1]) == ARGV[1] then
          return redis.call("expire", KEYS[1], ARGV[2])
        else
          return 0
        end
      `;
      const result = await redis.eval(luaScript, [lockKey], [lockId, extensionSeconds]);
      return result === 1;
    } catch (error) {
      console.warn("[Redis] Failed to extend lock in Redis:", error);
    }
  }

  // In-memory fallback
  const existing = memoryLockStore.get(lockKey);
  if (existing && existing.lockId === lockId) {
    existing.expiresAt = Date.now() + extensionSeconds * 1000;
    return true;
  }

  return false;
}

/**
 * Execute a critical asynchronous task protected by an atomic lock.
 *
 * @param key Unique key for the lock
 * @param fn Asynchronous function to execute under lock
 * @param ttlSeconds Lock expiration time in seconds (default: 60)
 */
export async function withLock<T>(
  key: string,
  fn: () => Promise<T>,
  ttlSeconds: number = 60
): Promise<T> {
  const lock = await acquireLock(key, ttlSeconds);
  if (!lock || !lock.acquired) {
    throw new Error(`Failed to acquire lock for key "${key}"`);
  }

  try {
    return await fn();
  } finally {
    await releaseLock(key, lock.lockId).catch((err) =>
      console.error(`[Redis] Error releasing lock "${key}":`, err)
    );
  }
}
