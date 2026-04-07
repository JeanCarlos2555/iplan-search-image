const IORedis = require("ioredis");

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

/**
 * Cria conexão Redis reutilizável para BullMQ.
 */
function createRedisConnection() {
  return new IORedis(REDIS_URL, {
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
  });
}

const QUEUE_NAMES = {
  SEARCH: "ean-search",
  CROP: "image-crop",
};

module.exports = { createRedisConnection, QUEUE_NAMES, REDIS_URL };
