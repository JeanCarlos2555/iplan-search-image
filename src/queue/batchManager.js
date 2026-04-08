const { createRedisConnection } = require("./config");
const crypto = require("crypto");

const redis = createRedisConnection();

const BATCH_TTL = parseInt(process.env.BATCH_TTL_SECONDS || "86400", 10); // 24h padrão

/**
 * Cria um novo lote e retorna o ID.
 * Armazena no Redis o estado do lote com todos os EANs.
 */
async function createBatch(eans) {
  const batchId = crypto.randomUUID();
  const key = `batch:${batchId}`;

  const batchData = {
    id: batchId,
    status: "processing",
    total: eans.length,
    completed: 0,
    failed: 0,
    eans: JSON.stringify(eans),
    results: JSON.stringify({ success: [], failed: [] }),
    createdAt: new Date().toISOString(),
    zipPath: "",
  };

  await redis.hset(key, batchData);
  await redis.expire(key, BATCH_TTL);

  return batchId;
}

/**
 * Retorna os dados do lote.
 */
async function getBatch(batchId) {
  const key = `batch:${batchId}`;
  const data = await redis.hgetall(key);

  if (!data || !data.id) return null;

  return {
    id: data.id,
    status: data.status,
    total: parseInt(data.total, 10),
    completed: parseInt(data.completed, 10),
    failed: parseInt(data.failed, 10),
    progress: Math.round(
      ((parseInt(data.completed, 10) + parseInt(data.failed, 10)) /
        parseInt(data.total, 10)) *
        100
    ),
    results: JSON.parse(data.results || "{}"),
    createdAt: data.createdAt,
    zipUrl: data.status === "done" && data.zipPath ? `/api/batch/${data.id}/download` : null,
  };
}

/**
 * Registra o resultado de um EAN processado (sucesso ou falha).
 * Quando todos os EANs são processados, marca o lote como pronto para ZIP.
 */
async function recordEanResult(batchId, { ean, success, source, url, reason }) {
  const key = `batch:${batchId}`;

  const resultsRaw = await redis.hget(key, "results");
  const results = JSON.parse(resultsRaw || '{"success":[],"failed":[]}');

  if (success) {
    results.success.push({ ean, source, url });
    await redis.hincrby(key, "completed", 1);
  } else {
    results.failed.push({ ean, reason });
    await redis.hincrby(key, "failed", 1);
  }

  await redis.hset(key, "results", JSON.stringify(results));

  // Verifica se todos foram processados
  const [total, completed, failed] = await Promise.all([
    redis.hget(key, "total"),
    redis.hget(key, "completed"),
    redis.hget(key, "failed"),
  ]);

  const processed = parseInt(completed, 10) + parseInt(failed, 10);
  if (processed >= parseInt(total, 10)) {
    return true; // sinaliza que o lote está completo
  }

  return false;
}

/**
 * Marca o lote como concluído e armazena o caminho do ZIP.
 */
async function completeBatch(batchId, zipPath) {
  const key = `batch:${batchId}`;
  await redis.hset(key, "status", "done", "zipPath", zipPath);
}

module.exports = {
  createBatch,
  getBatch,
  recordEanResult,
  completeBatch,
};
