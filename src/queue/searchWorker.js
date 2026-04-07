const { Worker, Queue } = require("bullmq");
const { createRedisConnection, QUEUE_NAMES } = require("./config");
const { searchImageByEan } = require("../imageSearch");
const {
  recordEanResult,
  storeImageBuffer,
} = require("./batchManager");

const connection = createRedisConnection();

// Fila de recorte - o searchWorker enfileira aqui após encontrar a imagem
const cropQueue = new Queue(QUEUE_NAMES.CROP, { connection: createRedisConnection() });

const SEARCH_CONCURRENCY = parseInt(process.env.SEARCH_CONCURRENCY || "5", 10);

/**
 * Worker da fila de busca.
 * Recebe { batchId, ean }, busca a imagem e enfileira na fila de recorte.
 */
const searchWorker = new Worker(
  QUEUE_NAMES.SEARCH,
  async (job) => {
    const { batchId, ean } = job.data;
    console.log(`[SearchWorker] Processando EAN ${ean} do lote ${batchId}`);

    try {
      const result = await searchImageByEan(ean);

      if (!result) {
        const batchDone = await recordEanResult(batchId, {
          ean,
          success: false,
          reason: "Imagem não encontrada",
        });

        if (batchDone) {
          // Enfileira job de finalização na fila de crop
          await cropQueue.add("finalize-batch", { batchId, action: "finalize" }, {
            priority: 1,
          });
        }
        return { ean, found: false };
      }

      // Armazena o buffer raw no Redis e enfileira o recorte
      await storeImageBuffer(batchId, ean, result.buffer);

      await cropQueue.add(
        `crop-${ean}`,
        {
          batchId,
          ean,
          source: result.source,
          url: result.url,
        },
        { attempts: 2, backoff: { type: "exponential", delay: 2000 } }
      );

      console.log(`[SearchWorker] EAN ${ean} encontrado via ${result.source}, enviado para recorte`);
      return { ean, found: true, source: result.source };
    } catch (err) {
      console.error(`[SearchWorker] Erro EAN ${ean}:`, err.message);

      const batchDone = await recordEanResult(batchId, {
        ean,
        success: false,
        reason: err.message,
      });

      if (batchDone) {
        await cropQueue.add("finalize-batch", { batchId, action: "finalize" }, {
          priority: 1,
        });
      }

      return { ean, error: err.message };
    }
  },
  {
    connection,
    concurrency: SEARCH_CONCURRENCY,
  }
);

searchWorker.on("failed", (job, err) => {
  console.error(`[SearchWorker] Job ${job?.id} falhou:`, err.message);
});

searchWorker.on("error", (err) => {
  console.error("[SearchWorker] Erro no worker:", err.message);
});

module.exports = { searchWorker };
