const { Worker, Queue } = require("bullmq");
const path = require("path");
const fs = require("fs");
const { createRedisConnection, QUEUE_NAMES } = require("./config");
const { searchImageByEan } = require("../imageSearch");
const { recordEanResult } = require("./batchManager");

const connection = createRedisConnection();

const cropQueue = new Queue(QUEUE_NAMES.CROP, { connection: createRedisConnection() });

const SEARCH_CONCURRENCY = parseInt(process.env.SEARCH_CONCURRENCY || "5", 10);
const OUTPUT_DIR = process.env.ZIP_OUTPUT_DIR || path.join(process.cwd(), "output");

/**
 * Worker da fila de busca.
 * Busca a imagem e salva o buffer raw em disco, depois enfileira o recorte.
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
          await cropQueue.add("finalize-batch", { batchId, action: "finalize" }, {
            priority: 1,
          });
        }
        return { ean, found: false };
      }

      // Salva buffer raw no disco
      const batchDir = path.join(OUTPUT_DIR, `batch_${batchId}`);
      if (!fs.existsSync(batchDir)) {
        fs.mkdirSync(batchDir, { recursive: true });
      }

      const rawPath = path.join(batchDir, `${ean}_raw.png`);
      fs.writeFileSync(rawPath, result.buffer);

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
