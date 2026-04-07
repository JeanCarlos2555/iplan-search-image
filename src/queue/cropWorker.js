const { Worker } = require("bullmq");
const path = require("path");
const fs = require("fs");
const archiver = require("archiver");
const { createRedisConnection, QUEUE_NAMES } = require("./config");
const { cropProductImage } = require("../imageCrop");
const {
  recordEanResult,
  completeBatch,
  getImageBuffer,
  cleanupImageBuffers,
  getBatch,
} = require("./batchManager");

const connection = createRedisConnection();

const CROP_CONCURRENCY = parseInt(process.env.CROP_CONCURRENCY || "3", 10);
const OUTPUT_DIR = process.env.ZIP_OUTPUT_DIR || path.join(process.cwd(), "output");

// Garante que o diretório de saída existe
if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

/**
 * Worker da fila de recorte.
 * Dois tipos de job:
 * 1. Recorte individual: recebe { batchId, ean, source, url }
 * 2. Finalização do lote: recebe { batchId, action: "finalize" }
 */
const cropWorker = new Worker(
  QUEUE_NAMES.CROP,
  async (job) => {
    const { batchId, action } = job.data;

    // Job de finalização: gera o ZIP
    if (action === "finalize") {
      return await finalizeBatch(batchId);
    }

    // Job de recorte individual
    const { ean, source, url } = job.data;
    console.log(`[CropWorker] Recortando EAN ${ean} do lote ${batchId}`);

    try {
      const imageBuffer = await getImageBuffer(batchId, ean);
      if (!imageBuffer) {
        throw new Error("Buffer da imagem não encontrado no Redis");
      }

      const croppedBuffer = await cropProductImage(imageBuffer);

      // Sobrescreve com a imagem recortada
      const { storeImageBuffer } = require("./batchManager");
      await storeImageBuffer(batchId, ean, croppedBuffer);

      const batchDone = await recordEanResult(batchId, {
        ean,
        success: true,
        source,
        url,
      });

      console.log(`[CropWorker] EAN ${ean} recortado com sucesso`);

      if (batchDone) {
        await finalizeBatch(batchId);
      }

      return { ean, cropped: true };
    } catch (err) {
      console.error(`[CropWorker] Erro ao recortar EAN ${ean}:`, err.message);

      const batchDone = await recordEanResult(batchId, {
        ean,
        success: false,
        reason: `Erro no recorte: ${err.message}`,
      });

      if (batchDone) {
        await finalizeBatch(batchId);
      }

      return { ean, error: err.message };
    }
  },
  {
    connection,
    concurrency: CROP_CONCURRENCY,
  }
);

/**
 * Gera o ZIP final com todas as imagens recortadas do lote.
 */
async function finalizeBatch(batchId) {
  console.log(`[CropWorker] Finalizando lote ${batchId}, gerando ZIP...`);

  const batch = await getBatch(batchId);
  if (!batch) {
    console.error(`[CropWorker] Lote ${batchId} não encontrado`);
    return;
  }

  const zipFileName = `batch_${batchId}.zip`;
  const zipPath = path.join(OUTPUT_DIR, zipFileName);

  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver("zip", { zlib: { level: 6 } });

    output.on("close", async () => {
      console.log(`[CropWorker] ZIP gerado: ${zipPath} (${archive.pointer()} bytes)`);
      await completeBatch(batchId, zipPath);

      // Limpa buffers temporários do Redis
      const successEans = batch.results.success.map((r) => r.ean);
      await cleanupImageBuffers(batchId, successEans);

      resolve({ batchId, zipPath });
    });

    archive.on("error", (err) => {
      console.error(`[CropWorker] Erro ao gerar ZIP:`, err.message);
      reject(err);
    });

    archive.pipe(output);

    // Adiciona cada imagem de sucesso ao ZIP
    const addImages = async () => {
      for (const item of batch.results.success) {
        const buffer = await getImageBuffer(batchId, item.ean);
        if (buffer) {
          archive.append(buffer, { name: `${item.ean}_L.png` });
        }
      }

      // Adiciona relatório
      const report = JSON.stringify(batch.results, null, 2);
      archive.append(report, { name: "relatorio.json" });

      await archive.finalize();
    };

    addImages().catch(reject);
  });
}

cropWorker.on("failed", (job, err) => {
  console.error(`[CropWorker] Job ${job?.id} falhou:`, err.message);
});

cropWorker.on("error", (err) => {
  console.error("[CropWorker] Erro no worker:", err.message);
});

module.exports = { cropWorker };
