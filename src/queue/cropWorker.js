const { Worker } = require("bullmq");
const path = require("path");
const fs = require("fs");
const archiver = require("archiver");
const { createRedisConnection, QUEUE_NAMES } = require("./config");
const { cropProductImage } = require("../imageCrop");
const {
  recordEanResult,
  completeBatch,
  getBatch,
} = require("./batchManager");

const connection = createRedisConnection();

const CROP_CONCURRENCY = parseInt(process.env.CROP_CONCURRENCY || "3", 10);
const OUTPUT_DIR = process.env.ZIP_OUTPUT_DIR || path.join(process.cwd(), "output");

if (!fs.existsSync(OUTPUT_DIR)) {
  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

/**
 * Worker da fila de recorte.
 * Lê imagem raw do disco, recorta e salva como _L.png no disco.
 */
const cropWorker = new Worker(
  QUEUE_NAMES.CROP,
  async (job) => {
    const { batchId, action } = job.data;

    if (action === "finalize") {
      return await finalizeBatch(batchId);
    }

    const { ean, source, url } = job.data;
    console.log(`[CropWorker] Recortando EAN ${ean} do lote ${batchId}`);

    try {
      const batchDir = path.join(OUTPUT_DIR, `batch_${batchId}`);
      const rawPath = path.join(batchDir, `${ean}_raw.png`);

      if (!fs.existsSync(rawPath)) {
        throw new Error("Arquivo raw não encontrado no disco");
      }

      const imageBuffer = fs.readFileSync(rawPath);
      const croppedBuffer = await cropProductImage(imageBuffer);

      // Salva recortada e remove a raw
      const croppedPath = path.join(batchDir, `${ean}_L.png`);
      fs.writeFileSync(croppedPath, croppedBuffer);
      fs.unlinkSync(rawPath);

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

      // Remove raw se existir
      const rawPath = path.join(OUTPUT_DIR, `batch_${batchId}`, `${ean}_raw.png`);
      if (fs.existsSync(rawPath)) fs.unlinkSync(rawPath);

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
 * Gera o ZIP final a partir dos arquivos _L.png já salvos no disco.
 */
async function finalizeBatch(batchId) {
  console.log(`[CropWorker] Finalizando lote ${batchId}, gerando ZIP...`);

  const batch = await getBatch(batchId);
  if (!batch) {
    console.error(`[CropWorker] Lote ${batchId} não encontrado`);
    return;
  }

  const batchDir = path.join(OUTPUT_DIR, `batch_${batchId}`);
  const zipPath = path.join(OUTPUT_DIR, `batch_${batchId}.zip`);

  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver("zip", { store: true });

    output.on("close", async () => {
      console.log(`[CropWorker] ZIP gerado: ${zipPath} (${archive.pointer()} bytes)`);
      await completeBatch(batchId, zipPath);

      // Remove a pasta temporária do lote
      fs.rmSync(batchDir, { recursive: true, force: true });

      resolve({ batchId, zipPath });
    });

    archive.on("error", (err) => {
      console.error(`[CropWorker] Erro ao gerar ZIP:`, err.message);
      reject(err);
    });

    archive.pipe(output);

    // Lê os arquivos _L.png direto do disco
    for (const item of batch.results.success) {
      const filePath = path.join(batchDir, `${item.ean}_L.png`);
      if (fs.existsSync(filePath)) {
        archive.file(filePath, { name: `${item.ean}_L.png` });
      }
    }

    // Relatório
    const report = JSON.stringify(batch.results, null, 2);
    archive.append(report, { name: "relatorio.json" });

    archive.finalize();
  });
}

cropWorker.on("failed", (job, err) => {
  console.error(`[CropWorker] Job ${job?.id} falhou:`, err.message);
});

cropWorker.on("error", (err) => {
  console.error("[CropWorker] Erro no worker:", err.message);
});

module.exports = { cropWorker };
