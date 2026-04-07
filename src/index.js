require("dotenv").config();
const express = require("express");
const path = require("path");
const fs = require("fs");
const archiver = require("archiver");
const multer = require("multer");
const AdmZip = require("adm-zip");
const { Queue } = require("bullmq");
const { createBullBoard } = require("@bull-board/api");
const { BullMQAdapter } = require("@bull-board/api/bullMQAdapter");
const { ExpressAdapter } = require("@bull-board/express");
const { searchImageByEan } = require("./imageSearch");
const { cropProductImage } = require("./imageCrop");
const { createRedisConnection, QUEUE_NAMES } = require("./queue/config");
const { createBatch, getBatch } = require("./queue/batchManager");

// Inicia os workers (basta importar para registrar)
require("./queue/searchWorker");
require("./queue/cropWorker");

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 50 * 1024 * 1024 },
});

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const MAX_EANS_PER_REQUEST = parseInt(process.env.MAX_EANS_PER_REQUEST || "1000", 10);
const OUTPUT_DIR = process.env.ZIP_OUTPUT_DIR || path.join(process.cwd(), "output");

// Filas
const searchQueue = new Queue(QUEUE_NAMES.SEARCH, {
  connection: createRedisConnection(),
});
const cropQueue = new Queue(QUEUE_NAMES.CROP, {
  connection: createRedisConnection(),
});

// Bull Board - painel de monitoramento das filas
const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/admin/queues");

createBullBoard({
  queues: [new BullMQAdapter(searchQueue), new BullMQAdapter(cropQueue)],
  serverAdapter,
});

app.use("/admin/queues", serverAdapter.getRouter());

/**
 * GET /api/image/:ean
 * Busca e retorna a imagem recortada de um único EAN (em memória).
 */
app.get("/api/image/:ean", async (req, res) => {
  const { ean } = req.params;

  if (!ean || !/^\d{8,14}$/.test(ean)) {
    return res
      .status(400)
      .json({ error: "EAN inválido. Deve conter entre 8 e 14 dígitos." });
  }

  try {
    console.log(`[EAN] Buscando imagem para: ${ean}`);

    const result = await searchImageByEan(ean);
    if (!result) {
      return res
        .status(404)
        .json({ error: `Imagem não encontrada para o EAN: ${ean}` });
    }

    console.log(`[EAN] Imagem encontrada via ${result.source}, recortando...`);

    const croppedImage = await cropProductImage(result.buffer);

    res.set({
      "Content-Type": "image/png",
      "Content-Disposition": `inline; filename="${ean}_L.png"`,
      "Cache-Control": "no-store",
    });

    return res.send(croppedImage);
  } catch (err) {
    console.error(`[ERRO] EAN ${ean}:`, err.message);
    return res
      .status(500)
      .json({ error: "Erro ao processar imagem", details: err.message });
  }
});

/**
 * POST /api/images
 * Recebe um array de EANs, cria um lote e enfileira para processamento assíncrono.
 * Body: { "eans": ["7896685301494", "7891000100103", ...] }
 *
 * Para 1 EAN: retorna imagem diretamente (em memória).
 * Para múltiplos: retorna ID do lote para acompanhamento via GET /api/batch/:id.
 */
app.post("/api/images", async (req, res) => {
  const { eans } = req.body;

  if (!Array.isArray(eans) || eans.length === 0) {
    return res
      .status(400)
      .json({ error: "Envie um array 'eans' com pelo menos 1 código." });
  }

  if (eans.length > MAX_EANS_PER_REQUEST) {
    return res.status(400).json({
      error: `Máximo de ${MAX_EANS_PER_REQUEST} EANs por requisição.`,
    });
  }

  const invalidEans = eans.filter((e) => !/^\d{8,14}$/.test(e));
  if (invalidEans.length > 0) {
    return res.status(400).json({
      error: "EANs inválidos encontrados",
      invalid: invalidEans,
    });
  }

  // Se for apenas 1 EAN, retorna a imagem diretamente (em memória)
  if (eans.length === 1) {
    try {
      const result = await searchImageByEan(eans[0]);
      if (!result) {
        return res
          .status(404)
          .json({ error: `Imagem não encontrada para o EAN: ${eans[0]}` });
      }
      const croppedImage = await cropProductImage(result.buffer);

      res.set({
        "Content-Type": "image/png",
        "Content-Disposition": `inline; filename="${eans[0]}_L.png"`,
        "Cache-Control": "no-store",
      });
      return res.send(croppedImage);
    } catch (err) {
      console.error(`[ERRO] EAN ${eans[0]}:`, err.message);
      return res
        .status(500)
        .json({ error: "Erro ao processar imagem", details: err.message });
    }
  }

  // Para múltiplos EANs: cria lote e enfileira
  try {
    const batchId = await createBatch(eans);

    console.log(`[BATCH] Lote ${batchId} criado com ${eans.length} EANs`);

    // Enfileira cada EAN na fila de busca
    const jobs = eans.map((ean) => ({
      name: `search-${ean}`,
      data: { batchId, ean },
      opts: { attempts: 2, backoff: { type: "exponential", delay: 3000 } },
    }));

    await searchQueue.addBulk(jobs);

    console.log(`[BATCH] ${eans.length} jobs enfileirados na fila de busca`);

    return res.status(202).json({
      batchId,
      total: eans.length,
      status: "processing",
      statusUrl: `/api/batch/${batchId}`,
      message: `Lote criado com ${eans.length} EANs. Acompanhe o progresso na URL de status.`,
    });
  } catch (err) {
    console.error("[BATCH ERRO]:", err.message);
    return res
      .status(500)
      .json({ error: "Erro ao criar lote", details: err.message });
  }
});

/**
 * GET /api/batch/:batchId
 * Retorna o status do lote. Quando 100% concluído, inclui a URL de download.
 */
app.get("/api/batch/:batchId", async (req, res) => {
  const { batchId } = req.params;

  try {
    const batch = await getBatch(batchId);

    if (!batch) {
      return res.status(404).json({ error: "Lote não encontrado." });
    }

    return res.json(batch);
  } catch (err) {
    console.error(`[BATCH STATUS] Erro:`, err.message);
    return res
      .status(500)
      .json({ error: "Erro ao consultar lote", details: err.message });
  }
});

/**
 * GET /api/batch/:batchId/download
 * Faz o download do ZIP gerado para o lote.
 */
app.get("/api/batch/:batchId/download", async (req, res) => {
  const { batchId } = req.params;

  try {
    const batch = await getBatch(batchId);

    if (!batch) {
      return res.status(404).json({ error: "Lote não encontrado." });
    }

    if (batch.status !== "done") {
      return res.status(409).json({
        error: "Lote ainda em processamento.",
        progress: `${batch.progress}%`,
      });
    }

    const zipPath = path.join(OUTPUT_DIR, `batch_${batchId}.zip`);

    if (!fs.existsSync(zipPath)) {
      return res.status(404).json({ error: "Arquivo ZIP não encontrado." });
    }

    res.set({
      "Content-Type": "application/zip",
      "Content-Disposition": `attachment; filename="batch_${batchId}.zip"`,
      "Cache-Control": "no-store",
    });

    const stream = fs.createReadStream(zipPath);
    stream.pipe(res);
  } catch (err) {
    console.error(`[BATCH DOWNLOAD] Erro:`, err.message);
    return res
      .status(500)
      .json({ error: "Erro ao baixar ZIP", details: err.message });
  }
});

/**
 * POST /api/crop
 * Recebe um ZIP com imagens, recorta as bordas de cada uma e retorna um novo ZIP.
 * Enviar como multipart/form-data com campo "file".
 */
app.post("/api/crop", upload.single("file"), async (req, res) => {
  if (!req.file) {
    return res
      .status(400)
      .json({ error: "Envie um arquivo ZIP no campo 'file'." });
  }

  try {
    const zip = new AdmZip(req.file.buffer);
    const entries = zip.getEntries();

    const imageEntries = entries.filter((e) => {
      const name = e.entryName.toLowerCase();
      return (
        !e.isDirectory &&
        (name.endsWith(".jpg") ||
          name.endsWith(".jpeg") ||
          name.endsWith(".png") ||
          name.endsWith(".webp"))
      );
    });

    if (imageEntries.length === 0) {
      return res.status(400).json({
        error: "Nenhuma imagem encontrada no ZIP (jpg, jpeg, png, webp).",
      });
    }

    console.log(`[CROP] Processando ${imageEntries.length} imagens do ZIP...`);

    res.set({
      "Content-Type": "application/zip",
      "Content-Disposition": 'attachment; filename="cropped_images.zip"',
      "Cache-Control": "no-store",
    });

    const archive = archiver("zip", { zlib: { level: 6 } });
    archive.pipe(res);

    const results = { success: [], failed: [] };

    for (const entry of imageEntries) {
      try {
        const imageBuffer = entry.getData();
        const cropped = await cropProductImage(imageBuffer);

        // Mantém o nome original mas força extensão .png
        const baseName = entry.entryName.replace(/\.[^.]+$/, "");
        const outputName = `${baseName}.png`;

        archive.append(cropped, { name: outputName });
        results.success.push(entry.entryName);
        console.log(`  [CROP] OK: ${entry.entryName}`);
      } catch (err) {
        console.error(`  [CROP] ERRO: ${entry.entryName} - ${err.message}`);
        results.failed.push({ file: entry.entryName, reason: err.message });
      }
    }

    const report = JSON.stringify(results, null, 2);
    archive.append(report, { name: "relatorio.json" });

    await archive.finalize();

    console.log(
      `[CROP] Concluído. Sucesso: ${results.success.length}, Falhas: ${results.failed.length}`,
    );
  } catch (err) {
    console.error("[CROP ERRO]:", err.message);
    if (!res.headersSent) {
      return res
        .status(500)
        .json({ error: "Erro ao processar ZIP", details: err.message });
    }
  }
});

/**
 * GET /api/health
 * Health check endpoint.
 */
app.get("/api/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

app.listen(PORT, () => {
  console.log(`Servidor rodando em http://localhost:${PORT}`);
  console.log(`Limite de EANs por requisição: ${MAX_EANS_PER_REQUEST}`);
  console.log(`Endpoints:`);
  console.log(`  GET  /api/image/:ean         - Busca imagem de 1 EAN (em memória)`);
  console.log(
    `  POST /api/images             - Enfileira busca de vários EANs (body: { eans: [...] })`,
  );
  console.log(`  GET  /api/batch/:id          - Status do lote`);
  console.log(`  GET  /api/batch/:id/download - Download do ZIP do lote`);
  console.log(
    `  POST /api/crop               - Recebe ZIP com imagens e retorna ZIP com imagens recortadas`,
  );
  console.log(`  GET  /api/health             - Health check`);
  console.log(`  GET  /admin/queues           - Bull Board (painel das filas)`);
});
