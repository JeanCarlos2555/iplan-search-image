const sharp = require("sharp");

/**
 * Recorta a imagem removendo o fundo/espaço em branco ao redor do produto.
 * Usa o sharp para detectar e remover bordas com cor uniforme (trim).
 *
 * @param {Buffer} imageBuffer - Buffer da imagem original
 * @returns {Promise<Buffer>} - Buffer da imagem recortada em PNG
 */
const MAX_IMAGE_SIZE = parseInt(process.env.MAX_IMAGE_SIZE || "0", 10);

async function cropProductImage(imageBuffer) {
  const metadata = await sharp(imageBuffer).metadata();
  if (!metadata.width || !metadata.height) {
    throw new Error("Não foi possível obter dimensões da imagem");
  }

  const trimmed = await sharp(imageBuffer)
    .trim({ threshold: 30 })
    .png()
    .toBuffer();

  const trimmedMeta = await sharp(trimmed).metadata();
  let result = trimmed;

  if (trimmedMeta.width < 10 || trimmedMeta.height < 10) {
    result = await sharp(imageBuffer).trim({ threshold: 10 }).png().toBuffer();
  }

  if (MAX_IMAGE_SIZE > 0) {
    const meta = await sharp(result).metadata();
    if (meta.width > MAX_IMAGE_SIZE || meta.height > MAX_IMAGE_SIZE) {
      result = await sharp(result)
        .resize(MAX_IMAGE_SIZE, MAX_IMAGE_SIZE, { fit: "inside", withoutEnlargement: true })
        .png()
        .toBuffer();
    }
  }

  return result;
}

module.exports = { cropProductImage };
