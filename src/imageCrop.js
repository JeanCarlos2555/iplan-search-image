const sharp = require("sharp");

/**
 * Recorta a imagem removendo o fundo/espaço em branco ao redor do produto.
 * Usa o sharp para detectar e remover bordas com cor uniforme (trim).
 *
 * @param {Buffer} imageBuffer - Buffer da imagem original
 * @returns {Promise<Buffer>} - Buffer da imagem recortada em PNG
 */
async function cropProductImage(imageBuffer) {
  // Primeiro, converte para PNG com canal alpha para manipulação
  let image = sharp(imageBuffer).png();

  // Obtém metadata para saber dimensões
  const metadata = await image.metadata();
  if (!metadata.width || !metadata.height) {
    throw new Error("Não foi possível obter dimensões da imagem");
  }

  // Usa trim() do sharp para remover bordas com cor similar
  // threshold: tolerância de cor para considerar como "fundo"
  const trimmed = await sharp(imageBuffer)
    .trim({ threshold: 30 })
    .png()
    .toBuffer();

  // Verifica se a imagem resultante não ficou muito pequena
  const trimmedMeta = await sharp(trimmed).metadata();
  if (trimmedMeta.width < 10 || trimmedMeta.height < 10) {
    // Se ficou muito pequena, retorna a original sem trim agressivo
    return sharp(imageBuffer).trim({ threshold: 10 }).png().toBuffer();
  }

  return trimmed;
}

module.exports = { cropProductImage };
