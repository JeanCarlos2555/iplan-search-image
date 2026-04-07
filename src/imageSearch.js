const axios = require("axios");
const cheerio = require("cheerio");
const { execFileSync } = require("child_process");

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

/**
 * Carrega os sites customizados da variável de ambiente CUSTOM_SEARCH_URLS.
 * Formato: URLs separadas por vírgula, com {EAN} como placeholder.
 *
 * Exemplo:
 * CUSTOM_SEARCH_URLS=https://www.farma22.com.br/{EAN},https://www.drogariascampea.com.br/{EAN}
 */
function getCustomUrls() {
  const raw = process.env.CUSTOM_SEARCH_URLS || "";
  if (!raw.trim()) return [];
  return raw
    .split(",")
    .map((u) => u.trim())
    .filter(Boolean);
}

/**
 * Baixa uma imagem a partir de uma URL e retorna o Buffer.
 */
async function downloadImage(url) {
  const response = await axios.get(url, {
    responseType: "arraybuffer",
    timeout: 15000,
    headers: {
      "User-Agent": USER_AGENT,
      Accept: "image/*",
    },
    maxContentLength: 10 * 1024 * 1024,
  });
  const contentType = response.headers["content-type"] || "";
  if (!contentType.startsWith("image/")) {
    throw new Error("Resposta não é uma imagem");
  }
  return Buffer.from(response.data);
}

/**
 * Busca imagem do produto pelo EAN.
 * Tenta múltiplas fontes em sequência até encontrar.
 */
async function searchImageByEan(ean) {
  const sources = [
    { name: "CustomSites", fn: () => searchCustomSites(ean) },
    { name: "OpenFoodFacts", fn: () => searchOpenFoodFacts(ean) },
    { name: "OpenBeautyFacts", fn: () => searchOpenBeautyFacts(ean) },
    { name: "Serper", fn: () => searchSerper(ean) },
    { name: "SerpApi", fn: () => searchSerpApi(ean) },
  ];

  for (const source of sources) {
    try {
      console.log(`  [${source.name}] Tentando...`);
      const result = await source.fn();
      if (result) {
        console.log(`  [${source.name}] Imagem encontrada!`);
        return { buffer: result.buffer, source: source.name, url: result.url };
      }
      console.log(`  [${source.name}] Nada encontrado.`);
    } catch (err) {
      console.log(`  [${source.name}] Erro: ${err.message}`);
      continue;
    }
  }

  return null;
}

/**
 * Open Food Facts - banco de dados aberto de produtos alimentícios.
 */
async function searchOpenFoodFacts(ean) {
  const url = `https://world.openfoodfacts.org/api/v2/product/${ean}.json`;
  const { data } = await axios.get(url, { timeout: 10000 });

  if (data.status === 1 && data.product) {
    const imageUrl = data.product.image_front_url || data.product.image_url;
    if (imageUrl) {
      const buffer = await downloadImage(imageUrl);
      return { buffer, url: imageUrl };
    }
  }
  return null;
}

/**
 * Open Beauty Facts - banco de dados de cosméticos/beleza.
 */
async function searchOpenBeautyFacts(ean) {
  const url = `https://world.openbeautyfacts.org/api/v2/product/${ean}.json`;
  const { data } = await axios.get(url, { timeout: 10000 });

  if (data.status === 1 && data.product) {
    const imageUrl = data.product.image_front_url || data.product.image_url;
    if (imageUrl) {
      const buffer = await downloadImage(imageUrl);
      return { buffer, url: imageUrl };
    }
  }
  return null;
}

/**
 * Serper API - Google Images via API (fallback pago #1).
 */
async function searchSerper(ean) {
  const apiKey = process.env.SERPER_API_KEY;
  if (!apiKey) return null;

  const { data } = await axios.post(
    "https://google.serper.dev/images",
    { q: ean, gl: "br", hl: "pt" },
    {
      headers: {
        "X-API-KEY": apiKey,
        "Content-Type": "application/json",
      },
      timeout: 15000,
    },
  );

  const images = data.images || [];
  for (const img of images.slice(0, 8)) {
    try {
      if (img.imageUrl) {
        const buffer = await downloadImage(img.imageUrl);
        return { buffer, url: img.imageUrl };
      }
    } catch {
      continue;
    }
  }

  return null;
}

/**
 * SerpApi - Google Images via API (fallback pago #2).
 */
async function searchSerpApi(ean) {
  const apiKey = process.env.SERPAPI_API_KEY;
  if (!apiKey) return null;

  const { data } = await axios.get("https://serpapi.com/search", {
    params: {
      engine: "google_images",
      q: ean,
      google_domain: "google.com",
      hl: "en",
      gl: "us",
      api_key: apiKey,
    },
    timeout: 15000,
  });

  const results = data.images_results || [];
  for (const img of results.slice(0, 8)) {
    try {
      if (img.original) {
        const buffer = await downloadImage(img.original);
        return { buffer, url: img.original };
      }
    } catch {
      continue;
    }
  }

  return null;
}

/**
 * Busca uma página ou imagem via curl (bypass Cloudflare).
 * Retorna o conteúdo como string (HTML) ou Buffer (imagem).
 */
function fetchWithCurl(url, binary = false) {
  const args = [
    "-s",
    "-L",
    "--max-time",
    "15",
    "-H",
    `User-Agent: ${USER_AGENT}`,
    "-H",
    "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "-H",
    "Accept-Language: pt-BR,pt;q=0.9,en;q=0.8",
    url,
  ];
  const result = execFileSync("curl", args, {
    encoding: binary ? "buffer" : "utf-8",
    maxBuffer: 10 * 1024 * 1024,
    timeout: 20000,
  });
  return result;
}

/**
 * Busca em sites customizados definidos via CUSTOM_SEARCH_URLS.
 * Faz scraping da página buscando a imagem do produto via:
 * 1. JSON-LD (application/ld+json) → campo "image"
 * 2. Meta tag og:image
 * 3. Tags <img> com src grande (heurística)
 *
 * Usa Axios primeiro; se der 403 (Cloudflare), tenta via curl.
 */
async function searchCustomSites(ean) {
  const templates = getCustomUrls();
  if (templates.length === 0) return null;

  for (const template of templates) {
    const pageUrl = template.replace(/\{EAN\}/gi, ean);
    const siteName = new URL(pageUrl).hostname;

    try {
      console.log(`    [Custom] ${pageUrl}...`);

      let html;
      try {
        const { data } = await axios.get(pageUrl, {
          headers: {
            "User-Agent": USER_AGENT,
            Accept: "text/html,application/xhtml+xml",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
          },
          timeout: 15000,
          maxRedirects: 5,
          validateStatus: (status) => status < 400,
        });
        html = data;
      } catch (axiosErr) {
        const status = axiosErr.response?.status;
        if (status === 403) {
          console.log(`    [Custom] ${siteName} → 403, tentando via curl...`);
          html = fetchWithCurl(pageUrl);
        } else {
          console.log(`    [Custom] ${siteName} → HTTP ${status || axiosErr.code || axiosErr.message}`);
          continue;
        }
      }

      const imageUrl = extractProductImage(html, pageUrl);
      if (imageUrl) {
        console.log(`    [Custom] ${siteName} → imagem encontrada`);
        try {
          const buffer = await downloadImage(imageUrl);
          return { buffer, url: pageUrl };
        } catch {
          // Se download via axios falhar, tenta curl
          console.log(
            `    [Custom] ${siteName} → download axios falhou, tentando curl...`,
          );
          const buf = fetchWithCurl(imageUrl, true);
          if (buf && buf.length > 100) return { buffer: buf, url: pageUrl };
        }
      }
    } catch (err) {
      console.log(`    [Custom] ${siteName} → erro: ${err.message}`);
      continue;
    }
  }

  return null;
}

/**
 * Extrai a URL da imagem do produto a partir do HTML da página.
 * Estratégias (em ordem de prioridade):
 * 1. JSON-LD structured data
 * 2. Meta og:image
 * 3. Maior imagem <img> na página (heurística)
 */
function extractProductImage(html, pageUrl) {
  const $ = cheerio.load(html);
  const baseUrl = new URL(pageUrl).origin;

  // 1. JSON-LD (application/ld+json)
  const jsonLdImage = extractFromJsonLd($);
  if (jsonLdImage) {
    return resolveUrl(jsonLdImage, baseUrl);
  }

  // 2. Meta og:image
  const ogImage = $('meta[property="og:image"]').attr("content");
  if (ogImage && isProductImageUrl(ogImage)) {
    return resolveUrl(ogImage, baseUrl);
  }

  // 3. Heurística: busca <img> com atributos que indicam imagem de produto
  const productSelectors = [
    "img[data-zoom-image]",
    "img.product-image",
    "img.js-product-image",
    ".product-image img",
    ".product-main-image img",
    ".gallery-image img",
    'img[itemprop="image"]',
    ".vtex-store-components-3-x-productImageTag",
    "img.vtex-store-components-3-x-productImageTag--main",
  ];

  for (const selector of productSelectors) {
    const src =
      $(selector).first().attr("src") ||
      $(selector).first().attr("data-src") ||
      $(selector).first().attr("data-zoom-image");
    if (src && isProductImageUrl(src)) {
      return resolveUrl(src, baseUrl);
    }
  }

  // 4. Fallback: qualquer <img> grande que pareça imagem de produto
  let bestImg = null;
  $("img").each(function () {
    const src = $(this).attr("src") || $(this).attr("data-src") || "";
    if (!src || !isProductImageUrl(src)) return;

    const width = parseInt($(this).attr("width") || "0", 10);
    const height = parseInt($(this).attr("height") || "0", 10);

    if (
      width > 200 ||
      height > 200 ||
      src.includes("arquivos/ids/") ||
      src.includes("/img_prod/") ||
      src.includes("product")
    ) {
      if (!bestImg) bestImg = src;
    }
  });

  if (bestImg) {
    return resolveUrl(bestImg, baseUrl);
  }

  return null;
}

/**
 * Extrai URL de imagem de dados JSON-LD na página.
 */
function extractFromJsonLd($) {
  const scripts = $('script[type="application/ld+json"]').toArray();

  for (const script of scripts) {
    try {
      const json = JSON.parse($(script).html());
      const imageUrl = findImageInJsonLd(json);
      if (imageUrl) return imageUrl;
    } catch {
      continue;
    }
  }
  return null;
}

/**
 * Busca recursivamente o campo "image" dentro de um objeto JSON-LD.
 */
function findImageInJsonLd(obj) {
  if (!obj || typeof obj !== "object") return null;

  // Prioriza Product type
  if (obj["@type"] === "Product" && obj.image) {
    const img = Array.isArray(obj.image) ? obj.image[0] : obj.image;
    if (typeof img === "string") return img;
    if (img && img.url) return img.url;
    if (img && img.contentUrl) return img.contentUrl;
  }

  // Busca em ItemList
  if (obj.itemListElement && Array.isArray(obj.itemListElement)) {
    for (const item of obj.itemListElement) {
      const found = findImageInJsonLd(item.item || item);
      if (found) return found;
    }
  }

  // Busca genérica
  if (obj.image) {
    const img = Array.isArray(obj.image) ? obj.image[0] : obj.image;
    if (typeof img === "string" && isProductImageUrl(img)) return img;
  }

  return null;
}

/**
 * Verifica se uma URL parece ser de uma imagem de produto (não ícone/logo).
 */
function isProductImageUrl(url) {
  if (!url) return false;
  const lower = url.toLowerCase();
  return (
    (lower.includes(".jpg") ||
      lower.includes(".jpeg") ||
      lower.includes(".png") ||
      lower.includes(".webp")) &&
    !lower.includes("favicon") &&
    !lower.includes("icon") &&
    !lower.includes("logo") &&
    !lower.includes("banner") &&
    !lower.includes("sprite") &&
    !lower.includes("placeholder") &&
    !lower.includes("1x1")
  );
}

/**
 * Resolve URLs relativas para absolutas.
 */
function resolveUrl(url, baseUrl) {
  if (url.startsWith("http")) return url;
  if (url.startsWith("//")) return "https:" + url;
  return baseUrl + (url.startsWith("/") ? "" : "/") + url;
}

module.exports = { searchImageByEan };
