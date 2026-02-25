import { createServer } from 'http';
import { readFile } from 'fs/promises';
import { extname, join, normalize } from 'path';
import { fileURLToPath } from 'url';

const PORT = process.env.PORT || 8787;
const PUBLIC_DIR = join(process.cwd(), 'public');
const CACHE_TTL_MS = 1500;

const cache = new Map();

const EXCHANGES = {
  binance: {
    name: 'Binance',
    adapter: fetchBinanceBook,
  },
  bybit: {
    name: 'Bybit',
    adapter: fetchBybitBook,
  },
  hyperliquid: {
    name: 'Hyperliquid',
    adapter: fetchHyperliquidBook,
  },
  aster: {
    name: 'Aster',
    adapter: fetchAsterBook,
  },
  okx: {
    name: 'OKX',
    adapter: fetchOkxBook,
  },
  lighter: {
    name: 'Lighter',
    adapter: fetchLighterBook,
  },
  variational: {
    name: 'Variational',
    adapter: fetchVariationalBook,
  },
};

const SUPPORTED_COINS = new Set(['BTC', 'ETH', 'SOL', 'BNB']);
const TAKER_FEE_BPS = {
  binance: 5,
  bybit: 5.5,
  hyperliquid: 4.5,
  aster: 5,
  okx: 5,
  lighter: 0,
  variational: 0,
};

// Fallback market ids observed in production funding feed.
const LIGHTER_MARKET_ID_BY_COIN = {
  BTC: 1,
  ETH: 0,
  SOL: 2,
  BNB: 25,
};

function toNum(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeLevels(rawLevels) {
  if (!Array.isArray(rawLevels)) {
    return [];
  }
  return rawLevels
    .map((level) => {
      if (Array.isArray(level)) {
        return { price: toNum(level[0]), size: toNum(level[1]) };
      }
      if (typeof level === 'object' && level) {
        return {
          price: toNum(level.price ?? level.px ?? level[0]),
          size: toNum(level.size ?? level.sz ?? level.quantity ?? level.qty ?? level[1]),
        };
      }
      return { price: null, size: null };
    })
    .filter((l) => l.price && l.size && l.price > 0 && l.size > 0);
}

function simulateMarketOrder({ asks, bids, qty, side }) {
  const bookSide = side === 'buy' ? asks : bids;
  if (!bookSide.length) {
    throw new Error('Empty book side');
  }

  const topPrice = bookSide[0].price;
  let remain = qty;
  let fillBase = 0;
  let fillQuote = 0;

  for (const level of bookSide) {
    if (remain <= 0) {
      break;
    }
    const take = Math.min(remain, level.size);
    fillBase += take;
    fillQuote += take * level.price;
    remain -= take;
  }

  if (fillBase <= 0) {
    throw new Error('No fill');
  }

  const avgExecutionPrice = fillQuote / fillBase;
  const isPartial = fillBase < qty;
  const slippagePct =
    side === 'buy'
      ? ((avgExecutionPrice - topPrice) / topPrice) * 100
      : ((topPrice - avgExecutionPrice) / topPrice) * 100;

  return {
    topPrice,
    avgExecutionPrice,
    requestedQty: qty,
    filledQty: fillBase,
    fillRatio: fillBase / qty,
    slippagePct,
    slippageBps: slippagePct * 100,
    notionalQuote: fillQuote,
    isPartial,
  };
}

function enrichWithFee(simulation, exchange, side) {
  const feeBps = TAKER_FEE_BPS[exchange] ?? 0;
  const feeQuote = simulation.notionalQuote * (feeBps / 10000);
  const topNotionalQuote = simulation.topPrice * simulation.filledQty;
  const executionCostQuote =
    side === 'buy'
      ? simulation.notionalQuote - topNotionalQuote
      : topNotionalQuote - simulation.notionalQuote;

  const allInCostQuote = executionCostQuote + feeQuote;
  const allInCostBps = simulation.slippageBps + feeBps;

  return {
    ...simulation,
    feeBps,
    feePct: feeBps / 100,
    feeQuote,
    executionCostQuote,
    allInCostQuote,
    allInCostBps,
    allInCostPct: allInCostBps / 100,
  };
}

async function getJson(url, options = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), options.timeoutMs ?? 5000);
  try {
    const res = await fetch(url, {
      ...options,
      headers: {
        'content-type': 'application/json',
        ...(options.headers || {}),
      },
      signal: controller.signal,
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}`);
    }
    return await res.json();
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchWithCache(cacheKey, fn) {
  const now = Date.now();
  const entry = cache.get(cacheKey);
  if (entry && now - entry.time < CACHE_TTL_MS) {
    return entry.value;
  }
  const value = await fn();
  cache.set(cacheKey, { time: now, value });
  return value;
}

async function fetchBinanceBook(coin) {
  const symbol = `${coin}USDT`;
  return fetchWithCache(`binance:${symbol}`, async () => {
    const urls = [
      `https://api.binance.com/api/v3/depth?symbol=${symbol}&limit=1000`,
      `https://data-api.binance.vision/api/v3/depth?symbol=${symbol}&limit=1000`,
      `https://fapi.binance.com/fapi/v1/depth?symbol=${symbol}&limit=1000`,
    ];
    let lastErr = null;
    for (const url of urls) {
      try {
        const data = await getJson(url);
        const asks = normalizeLevels(data?.asks);
        const bids = normalizeLevels(data?.bids);
        if (asks.length && bids.length) {
          return { asks, bids };
        }
      } catch (err) {
        lastErr = err;
      }
    }
    throw new Error(`Binance book unavailable: ${lastErr instanceof Error ? lastErr.message : 'unknown error'}`);
  });
}

async function fetchBybitBook(coin) {
  const symbol = `${coin}USDT`;
  return fetchWithCache(`bybit:${symbol}`, async () => {
    const data = await getJson(`https://api.bybit.com/v5/market/orderbook?category=linear&symbol=${symbol}&limit=200`);
    return {
      asks: normalizeLevels(data?.result?.a),
      bids: normalizeLevels(data?.result?.b),
    };
  });
}

async function fetchHyperliquidBook(coin) {
  return fetchWithCache(`hyperliquid:${coin}`, async () => {
    const data = await getJson('https://api.hyperliquid.xyz/info', {
      method: 'POST',
      body: JSON.stringify({
        type: 'l2Book',
        coin,
      }),
    });
    const levels = Array.isArray(data?.levels) ? data.levels : [];
    return {
      bids: normalizeLevels(levels[0]),
      asks: normalizeLevels(levels[1]),
    };
  });
}

async function fetchAsterBook(coin) {
  const symbol = `${coin}USDT`;
  return fetchWithCache(`aster:${symbol}`, async () => {
    const data = await getJson(`https://fapi.asterdex.com/fapi/v1/depth?symbol=${symbol}&limit=1000`);
    return {
      asks: normalizeLevels(data.asks),
      bids: normalizeLevels(data.bids),
    };
  });
}

async function fetchOkxBook(coin) {
  const instId = `${coin}-USDT`;
  return fetchWithCache(`okx:${instId}`, async () => {
    const data = await getJson(`https://www.okx.com/api/v5/market/books?instId=${instId}&sz=400`);
    const book = data?.data?.[0] ?? {};
    return {
      asks: normalizeLevels(book.asks),
      bids: normalizeLevels(book.bids),
    };
  });
}

function unpackCollection(payload, keys = []) {
  if (Array.isArray(payload)) {
    return payload;
  }
  if (!payload || typeof payload !== 'object') {
    return [];
  }
  for (const key of keys) {
    if (Array.isArray(payload[key])) {
      return payload[key];
    }
    if (payload[key] && typeof payload[key] === 'object') {
      for (const nestedKey of keys) {
        if (Array.isArray(payload[key][nestedKey])) {
          return payload[key][nestedKey];
        }
      }
    }
  }
  return [];
}

function collectObjects(payload, keys = []) {
  const out = [];
  const first = unpackCollection(payload, keys);
  if (first.length) out.push(...first);
  if (payload && typeof payload === 'object') {
    for (const value of Object.values(payload)) {
      const nested = unpackCollection(value, keys);
      if (nested.length) out.push(...nested);
    }
  }
  return out;
}

function looksLikeLighterMarket(obj) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;
  return (
    obj.market_id != null ||
    obj.marketId != null ||
    obj.market_index != null ||
    obj.marketIndex != null ||
    obj.id != null ||
    obj.index != null ||
    typeof obj.symbol === 'string' ||
    typeof obj.market_symbol === 'string' ||
    typeof obj.name === 'string' ||
    typeof obj.base_symbol === 'string'
  );
}

function collectLighterMarketRecords(payload) {
  const seen = new Set();
  const out = [];
  const visit = (node, depth) => {
    if (!node || depth > 4) return;
    if (Array.isArray(node)) {
      for (const item of node) visit(item, depth + 1);
      return;
    }
    if (typeof node !== 'object') return;

    if (looksLikeLighterMarket(node)) {
      const key = String(node.market_id ?? node.marketId ?? node.id ?? node.index ?? node.symbol ?? node.name);
      if (!seen.has(key)) {
        seen.add(key);
        out.push(node);
      }
    }

    for (const value of Object.values(node)) {
      if (value && typeof value === 'object') {
        visit(value, depth + 1);
      }
    }
  };
  visit(payload, 0);
  return out;
}

function rankLighterOrderBooks(allBooks, coin) {
  const normCoin = coin.toUpperCase();
  const aliases = [normCoin];
  if (normCoin === 'BTC') aliases.push('XBT', 'BITCOIN');
  const hasAlias = (text) => {
    const upper = String(text ?? '').toUpperCase();
    if (!upper) return false;
    return aliases.some((alias) => {
      return (
        upper === alias ||
        upper.startsWith(`${alias}-`) ||
        upper.startsWith(`${alias}/`) ||
        upper.includes(`-${alias}-`) ||
        upper.includes(`/${alias}/`) ||
        upper.includes(`${alias}/`) ||
        upper.includes(`/${alias}`) ||
        upper.includes(alias)
      );
    });
  };

  const blockedStatus = new Set(['INACTIVE', 'CLOSED', 'DELISTED', 'SETTLED']);
  const scored = allBooks
    .map((book) => {
      const symbol = String(book?.symbol ?? book?.market_symbol ?? '').toUpperCase();
      const baseSymbol = String(book?.base_symbol ?? book?.baseTokenSymbol ?? book?.base_asset_symbol ?? '').toUpperCase();
      const marketName = String(book?.name ?? '').toUpperCase();
      const ticker = String(book?.ticker ?? '').toUpperCase();
      const quoteSymbol = String(book?.quote_symbol ?? '').toUpperCase();
      const status = String(book?.status ?? '').toUpperCase();

      if (status && blockedStatus.has(status)) {
        return null;
      }

      const coinMatched = hasAlias(symbol) || hasAlias(baseSymbol) || hasAlias(marketName) || hasAlias(ticker);
      if (!coinMatched) return null;

      let score = 0;
      if (aliases.some((a) => symbol === a || symbol.startsWith(`${a}-`) || symbol.startsWith(`${a}/`))) score += 100;
      if (hasAlias(baseSymbol)) score += 60;
      if (hasAlias(symbol)) score += 40;
      if (hasAlias(marketName)) score += 20;
      if (hasAlias(ticker)) score += 15;
      if (symbol.includes('PERP')) score += 10;
      if (symbol.includes('USD') || quoteSymbol.includes('USD')) score += 8;
      if (status === 'ACTIVE') score += 5;

      return { book, score };
    })
    .filter(Boolean);

  scored.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    const aSymbol = String(a.book?.symbol ?? a.book?.market_symbol ?? '');
    const bSymbol = String(b.book?.symbol ?? b.book?.market_symbol ?? '');
    const aRank = aSymbol.includes('USDC') ? 0 : aSymbol.includes('USDT') ? 1 : 2;
    const bRank = bSymbol.includes('USDC') ? 0 : bSymbol.includes('USDT') ? 1 : 2;
    return aRank - bRank;
  });

  return scored.map((item) => item.book);
}

function pickLighterOrderBook(allBooks, coin) {
  const ranked = rankLighterOrderBooks(allBooks, coin);
  return ranked.length ? ranked[0] : null;
}

function parseScaledValue(raw, decimals) {
  if (raw == null) return null;
  const s = String(raw);
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  if (s.includes('.')) return n;
  return n / 10 ** decimals;
}

function mapLighterLevels(levels, priceDecimals, sizeDecimals) {
  if (!Array.isArray(levels)) return [];
  return levels
    .map((level) => {
      const priceRaw = Array.isArray(level) ? level[0] : level?.price;
      const sizeRaw = Array.isArray(level)
        ? level[1]
        : level?.remaining_base_amount ?? level?.size ?? level?.base_amount ?? level?.quantity;
      const price = parseScaledValue(priceRaw, priceDecimals);
      const size = parseScaledValue(sizeRaw, sizeDecimals);
      return [price, size];
    })
    .filter((level) => level[0] && level[1] && level[0] > 0 && level[1] > 0);
}

function splitLighterOrderRows(rows) {
  if (!Array.isArray(rows)) return { asks: [], bids: [] };

  const asks = [];
  const bids = [];
  for (const row of rows) {
    if (!row || typeof row !== 'object') continue;
    const sideRaw = String(row.side ?? row.order_side ?? row.direction ?? row.taker_side ?? '').toUpperCase();
    const isAskRaw = row.is_ask ?? row.isAsk ?? row.ask ?? row.isSell;
    const isAsk = typeof isAskRaw === 'boolean' ? isAskRaw : null;

    const side =
      sideRaw.includes('ASK') || sideRaw.includes('SELL')
        ? 'ask'
        : sideRaw.includes('BID') || sideRaw.includes('BUY')
          ? 'bid'
          : isAsk === true
            ? 'ask'
            : isAsk === false
              ? 'bid'
              : null;
    if (!side) continue;

    const price = row.price ?? row.px ?? row.limit_price ?? row.limitPrice ?? row.quote_price;
    const size =
      row.remaining_base_amount ??
      row.remainingBaseAmount ??
      row.size ??
      row.sz ??
      row.base_amount ??
      row.baseAmount ??
      row.quantity ??
      row.qty;

    const level = [price, size];
    if (side === 'ask') asks.push(level);
    else bids.push(level);
  }
  return { asks, bids };
}

function extractLighterOrders(payload) {
  if (!payload || typeof payload !== 'object') {
    return { asks: [], bids: [] };
  }
  if (Array.isArray(payload?.asks) || Array.isArray(payload?.bids)) {
    return { asks: payload.asks ?? [], bids: payload.bids ?? [] };
  }
  if (payload?.data && (Array.isArray(payload.data.asks) || Array.isArray(payload.data.bids))) {
    return { asks: payload.data.asks ?? [], bids: payload.data.bids ?? [] };
  }
  if (Array.isArray(payload?.sell_orders) || Array.isArray(payload?.buy_orders)) {
    return { asks: payload.sell_orders ?? [], bids: payload.buy_orders ?? [] };
  }
  if (payload?.data && (Array.isArray(payload.data.sell_orders) || Array.isArray(payload.data.buy_orders))) {
    return { asks: payload.data.sell_orders ?? [], bids: payload.data.buy_orders ?? [] };
  }
  const candidates = [
    payload?.orders,
    payload?.data?.orders,
    payload?.orderBookOrders,
    payload?.data?.orderBookOrders,
    payload?.rows,
    payload?.data?.rows,
    payload?.data?.data,
  ];
  for (const c of candidates) {
    if (!Array.isArray(c)) continue;
    const split = splitLighterOrderRows(c);
    if (split.asks.length || split.bids.length) return split;
  }
  return { asks: [], bids: [] };
}

function lighterSymbolCandidates(coin) {
  const base = coin.toUpperCase();
  const alt = base === 'BTC' ? 'XBT' : null;
  const bases = [base, alt].filter(Boolean);
  const quotes = ['USDC', 'USDT', 'USD'];
  const forms = [];
  for (const b of bases) {
    forms.push(b);
    forms.push(`${b}-PERP`);
    for (const q of quotes) {
      forms.push(`${b}/${q}`);
      forms.push(`${b}-${q}`);
    }
  }
  return Array.from(new Set(forms));
}

function extractLighterBookWithFallbackDecimals(payload, priceDecimals = 0, sizeDecimals = 0) {
  const orders = extractLighterOrders(payload);
  const asks = normalizeLevels(mapLighterLevels(orders.asks, priceDecimals, sizeDecimals));
  const bids = normalizeLevels(mapLighterLevels(orders.bids, priceDecimals, sizeDecimals));
  return { asks, bids };
}

function lighterMarketIndexOf(market) {
  return market?.market_index ?? market?.marketIndex ?? market?.market_id ?? market?.marketId ?? market?.id ?? market?.index;
}

function lighterMarketDecimalsOf(market) {
  const p = Number(market?.supported_price_decimals ?? market?.price_decimals ?? market?.priceDecimals ?? 0);
  const s = Number(market?.supported_size_decimals ?? market?.size_decimals ?? market?.sizeDecimals ?? 0);
  return {
    priceDecimals: Number.isFinite(p) && p >= 0 ? p : 0,
    sizeDecimals: Number.isFinite(s) && s >= 0 ? s : 0,
  };
}

async function fetchLighterBookByKnownMarketId(makeUrls, marketId, priceDecimals = 0, sizeDecimals = 0) {
  const limitValues = ['300', '200', '100', '50', ''];
  const urls = [];
  for (const limit of limitValues) {
    const limitQ = limit ? `&limit=${limit}` : '';
    urls.push(...makeUrls('orderBookOrders', `market_index=${marketId}${limitQ}`));
    urls.push(...makeUrls('orderBookOrders', `marketIndex=${marketId}${limitQ}`));
    urls.push(...makeUrls('orderBookOrders', `market_id=${marketId}${limitQ}`));
    urls.push(...makeUrls('orderBookOrders', `marketId=${marketId}${limitQ}`));
    urls.push(...makeUrls('orderBookOrders', `by=market_index&value=${marketId}${limitQ}`));
    urls.push(...makeUrls('orderBookOrders', `by=market_id&value=${marketId}${limitQ}`));
    urls.push(...makeUrls('orderBookOrders', `by=index&value=${marketId}${limitQ}`));
  }

  for (const url of Array.from(new Set(urls))) {
    try {
      const payload = await getJson(url);
      const book = extractLighterBookWithFallbackDecimals(payload, priceDecimals, sizeDecimals);
      if (book.asks.length && book.bids.length) return book;
    } catch (_) {}
  }
  return null;
}

async function fetchLighterBook(coin) {
  return fetchWithCache(`lighter:${coin}`, async () => {
    const baseConfigs = [
      { base: 'https://api.lighter.xyz', prefixes: ['/api/v1', '/v1', ''] },
      { base: 'https://lighter.xyz', prefixes: ['/api/v1', '/api', ''] },
      { base: 'https://mainnet.zklighter.elliot.ai', prefixes: ['/api/v1'] },
      { base: 'https://api.prod.lighter.xyz', prefixes: ['/api/v1', '/v1'] },
    ];
    let lastErr = null;

    for (const conf of baseConfigs) {
      const { base, prefixes } = conf;
      const makeUrls = (path, query = '') =>
        prefixes.map((prefix) => `${base}${prefix}/${path}${query ? `?${query}` : ''}`);

      try {
        const directSymbols = lighterSymbolCandidates(coin);
        for (const symbol of directSymbols) {
          const directUrls = [
            ...makeUrls('orderBookOrders', `symbol=${encodeURIComponent(symbol)}&limit=300`),
            ...makeUrls('orderBookOrders', `by=symbol&value=${encodeURIComponent(symbol)}&limit=300`),
          ];
          for (const url of directUrls) {
            try {
              const directPayload = await getJson(url);
              const directBook = extractLighterBookWithFallbackDecimals(directPayload, 0, 0);
              if (directBook.asks.length && directBook.bids.length) {
                return directBook;
              }
            } catch (_) {
              // fallback to market catalog lookup below
            }
          }
        }

        const marketCatalogUrls = [
          ...makeUrls('orderBookDetails'),
          ...makeUrls('orderBookDetails', 'limit=1000'),
          ...makeUrls('orderBookDetails', `symbol=${coin}`),
          ...makeUrls('orderBookDetails', `symbol=${coin}-USDC`),
          ...makeUrls('orderBookDetails', `symbol=${coin}/USDC`),
          ...(coin === 'BTC' ? makeUrls('orderBookDetails', 'symbol=XBT-USDC') : []),
          ...(coin === 'BTC' ? makeUrls('orderBookDetails', 'symbol=XBT/USDC') : []),
          ...makeUrls('orderBooks'),
          ...makeUrls('orderBooks', 'limit=1000'),
          ...makeUrls('orderBooks', `symbol=${coin}`),
          ...makeUrls('orderBooks', `symbol=${coin}-USDC`),
          ...makeUrls('orderBooks', `symbol=${coin}/USDC`),
          ...(coin === 'BTC' ? makeUrls('orderBooks', 'symbol=XBT-USDC') : []),
          ...(coin === 'BTC' ? makeUrls('orderBooks', 'symbol=XBT/USDC') : []),
          `${base}/api/explorer/markets`,
          `${base}/api/explorer/markets?limit=1000`,
          `${base}/api/explorer/markets?symbol=${coin}`,
          `${base}/api/explorer/markets?symbol=${coin}-USDC`,
          `${base}/api/explorer/markets?symbol=${coin}/USDC`,
          coin === 'BTC' ? `${base}/api/explorer/markets?symbol=XBT-USDC` : '',
          coin === 'BTC' ? `${base}/api/explorer/markets?symbol=XBT/USDC` : '',
        ];

        let books = [];
        let catalogErr = null;
        for (const url of marketCatalogUrls.filter(Boolean)) {
          try {
            const payload = await getJson(url);
            const chunk = collectObjects(payload, ['data', 'order_books', 'orderBooks', 'order_book_details', 'list']);
            if (chunk.length) books.push(...chunk);
            const genericChunk = collectLighterMarketRecords(payload);
            if (genericChunk.length) books.push(...genericChunk);
          } catch (err) {
            catalogErr = err;
            // try next endpoint shape
          }
        }
        const uniq = new Map();
        for (const b of books) {
          const key = String(b?.market_id ?? b?.marketId ?? b?.id ?? b?.symbol ?? b?.name ?? Math.random());
          if (!uniq.has(key)) uniq.set(key, b);
        }
        books = Array.from(uniq.values());

        const rankedCandidates = rankLighterOrderBooks(books, coin);
        const knownId = LIGHTER_MARKET_ID_BY_COIN[String(coin).toUpperCase()];
        if (!rankedCandidates.length && knownId != null) {
          const fallbackBook = await fetchLighterBookByKnownMarketId(makeUrls, knownId, 0, 0);
          if (fallbackBook && fallbackBook.asks.length && fallbackBook.bids.length) {
            return fallbackBook;
          }
        }
        if (!rankedCandidates.length) {
          const sample = books
            .slice(0, 12)
            .map((b) => b?.symbol ?? b?.market_symbol ?? b?.name ?? 'unknown')
            .join(', ');
          throw new Error(`No active ${coin} perp market found (markets=${books.length}, sample: ${sample || 'n/a'})`);
        }
        let asks = [];
        let bids = [];
        let orderErr = null;
        let selectedDesc = 'n/a';
        const shortlist = rankedCandidates.slice(0, 8);

        for (const selected of shortlist) {
          const marketIndex = lighterMarketIndexOf(selected);
          const { priceDecimals, sizeDecimals } = lighterMarketDecimalsOf(selected);
          const symbol = String(selected.symbol ?? selected.market_symbol ?? '').toUpperCase();
          selectedDesc = `${symbol || 'unknown'}#${marketIndex ?? 'na'}`;

          const directBook = extractLighterBookWithFallbackDecimals(selected, priceDecimals, sizeDecimals);
          if (directBook.asks.length && directBook.bids.length) {
            return directBook;
          }

          const limitValues = ['300', '200', '100', '50', ''];
          const orderUrls = [];
          for (const limit of limitValues) {
            const limitQ = limit ? `&limit=${limit}` : '';
            if (marketIndex != null) {
              orderUrls.push(...makeUrls('orderBookOrders', `market_index=${marketIndex}${limitQ}`));
              orderUrls.push(...makeUrls('orderBookOrders', `marketIndex=${marketIndex}${limitQ}`));
              orderUrls.push(...makeUrls('orderBookOrders', `market_id=${marketIndex}${limitQ}`));
              orderUrls.push(...makeUrls('orderBookOrders', `marketId=${marketIndex}${limitQ}`));
              orderUrls.push(...makeUrls('orderBookOrders', `by=market_index&value=${marketIndex}${limitQ}`));
              orderUrls.push(...makeUrls('orderBookOrders', `by=market_id&value=${marketIndex}${limitQ}`));
              orderUrls.push(...makeUrls('orderBookOrders', `by=index&value=${marketIndex}${limitQ}`));
            }
            if (symbol) {
              orderUrls.push(...makeUrls('orderBookOrders', `symbol=${encodeURIComponent(symbol)}${limitQ}`));
              orderUrls.push(...makeUrls('orderBookOrders', `by=symbol&value=${encodeURIComponent(symbol)}${limitQ}`));
            }
          }

          const uniqOrderUrls = Array.from(new Set(orderUrls.filter(Boolean)));
          for (const url of uniqOrderUrls) {
            try {
              const ordersPayload = await getJson(url);
              const orders = extractLighterOrders(ordersPayload);
              asks = normalizeLevels(mapLighterLevels(orders.asks, priceDecimals, sizeDecimals));
              bids = normalizeLevels(mapLighterLevels(orders.bids, priceDecimals, sizeDecimals));
              if (asks.length && bids.length) {
                return { asks, bids };
              }
            } catch (err) {
              orderErr = err;
            }
          }
        }

        if (knownId != null) {
          const fallbackBook = await fetchLighterBookByKnownMarketId(makeUrls, knownId, 0, 0);
          if (fallbackBook && fallbackBook.asks.length && fallbackBook.bids.length) {
            return fallbackBook;
          }
        }

        throw new Error(
          `Lighter returned empty asks/bids for ${selectedDesc}${orderErr instanceof Error ? `: ${orderErr.message}` : ''}${
            catalogErr instanceof Error ? ` (catalog: ${catalogErr.message})` : ''
          }`,
        );
      } catch (err) {
        lastErr = err;
      }
    }

    const msg = lastErr instanceof Error ? lastErr.message : 'Unknown Lighter API error';
    throw new Error(`Unable to load Lighter book: ${msg}`);
  });
}

function pickVariationalQuotePrice(quotes, notional, side) {
  if (!quotes || typeof quotes !== 'object') {
    return null;
  }
  const resolveSidePrice = (entry) => toNum(entry?.[side] ?? entry?.[side.toUpperCase()]);
  const resolveBucket = (bucketKeys) => {
    for (const key of bucketKeys) {
      if (quotes[key]) {
        const price = resolveSidePrice(quotes[key]);
        if (price) return price;
      }
    }
    return null;
  };

  const points = [
    { size: 1_000, keys: ['size_1k', '1k', 'notional_1k'] },
    { size: 100_000, keys: ['size_100k', '100k', 'notional_100k'] },
    { size: 1_000_000, keys: ['size_1m', '1m', 'notional_1m'] },
  ]
    .map((point) => {
      const price = resolveBucket(point.keys);
      return price ? { size: point.size, price } : null;
    })
    .filter(Boolean);

  if (!points.length) {
    return null;
  }
  points.sort((a, b) => a.size - b.size);

  if (notional <= points[0].size) return points[0].price;
  if (notional >= points[points.length - 1].size) return points[points.length - 1].price;

  for (let i = 0; i < points.length - 1; i += 1) {
    const left = points[i];
    const right = points[i + 1];
    if (notional >= left.size && notional <= right.size) {
      const t = (notional - left.size) / (right.size - left.size);
      return left.price + (right.price - left.price) * t;
    }
  }

  return points[points.length - 1].price;
}

function pickVariationalTopPrice(quotes, side) {
  if (!quotes || typeof quotes !== 'object') return null;
  const keys = ['size_1k', '1k', 'notional_1k', 'size_100k', '100k', 'notional_100k'];
  for (const key of keys) {
    const entry = quotes[key];
    const price = toNum(entry?.[side] ?? entry?.[side.toUpperCase()]);
    if (price && price > 0) return price;
  }
  return null;
}

function estimateFromBaseSpread(markPrice, baseSpreadBps, side) {
  if (!markPrice || !baseSpreadBps) return null;
  const halfSpreadRatio = (baseSpreadBps / 10000) / 2;
  return side === 'buy' ? markPrice * (1 + halfSpreadRatio) : markPrice * (1 - halfSpreadRatio);
}

async function fetchVariationalBook(coin, qty, side) {
  const payload = await fetchWithCache('variational:stats', async () =>
    getJson('https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats'),
  );
  const listings = unpackCollection(payload, ['listings', 'data']);
  const nestedListings = unpackCollection(payload?.data, ['listings', 'data']);
  const rows = listings.length ? listings : nestedListings;
  const listing = rows.find((item) => {
    const ticker = String(item?.ticker ?? item?.symbol ?? '').toUpperCase();
    return ticker === coin || ticker.startsWith(`${coin}-`) || ticker.startsWith(`${coin}/`);
  });

  if (!listing) {
    throw new Error(`Variational listing not found for ${coin}`);
  }

  const markPrice = toNum(listing?.mark_price ?? listing?.markPrice ?? listing?.oracle_price);
  if (!markPrice || markPrice <= 0) {
    throw new Error(`Variational mark price unavailable for ${coin}`);
  }

  const quoteUpdatedAt = Date.parse(String(listing?.quotes?.updated_at ?? ''));
  const quoteAgeSec = Number.isFinite(quoteUpdatedAt) ? Math.max(0, (Date.now() - quoteUpdatedAt) / 1000) : Infinity;
  const isQuoteStale = quoteAgeSec > 60;
  const notional = qty * markPrice;
  const quoteSide = side === 'buy' ? 'ask' : 'bid';
  let quotePrice = pickVariationalQuotePrice(listing?.quotes, notional, quoteSide);
  if (!quotePrice || isQuoteStale) {
    const spreadBps = toNum(listing?.base_spread_bps);
    const spreadBasedPrice = estimateFromBaseSpread(markPrice, spreadBps, side);
    if (spreadBasedPrice) {
      quotePrice = spreadBasedPrice;
    }
  }
  if (!quotePrice || quotePrice <= 0) {
    throw new Error(`Variational quote unavailable for ${coin}`);
  }

  let topPrice = pickVariationalTopPrice(listing?.quotes, quoteSide) ?? quotePrice;
  // Enforce market-order invariants to avoid negative slippage from stale/misaligned quote buckets.
  if (side === 'buy' && quotePrice < topPrice) {
    topPrice = quotePrice;
  }
  if (side === 'sell' && quotePrice > topPrice) {
    topPrice = quotePrice;
  }
  const slippagePct =
    side === 'buy' ? ((quotePrice - topPrice) / topPrice) * 100 : ((topPrice - quotePrice) / topPrice) * 100;

  return {
    kind: 'simulation',
    simulation: {
      topPrice,
      avgExecutionPrice: quotePrice,
      requestedQty: qty,
      filledQty: qty,
      fillRatio: 1,
      slippagePct,
      slippageBps: slippagePct * 100,
      notionalQuote: quotePrice * qty,
      isPartial: false,
      priceReference: 'mark',
      quoteAgeSec,
      quoteStaleFallback: isQuoteStale,
    },
  };
}

function json(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  });
  res.end(body);
}

function resolveContentType(pathname) {
  const ext = extname(pathname).toLowerCase();
  if (ext === '.html') return 'text/html; charset=utf-8';
  if (ext === '.css') return 'text/css; charset=utf-8';
  if (ext === '.js') return 'application/javascript; charset=utf-8';
  if (ext === '.json') return 'application/json; charset=utf-8';
  return 'text/plain; charset=utf-8';
}

async function handleSlippage(req, res, url) {
  const coin = String(url.searchParams.get('coin') || 'BTC').toUpperCase();
  const qty = Number(url.searchParams.get('qty') || '1');
  const side = String(url.searchParams.get('side') || 'buy').toLowerCase();

  if (!SUPPORTED_COINS.has(coin)) {
    return json(res, 400, { error: 'coin must be one of BTC, ETH, SOL, BNB' });
  }

  if (!Number.isFinite(qty) || qty <= 0) {
    return json(res, 400, { error: 'qty must be a positive number' });
  }

  if (side !== 'buy' && side !== 'sell') {
    return json(res, 400, { error: 'side must be buy or sell' });
  }

  const tasks = Object.entries(EXCHANGES).map(async ([id, conf]) => {
    try {
      const result = await conf.adapter(coin, qty, side);
      const simulation =
        result?.kind === 'simulation'
          ? result.simulation
          : simulateMarketOrder({
              asks: normalizeLevels(result.asks).sort((a, b) => a.price - b.price),
              bids: normalizeLevels(result.bids).sort((a, b) => b.price - a.price),
              qty,
              side,
            });
      const enriched = enrichWithFee(simulation, id, side);
      return {
        exchange: id,
        name: conf.name,
        status: 'ok',
        ...enriched,
      };
    } catch (err) {
      return {
        exchange: id,
        name: conf.name,
        status: 'error',
        error: err instanceof Error ? err.message : String(err),
      };
    }
  });

  const rows = await Promise.all(tasks);
  const okRows = rows.filter((r) => r.status === 'ok').sort((a, b) => a.allInCostBps - b.allInCostBps);
  const errRows = rows.filter((r) => r.status === 'error');

  json(res, 200, {
    ts: Date.now(),
    coin,
    qty,
    side,
    rows: [...okRows, ...errRows],
  });
}

async function handleStatic(req, res, url) {
  const pathname = url.pathname === '/' ? '/index.html' : url.pathname;
  const safePath = normalize(pathname).replace(/^\.+/, '');
  const filePath = join(PUBLIC_DIR, safePath);

  try {
    const content = await readFile(filePath);
    res.writeHead(200, {
      'content-type': resolveContentType(filePath),
      'cache-control': 'no-store',
    });
    res.end(content);
  } catch {
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('Not found');
  }
}

export async function handleSlippageRequest(req, res) {
  const host = req?.headers?.host || `localhost:${PORT}`;
  const url = new URL(req?.url || '/api/slippage', `http://${host}`);
  return handleSlippage(req, res, url);
}

export function createDashboardServer() {
  return createServer(async (req, res) => {
    try {
      const url = new URL(req.url || '/', `http://${req.headers.host || `localhost:${PORT}`}`);

      if (url.pathname === '/api/slippage') {
        await handleSlippage(req, res, url);
        return;
      }

      await handleStatic(req, res, url);
    } catch (err) {
      json(res, 500, {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });
}

const isMain = (() => {
  try {
    const thisFile = fileURLToPath(import.meta.url);
    const entry = process.argv?.[1] ? String(process.argv[1]) : '';
    return thisFile === entry;
  } catch {
    return false;
  }
})();

if (isMain) {
  const server = createDashboardServer();
  server.listen(PORT, () => {
    console.log(`slippage dashboard listening on http://localhost:${PORT}`);
  });
}
