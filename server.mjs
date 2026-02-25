import { createServer } from 'http';
import { readFile } from 'fs/promises';
import { extname, join, normalize } from 'path';
import { fileURLToPath } from 'url';

const PORT = process.env.PORT || 8787;
const PUBLIC_DIR = join(process.cwd(), 'public');
const CACHE_TTL_MS = 1500;

const O1_BASE = 'https://zo-mainnet.n1.xyz';
const NADO_GATEWAY = 'https://gateway.prod.nado.xyz/v1';
const PACIFICA_BASE = process.env.PACIFICA_BASE_URL || 'https://api.pacifica.fi';
const PARADEX_BASE = process.env.PARADEX_BASE_URL || 'https://api.prod.paradex.trade';
const EXTENDED_BASE = process.env.EXTENDED_BASE_URL || 'https://api.starknet.extended.exchange';
const EXTENDED_FALLBACK_BASES = ['https://api.extended.exchange'];
const FUNDING_API_URL = process.env.FUNDING_API_URL || 'https://fundingrate-auto.vercel.app/api/funding-8h';
const PARADEX_FEE_PROFILE = String(process.env.PARADEX_FEE_PROFILE || 'retail').trim().toLowerCase();
const PARADEX_TAKER_FEE_BPS = (() => {
  const explicit = Number(process.env.PARADEX_TAKER_FEE_BPS);
  if (Number.isFinite(explicit) && explicit >= 0) return explicit;
  // Paradex docs: retail perp taker fee is 0%, pro is 0.02% (=2 bps).
  return PARADEX_FEE_PROFILE === 'pro' ? 2 : 0;
})();

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
  '01xyz': {
    name: '01xyz',
    adapter: fetch01xyzBook,
  },
  nado: {
    name: 'Nado',
    adapter: fetchNadoBook,
  },
  pacifica: {
    name: 'Pacifica',
    adapter: fetchPacificaBook,
  },
  paradex: {
    name: 'Paradex',
    adapter: fetchParadexBook,
  },
  extended: {
    name: 'Extended',
    adapter: fetchExtendedBook,
  },
};

const DEFAULT_DISABLED_EXCHANGES = new Set(['bybit']);
const SUPPORTED_COINS = new Set(['BTC', 'ETH', 'SOL', 'BNB']);
const TAKER_FEE_BPS = {
  binance: 5,
  bybit: 5.5,
  hyperliquid: 4.5,
  aster: 5,
  okx: 5,
  lighter: 0,
  variational: 0,
  '01xyz': 5,
  nado: 5,
  pacifica: 5,
  paradex: PARADEX_TAKER_FEE_BPS,
  extended: 5,
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

function extractBaseSymbol(raw) {
  const s = String(raw || '').toUpperCase();
  if (!s) return '';
  for (const base of SUPPORTED_COINS) {
    if (s === base) return base;
    if (s.startsWith(`${base}-`)) return base;
    if (s.startsWith(`${base}/`)) return base;
    if (s.startsWith(`${base}_`)) return base;
    if (s.startsWith(base) && /PERP|USD|USDT|USDC/.test(s)) return base;
  }
  return '';
}

function pickFrom(obj, keys = []) {
  for (const key of keys) {
    if (obj && obj[key] != null) return obj[key];
  }
  return null;
}

function deepGet(obj, path) {
  if (!obj || typeof obj !== 'object') return undefined;
  const parts = String(path || '').split('.');
  let cur = obj;
  for (const part of parts) {
    if (cur == null || typeof cur !== 'object') return undefined;
    cur = cur[part];
  }
  return cur;
}

function pickNumDeep(obj, paths = []) {
  for (const path of paths) {
    const n = toNum(deepGet(obj, path));
    if (n != null) return n;
  }
  return null;
}

function flattenArrayish(payload) {
  if (Array.isArray(payload)) return payload;
  if (!payload || typeof payload !== 'object') return [];
  const arr = [];
  for (const v of Object.values(payload)) {
    if (Array.isArray(v)) arr.push(...v);
    else if (v && typeof v === 'object') arr.push(v);
  }
  return arr;
}

function buildFallbackBookFromMark(mark) {
  const p = toNum(mark);
  if (!(p > 0)) return null;
  return {
    asks: [[p, 1e9]],
    bids: [[p, 1e9]],
  };
}

async function fetchFundingSnapshotRows() {
  return fetchWithCache('funding:snapshot', async () => {
    const payload = await getJson(FUNDING_API_URL, { headers: { Accept: 'application/json' } });
    const rows = Array.isArray(payload?.rows) ? payload.rows : [];
    return rows;
  });
}

async function fetchFundingMarkFallback(exchange, coin) {
  const ex = String(exchange || '').toLowerCase();
  const sym = String(coin || '').toUpperCase();
  if (!ex || !sym) return null;

  try {
    const rows = await fetchFundingSnapshotRows();
    const row = rows.find((r) => {
      const rex = String(r?.exchange || '').toLowerCase();
      const rsym = String(r?.symbol || '').toUpperCase();
      return rex === ex && rsym === sym;
    });
    return toNum(row?.mark_price ?? row?.markPrice);
  } catch (_) {
    return null;
  }
}

function extractBookFromPayload(payload) {
  const roots = [
    payload,
    payload?.data,
    payload?.result,
    payload?.book,
    payload?.orderbook,
    payload?.orderBook,
    payload?.depth,
    payload?.marketDepth,
  ];

  for (const root of roots) {
    if (!root || typeof root !== 'object') continue;

    const asksRaw = root.asks ?? root.a ?? root.sell_orders ?? root.sellOrders;
    const bidsRaw = root.bids ?? root.b ?? root.buy_orders ?? root.buyOrders;
    const asks = normalizeLevels(asksRaw || []);
    const bids = normalizeLevels(bidsRaw || []);
    if (asks.length && bids.length) {
      return { asks, bids };
    }
  }

  return { asks: [], bids: [] };
}

function buildSyntheticSimulation({ qty, side, markPrice, bestBid, bestAsk }) {
  const qtyNum = Number(qty);
  if (!(qtyNum > 0)) throw new Error('qty must be positive');

  const mark = toNum(markPrice);
  const bid = toNum(bestBid);
  const ask = toNum(bestAsk);

  let topPrice = null;
  if (side === 'buy') topPrice = ask ?? mark ?? bid;
  else topPrice = bid ?? mark ?? ask;

  if (!(topPrice > 0)) throw new Error('top price unavailable');

  const avgExecutionPrice = topPrice;
  const notionalQuote = avgExecutionPrice * qtyNum;

  return {
    topPrice,
    avgExecutionPrice,
    requestedQty: qtyNum,
    filledQty: qtyNum,
    fillRatio: 1,
    slippagePct: 0,
    slippageBps: 0,
    notionalQuote,
    isPartial: false,
  };
}

function cacheQtyKey(qty) {
  const n = Number(qty);
  if (!Number.isFinite(n)) return '0';
  return (Math.round(n * 1e6) / 1e6).toString();
}

function normalizeQuoteForSynthetic(side, mark, bid, ask) {
  let m = toNum(mark);
  let b = toNum(bid);
  let a = toNum(ask);

  if (!(m > 0) && b > 0 && a > 0) {
    m = (b + a) / 2;
  }
  if (!(m > 0) && b > 0) m = b;
  if (!(m > 0) && a > 0) m = a;

  // Ensure side-specific top exists for buildSyntheticSimulation.
  if (side === 'buy' && !(a > 0) && m > 0) a = m;
  if (side === 'sell' && !(b > 0) && m > 0) b = m;

  return { mark: m, bid: b, ask: a };
}

async function fetchReferenceMark(coin) {
  const sym = String(coin || '').toUpperCase();
  if (!SUPPORTED_COINS.has(sym)) return null;

  return fetchWithCache(`refmark:${sym}`, async () => {
    // 1) Hyperliquid allMids
    try {
      const mids = await getJson('https://api.hyperliquid.xyz/info', {
        method: 'POST',
        body: JSON.stringify({ type: 'allMids' }),
      });
      const m = toNum(mids?.[sym] ?? mids?.mids?.[sym]);
      if (m && m > 0) return m;
    } catch (_) {}

    // 2) Coinbase spot ticker fallback
    try {
      const t = await getJson(`https://api.exchange.coinbase.com/products/${sym}-USD/ticker`);
      const p = toNum(t?.price ?? t?.last);
      if (p && p > 0) return p;
    } catch (_) {}

    // 3) Binance futures mark
    try {
      const t = await getJson(`https://fapi.binance.com/fapi/v1/premiumIndex?symbol=${sym}USDT`);
      const p = toNum(t?.markPrice ?? t?.indexPrice ?? t?.lastFundingRate);
      if (p && p > 0) return p;
    } catch (_) {}

    // 4) OKX ticker
    try {
      const t = await getJson(`https://www.okx.com/api/v5/market/ticker?instId=${sym}-USDT`);
      const row = Array.isArray(t?.data) ? t.data[0] : null;
      const p = toNum(row?.last ?? row?.markPx ?? row?.idxPx);
      if (p && p > 0) return p;
    } catch (_) {}

    return null;
  });
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

async function fetch01xyzBook(coin, qty, side) {
  const base = String(coin || '').toUpperCase();
  return fetchWithCache(`01xyz:${base}:${side}:${cacheQtyKey(qty)}`, async () => {
    const info = await getJson(`${O1_BASE}/info`);
    const markets = Array.isArray(info?.markets) ? info.markets : [];
    if (!markets.length) throw new Error('01xyz markets empty');

    const candidates = [];
    for (const m of markets) {
      const marketId = toNum(m?.market_id ?? m?.marketId ?? m?.id);
      if (marketId == null) continue;
      const symbol = String(m?.symbol || '').toUpperCase();
      if (!symbol.includes(base)) continue;
      const score =
        symbol === `${base}USD` ? 4 :
        symbol === `${base}-USD` ? 3 :
        symbol.startsWith(base) && symbol.endsWith('USD') ? 2 :
        symbol.startsWith(base) ? 1 : 0;
      if (score <= 0) continue;
      candidates.push({ marketId, symbol, score });
    }
    candidates.sort((a, b) => (b.score - a.score) || (a.marketId - b.marketId));
    if (!candidates.length) throw new Error(`01xyz market not found for ${base}`);

    let lastErr = null;
    for (const c of candidates.slice(0, 8)) {
      const marketId = c.marketId;

      const bookUrls = [
        `${O1_BASE}/market/${marketId}/orderbook`,
        `${O1_BASE}/market/${marketId}/book`,
        `${O1_BASE}/market/${marketId}/depth`,
      ];
      for (const url of bookUrls) {
        try {
          const payload = await getJson(url);
          const book = extractBookFromPayload(payload);
          if (book.asks.length && book.bids.length) return book;
        } catch (err) {
          lastErr = err;
        }
      }

      try {
        const st = await getJson(`${O1_BASE}/market/${marketId}/stats`);
        const perp = st?.perpStats ?? st?.perp_stats ?? {};
        const mark =
          toNum(st?.mark_price) ??
          toNum(st?.markPrice) ??
          toNum(perp?.mark_price) ??
          toNum(perp?.markPrice) ??
          await fetchReferenceMark(base);
        const bid =
          toNum(st?.best_bid) ??
          toNum(st?.bestBid) ??
          toNum(st?.bid_price) ??
          toNum(st?.bidPrice) ??
          toNum(perp?.best_bid) ??
          toNum(perp?.bestBid);
        const ask =
          toNum(st?.best_ask) ??
          toNum(st?.bestAsk) ??
          toNum(st?.ask_price) ??
          toNum(st?.askPrice) ??
          toNum(perp?.best_ask) ??
          toNum(perp?.bestAsk);
        const simulation = buildSyntheticSimulation({ qty, side, markPrice: mark, bestBid: bid, bestAsk: ask });
        return { kind: 'simulation', simulation };
      } catch (err) {
        lastErr = err;
      }
    }

    throw new Error(`01xyz book unavailable: ${lastErr instanceof Error ? lastErr.message : 'unknown error'}`);
  });
}

async function fetchNadoBook(coin, qty, side) {
  const base = String(coin || '').toUpperCase();
  return fetchWithCache(`nado:${base}:${side}:${cacheQtyKey(qty)}`, async () => {
    let picked = null;
    let wanted = `${base}-PERP`;
    let symbolsErr = null;
    try {
      const symbolsRes = await getJson(`${NADO_GATEWAY}/symbols`);
      const symbolsList = Array.isArray(symbolsRes)
        ? symbolsRes
        : Array.isArray(symbolsRes?.data)
          ? symbolsRes.data
          : Object.values(symbolsRes?.data?.symbols || symbolsRes?.symbols || {});

      if (Array.isArray(symbolsList) && symbolsList.length) {
        picked =
          symbolsList.find((it) => String(it?.symbol || it?.symbol_key || it?.symbolKey || '').toUpperCase() === wanted) ||
          symbolsList.find((it) => {
            const s = String(it?.symbol || it?.symbol_key || it?.symbolKey || it?.name || '').toUpperCase();
            return extractBaseSymbol(s) === base && s.includes('PERP');
          }) ||
          symbolsList.find((it) => {
            const s = String(it?.symbol || it?.symbol_key || it?.symbolKey || it?.name || '').toUpperCase();
            return extractBaseSymbol(s) === base;
          }) ||
          null;
      }
    } catch (e) {
      symbolsErr = e;
    }

    const productId = toNum(picked?.product_id ?? picked?.productId ?? picked?.id);
    const symbol = String(picked?.symbol || picked?.symbol_key || picked?.symbolKey || wanted);

    const bookUrls = [
      `${NADO_GATEWAY}/orderbook?symbol=${encodeURIComponent(symbol)}`,
      `${NADO_GATEWAY}/orderbook?symbol_key=${encodeURIComponent(symbol)}`,
      productId != null ? `${NADO_GATEWAY}/orderbook?product_id=${encodeURIComponent(productId)}` : '',
      `${NADO_GATEWAY}/depth?symbol=${encodeURIComponent(symbol)}`,
      productId != null ? `${NADO_GATEWAY}/depth?product_id=${encodeURIComponent(productId)}` : '',
      `${NADO_GATEWAY}/book?symbol=${encodeURIComponent(symbol)}`,
    ].filter(Boolean);

    let lastErr = null;
    for (const url of bookUrls) {
      try {
        const payload = await getJson(url);
        const book = extractBookFromPayload(payload);
        if (book.asks.length && book.bids.length) return book;
      } catch (err) {
        lastErr = err;
      }
    }

    let mark =
      pickNumDeep(picked, [
        'mark_price', 'markPrice', 'index_price', 'indexPrice', 'last_price', 'lastPrice',
        'price', 'last', 'oracle_price', 'oraclePrice',
        'stats.mark_price', 'stats.markPrice', 'stats.last_price', 'stats.lastPrice',
        'ticker.mark_price', 'ticker.markPrice', 'ticker.last_price', 'ticker.lastPrice',
      ]) ??
      (await fetchFundingMarkFallback('nado', base)) ??
      (await fetchReferenceMark(base));
    let bid =
      pickNumDeep(picked, [
        'best_bid', 'bestBid', 'bid_price', 'bidPrice', 'bid', 'bbo.bid',
        'stats.best_bid', 'stats.bestBid', 'ticker.best_bid', 'ticker.bestBid',
      ]);
    let ask =
      pickNumDeep(picked, [
        'best_ask', 'bestAsk', 'ask_price', 'askPrice', 'ask', 'bbo.ask',
        'stats.best_ask', 'stats.bestAsk', 'ticker.best_ask', 'ticker.bestAsk',
      ]);
    ({ mark, bid, ask } = normalizeQuoteForSynthetic(side, mark, bid, ask));

    const sideTop = side === 'buy' ? ask : bid;
    if (!(sideTop > 0) && !(mark > 0) && !(bid > 0) && !(ask > 0)) {
      const hint = symbolsErr instanceof Error ? `, symbols: ${symbolsErr.message}` : '';
      throw new Error(`nado book unavailable: ${lastErr instanceof Error ? lastErr.message : 'no market price'}${hint}`);
    }
    return { kind: 'simulation', simulation: buildSyntheticSimulation({ qty, side, markPrice: mark, bestBid: bid, bestAsk: ask }) };
  });
}

async function fetchPacificaBook(coin, qty, side) {
  const base = String(coin || '').toUpperCase();
  return fetchWithCache(`pacifica:${base}:${side}:${cacheQtyKey(qty)}`, async () => {
    const symbols = [`${base}-USD`, `${base}USD`, `${base}/USD`, base];
    const bookUrls = [];
    for (const s of symbols) {
      bookUrls.push(`${PACIFICA_BASE}/api/v1/info/orderbook?symbol=${encodeURIComponent(s)}`);
      bookUrls.push(`${PACIFICA_BASE}/api/v1/info/books?symbol=${encodeURIComponent(s)}`);
      bookUrls.push(`${PACIFICA_BASE}/api/v1/info/markets/${encodeURIComponent(s)}/orderbook`);
      bookUrls.push(`${PACIFICA_BASE}/api/v1/info/markets/${encodeURIComponent(s)}/book`);
    }

    let lastErr = null;
    for (const url of Array.from(new Set(bookUrls))) {
      try {
        const payload = await getJson(url);
        const book = extractBookFromPayload(payload);
        if (book.asks.length && book.bids.length) return book;
      } catch (err) {
        lastErr = err;
      }
    }

    let it = null;
    try {
      const prices = await getJson(`${PACIFICA_BASE}/api/v1/info/prices`, { headers: { Accept: 'application/json' } });
      const items = [
        ...flattenArrayish(prices),
        ...flattenArrayish(prices?.data),
        ...flattenArrayish(prices?.result),
        ...flattenArrayish(prices?.prices),
      ];
      it = items.find((x) => {
        const s = String(
          x?.symbol ?? x?.market ?? x?.pair ?? x?.name ?? x?.coin ?? x?.asset ?? x?.instrument ?? '',
        );
        return extractBaseSymbol(s) === base;
      }) || null;
    } catch (_) {
      it = null;
    }

    let mark =
      pickNumDeep(it, [
        'mark', 'mark_price', 'markPrice', 'price', 'last', 'last_price', 'lastPrice',
        'index_price', 'indexPrice', 'oracle_price', 'oraclePrice',
      ]) ??
      (await fetchFundingMarkFallback('pacifica', base)) ??
      (await fetchReferenceMark(base));
    let bid = pickNumDeep(it, ['best_bid', 'bestBid', 'bid_price', 'bidPrice', 'bid', 'bbo.bid']);
    let ask = pickNumDeep(it, ['best_ask', 'bestAsk', 'ask_price', 'askPrice', 'ask', 'bbo.ask']);
    ({ mark, bid, ask } = normalizeQuoteForSynthetic(side, mark, bid, ask));

    const sideTop = side === 'buy' ? ask : bid;
    if (!(sideTop > 0) && !(mark > 0) && !(bid > 0) && !(ask > 0)) {
      throw new Error(`pacifica book unavailable: ${lastErr instanceof Error ? lastErr.message : 'no market price'}`);
    }
    return { kind: 'simulation', simulation: buildSyntheticSimulation({ qty, side, markPrice: mark, bestBid: bid, bestAsk: ask }) };
  });
}

async function fetchParadexBook(coin, qty, side) {
  const base = String(coin || '').toUpperCase();
  return fetchWithCache(`paradex:${base}:${side}:${cacheQtyKey(qty)}`, async () => {
    const markets = [`${base}-USD-PERP`, `${base}-USD`, `${base}-PERP`];
    const bookUrls = [];
    for (const m of markets) {
      bookUrls.push(`${PARADEX_BASE}/v1/orderbook/${encodeURIComponent(m)}`);
      bookUrls.push(`${PARADEX_BASE}/v1/orderbook?market=${encodeURIComponent(m)}`);
      bookUrls.push(`${PARADEX_BASE}/v1/markets/${encodeURIComponent(m)}/orderbook`);
    }

    let lastErr = null;
    for (const url of Array.from(new Set(bookUrls))) {
      try {
        const payload = await getJson(url, { headers: { Accept: 'application/json' } });
        const book = extractBookFromPayload(payload);
        if (book.asks.length && book.bids.length) return book;
      } catch (err) {
        lastErr = err;
      }
    }

    const summary = await getJson(`${PARADEX_BASE}/v1/markets/summary?market=ALL`, { headers: { Accept: 'application/json' } });
    const items = Array.isArray(summary?.results) ? summary.results : Array.isArray(summary) ? summary : [];
    const it =
      items.find((x) => String(x?.symbol || '').toUpperCase() === `${base}-USD-PERP`) ||
      items.find((x) => String(x?.symbol || '').toUpperCase().startsWith(`${base}-`) && String(x?.symbol || '').toUpperCase().includes('PERP'));

    const mark = toNum(it?.mark_price) ?? toNum(it?.markPrice) ?? await fetchReferenceMark(base);
    const bid = toNum(it?.best_bid) ?? toNum(it?.bestBid) ?? toNum(it?.bid_price) ?? toNum(it?.bidPrice);
    const ask = toNum(it?.best_ask) ?? toNum(it?.bestAsk) ?? toNum(it?.ask_price) ?? toNum(it?.askPrice);
    if (!(mark > 0) && !(bid > 0) && !(ask > 0)) {
      throw new Error(`paradex book unavailable: ${lastErr instanceof Error ? lastErr.message : 'no market price'}`);
    }
    return { kind: 'simulation', simulation: buildSyntheticSimulation({ qty, side, markPrice: mark, bestBid: bid, bestAsk: ask }) };
  });
}

async function fetchExtendedBook(coin, qty, side) {
  const base = String(coin || '').toUpperCase();
  return fetchWithCache(`extended:${base}:${side}:${cacheQtyKey(qty)}`, async () => {
    const bases = [EXTENDED_BASE, ...EXTENDED_FALLBACK_BASES.filter((b) => b !== EXTENDED_BASE)];
    let lastErr = null;

    for (const baseUrl of bases) {
      const marketCandidates = [`${base}-USD`, `${base}-USD-PERP`, `${base}-PERP`];

      for (const market of marketCandidates) {
        const orderUrls = [
          `${baseUrl}/api/v1/info/markets/${encodeURIComponent(market)}/orderbook`,
          `${baseUrl}/api/v1/info/markets/${encodeURIComponent(market)}/book`,
          `${baseUrl}/api/v1/info/markets/${encodeURIComponent(market)}/depth`,
        ];
        for (const url of orderUrls) {
          try {
            const payload = await getJson(url, { headers: { Accept: 'application/json' } });
            const book = extractBookFromPayload(payload);
            if (book.asks.length && book.bids.length) return book;
          } catch (err) {
            lastErr = err;
          }
        }

        try {
          const st = await getJson(
            `${baseUrl}/api/v1/info/markets/${encodeURIComponent(market)}/stats`,
            { headers: { Accept: 'application/json' } },
          );
          const p = st?.data ?? st?.marketStats ?? st;
          const mark =
            toNum(p?.markPrice) ??
            toNum(p?.mark_price) ??
            toNum(p?.oraclePrice) ??
            toNum(p?.oracle_price) ??
            toNum(p?.indexPrice) ??
            toNum(p?.index_price) ??
            await fetchReferenceMark(base);
          const bid = toNum(p?.best_bid) ?? toNum(p?.bestBid) ?? toNum(p?.bid_price) ?? toNum(p?.bidPrice);
          const ask = toNum(p?.best_ask) ?? toNum(p?.bestAsk) ?? toNum(p?.ask_price) ?? toNum(p?.askPrice);
          if (mark > 0 || bid > 0 || ask > 0) {
            return { kind: 'simulation', simulation: buildSyntheticSimulation({ qty, side, markPrice: mark, bestBid: bid, bestAsk: ask }) };
          }
        } catch (err) {
          lastErr = err;
        }
      }
    }

    throw new Error(`extended book unavailable: ${lastErr instanceof Error ? lastErr.message : 'unknown error'}`);
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

    const fallbackMark = (await fetchFundingMarkFallback('lighter', coin)) ?? (await fetchReferenceMark(coin));
    const fallbackBook = buildFallbackBookFromMark(fallbackMark);
    if (fallbackBook) {
      return fallbackBook;
    }
    const msg = lastErr instanceof Error ? lastErr.message : 'Unknown Lighter API error';
    throw new Error(`Unable to load Lighter book: ${msg}`);
  });
}

function parseVariationalBucketSize_(key) {
  const s = String(key || '').trim().toLowerCase().replace(/_/g, '');
  if (!s || s === 'updatedat') return null;
  const m = s.match(/^(?:size|notional)?(\d+(?:\.\d+)?)([km]?)$/i);
  if (!m) return null;
  const n = Number(m[1]);
  if (!Number.isFinite(n) || n <= 0) return null;
  const suf = String(m[2] || '').toLowerCase();
  const mul = suf === 'k' ? 1_000 : suf === 'm' ? 1_000_000 : 1;
  return n * mul;
}

function getVariationalQuotePoints_(quotes, side) {
  if (!quotes || typeof quotes !== 'object') return [];
  const pointsBySize = new Map();

  for (const [k, v] of Object.entries(quotes)) {
    if (!v || typeof v !== 'object') continue;
    const size = parseVariationalBucketSize_(k);
    if (!(size > 0)) continue;
    const price = toNum(v?.[side] ?? v?.[String(side).toUpperCase()]);
    if (!(price > 0)) continue;
    if (!pointsBySize.has(size)) pointsBySize.set(size, price);
  }

  return Array.from(pointsBySize.entries())
    .map(([size, price]) => ({ size, price }))
    .sort((a, b) => a.size - b.size);
}

function pickVariationalQuotePrice(quotes, notional, side) {
  const points = getVariationalQuotePoints_(quotes, side);
  if (!points.length) return null;

  const smallest = points[0];
  const largest = points[points.length - 1];

  if (!(notional > 0) || notional <= smallest.size) return smallest.price;
  if (notional <= smallest.size * 10) return smallest.price; // small tickets: use top bucket directly
  if (notional >= largest.size) return largest.price;

  for (let i = 0; i < points.length - 1; i += 1) {
    const left = points[i];
    const right = points[i + 1];
    if (notional >= left.size && notional <= right.size) {
      const t = (notional - left.size) / (right.size - left.size);
      return left.price + (right.price - left.price) * t;
    }
  }

  return largest.price;
}

function pickVariationalTopPrice(quotes, side) {
  const points = getVariationalQuotePoints_(quotes, side);
  return points.length ? points[0].price : null;
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
  const topFromQuotes = pickVariationalTopPrice(listing?.quotes, quoteSide);
  let usedSpreadFallback = false;
  let usedStaleTopFallback = false;
  let quotePrice = !isQuoteStale ? pickVariationalQuotePrice(listing?.quotes, notional, quoteSide) : null;
  if (!quotePrice) {
    quotePrice = pickVariationalQuotePrice(listing?.quotes, notional, quoteSide);
  }

  if (isQuoteStale) {
    const spreadBps = toNum(listing?.base_spread_bps);
    const spreadBasedPrice = estimateFromBaseSpread(markPrice, spreadBps, side);
    if (spreadBasedPrice) {
      quotePrice = spreadBasedPrice;
      usedSpreadFallback = true;
    } else if (topFromQuotes && topFromQuotes > 0) {
      // stale quote interpolation can overshoot for small tickets; stick to top bucket when fresh spread is unavailable.
      quotePrice = topFromQuotes;
      usedStaleTopFallback = true;
    }
  }
  if (!quotePrice || quotePrice <= 0) {
    throw new Error(`Variational quote unavailable for ${coin}`);
  }

  let topPrice = topFromQuotes ?? quotePrice;
  if (usedSpreadFallback || usedStaleTopFallback) {
    // If we already switched to spread fallback, do not derive slippage from stale quote buckets.
    topPrice = quotePrice;
  }
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
      quoteStaleTopFallback: usedStaleTopFallback,
      quoteSpreadFallback: usedSpreadFallback,
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

function parseBoolParam(value, defaultValue = false) {
  if (value == null || value === '') return defaultValue;
  const s = String(value).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(s)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(s)) return false;
  return defaultValue;
}

async function handleSlippage(req, res, url) {
  const coin = String(url.searchParams.get('coin') || 'BTC').toUpperCase();
  const qty = Number(url.searchParams.get('qty') || '1');
  const side = String(url.searchParams.get('side') || 'buy').toLowerCase();
  const includeBybit = parseBoolParam(url.searchParams.get('include_bybit'), false);

  if (!SUPPORTED_COINS.has(coin)) {
    return json(res, 400, { error: 'coin must be one of BTC, ETH, SOL, BNB' });
  }

  if (!Number.isFinite(qty) || qty <= 0) {
    return json(res, 400, { error: 'qty must be a positive number' });
  }

  if (side !== 'buy' && side !== 'sell') {
    return json(res, 400, { error: 'side must be buy or sell' });
  }

  const exchangeEntries = Object.entries(EXCHANGES).filter(([id]) => {
    if (!includeBybit && DEFAULT_DISABLED_EXCHANGES.has(id)) return false;
    return true;
  });

  const tasks = exchangeEntries.map(async ([id, conf]) => {
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
