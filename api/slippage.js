import { handleSlippageRequest } from '../server.mjs';

export default async function handler(req, res) {
  try {
    await handleSlippageRequest(req, res);
  } catch (err) {
    res.statusCode = 500;
    res.setHeader('content-type', 'application/json; charset=utf-8');
    res.setHeader('cache-control', 'no-store');
    res.end(JSON.stringify({ error: err instanceof Error ? err.message : String(err) }));
  }
}
