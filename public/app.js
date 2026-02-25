const els = {
  coin: document.getElementById('coin'),
  qty: document.getElementById('qty'),
  side: document.getElementById('side'),
  interval: document.getElementById('interval'),
  refresh: document.getElementById('refresh'),
  status: document.getElementById('status'),
  updatedAt: document.getElementById('updatedAt'),
  rows: document.getElementById('rows'),
};

let timer = null;
let loading = false;

function fmt(n, digits = 4) {
  if (n == null || Number.isNaN(n)) return '-';
  return Number(n).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  });
}

function statusText(msg) {
  els.status.textContent = msg;
}

function rowHtml(row) {
  if (row.status !== 'ok') {
    return `
      <tr>
        <td>${row.name}</td>
        <td><span class="badge error">ERROR</span></td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>-</td>
        <td>${row.error ?? 'unknown error'}</td>
      </tr>
    `;
  }

  const fillMark = row.isPartial ? ' <span class="partial">(부분체결)</span>' : '';

  return `
    <tr>
      <td>${row.name}</td>
      <td><span class="badge ok">OK</span></td>
      <td>${fmt(row.allInCostBps, 2)}</td>
      <td>${fmt(row.allInCostPct, 4)}</td>
      <td>${fmt(row.feeBps, 2)}</td>
      <td>${fmt(row.feeQuote, 4)}</td>
      <td>${fmt(row.allInCostQuote, 4)}</td>
      <td>${fmt(row.topPrice, 6)}</td>
      <td>${fmt(row.avgExecutionPrice, 6)}</td>
      <td>${fmt(row.slippageBps, 2)}</td>
      <td>${fmt(row.slippagePct, 4)}</td>
      <td>${fmt(row.filledQty, 6)} / ${fmt(row.requestedQty, 6)}${fillMark}</td>
      <td>${fmt(row.notionalQuote, 2)}</td>
      <td>-</td>
    </tr>
  `;
}

async function loadOnce() {
  if (loading) return;
  loading = true;
  statusText('데이터 수집 중...');

  const coin = els.coin.value;
  const qty = Number(els.qty.value);
  const side = els.side.value;

  if (!Number.isFinite(qty) || qty <= 0) {
    statusText('수량은 0보다 큰 숫자여야 합니다.');
    loading = false;
    return;
  }

  try {
    const res = await fetch(`/api/slippage?coin=${coin}&qty=${qty}&side=${side}`);
    const payload = await res.json();
    if (!res.ok) {
      throw new Error(payload.error || 'request failed');
    }

    els.rows.innerHTML = payload.rows.map(rowHtml).join('');
    els.updatedAt.textContent = new Date(payload.ts).toLocaleString();

    const success = payload.rows.filter((r) => r.status === 'ok').length;
    statusText(`총 ${payload.rows.length}개 거래소 중 ${success}개 정상 응답`);
  } catch (err) {
    statusText(`오류: ${err instanceof Error ? err.message : String(err)}`);
  } finally {
    loading = false;
  }
}

function restartTimer() {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
  if (els.interval.value === 'off') {
    statusText('자동갱신 OFF (수동으로 지금 갱신 버튼 사용)');
    return;
  }
  const interval = Number(els.interval.value);
  timer = setInterval(loadOnce, interval);
}

els.refresh.addEventListener('click', loadOnce);
els.coin.addEventListener('change', loadOnce);
els.side.addEventListener('change', loadOnce);
els.qty.addEventListener('change', loadOnce);
els.interval.addEventListener('change', restartTimer);

restartTimer();
loadOnce();
