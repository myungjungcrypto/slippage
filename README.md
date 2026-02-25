# Real-time Slippage Dashboard

거래소별 오더북을 조회해서 시장가 주문의 예상 슬리피지를 비교하는 대시보드입니다.

지원 거래소:
- Binance
- Bybit
- Hyperliquid
- Aster
- OKX
- Lighter
- Variational (공개 quote 메타데이터 기반 추정 체결가 사용)

지원 코인:
- BTC
- ETH
- SOL
- BNB

## 실행

```bash
npm start
```

기본 URL: `http://localhost:8787`

## Vercel 배포 (운영용)

이 프로젝트는 Vercel용 API 라우트가 포함되어 있습니다.

- API: `/api/slippage`
- Dashboard: `/` (내부적으로 `public/index.html`로 rewrite)

절차:

1. GitHub에 `/Users/myunggeunjung/slippage` 저장소 push
2. Vercel에서 해당 저장소 Import
3. Build 설정은 기본값 사용 (Framework preset 없이도 동작)
4. 배포 완료 후 아래 주소 확인
   - `https://<your-domain>/api/slippage?coin=BTC&qty=1&side=buy`

fundingrate_auto 연동:

- `opt_inputs`에 아래 키를 설정
  - `use_live_slippage_api = TRUE`
  - `slippage_api_url = https://<your-domain>/api/slippage`

## 입력값
- 코인 (`BTC/ETH/SOL/BNB`)
- 수량 (베이스 코인 수량)
- 방향 (`buy` or `sell`)
- 갱신 주기 (`OFF` 가능)

## 계산 기준
- `buy`: asks를 위에서부터 consume
- `sell`: bids를 위에서부터 consume
- 슬리피지:
  - buy: `(avgFill - topAsk) / topAsk`
  - sell: `(topBid - avgFill) / topBid`
- 수수료:
  - 거래소별 기본 taker fee(bps) 가정값 사용 (예: Hyperliquid 4.5 bps)
- 올인 비용:
  - `all-in bps = slippage bps + fee bps`
  - `총비용(quote) = 체결 임팩트 비용 + 수수료`
- 정렬:
  - 정상 응답 거래소를 `all-in bps` 오름차순(좋은 조건 순)으로 표 상단에 배치

## 주의
- 각 거래소 API 정책/장애/레이트리밋에 따라 일부 거래소가 일시적으로 에러 상태일 수 있습니다.
- Lighter는 `orderBooks` + `orderBookOrders` 공개 API를 사용합니다.
- Variational은 공개 L2 orderbook 대신 `metadata/stats` quote를 사용해 체결가를 추정하며, quote가 stale이면 `base_spread_bps` 기반으로 보정합니다.
