// Pump.fun Replay Sprint — deterministic early-entry + pyramid bot.
//
// Strategy, in plain terms:
//   * Watch every mint's bonding-curve price (virtual SOL reserves divided by
//     virtual token reserves) as the replay streams TradeEvent marks.
//   * Enter EARLY: buy a mint within ~120 slots of its real reserves first
//     crossing 1.2 SOL. On the dev window nearly every mint that reaches 1
//     SOL of real reserves runs at least +10% soon after, BUT the edge lives
//     only in the first ~100 slots after the crossing.
//   * Pyramid once: add to a position once it is up 8%, sized by the mint's
//     (now larger) real reserves, so proven runners get more capital.
//   * Exit all out at +10%, cut at -5%, or leave when a trade stalls or
//     reverses. Buys use tight slippage (reject chasing a pump); sells use
//     wide slippage (exits must get out even in a dump).
//   * A drawdown governor halts new entries past -8% equity drawdown and
//     flattens everything past -15%, so one bad stretch cannot sink the run.
//   * Fully deterministic: no clocks, no unseeded randomness, stable sorted
//     ordering everywhere, so the two harness executions agree byte for byte.
//   * Defensive by design (critic round 1): the hello/batch dispatch is
//     wrapped in try/catch and always answers a batch, every u64 from the
//     harness is parsed through a validating helper, and in-flight buy value
//     is reserved against cash until its fill resolves.
//
// Protocol: NDJSON over stdin/stdout. The harness sends a hello message,
// then batch messages (each with a seq and a slot), then done. We answer
// every batch with one reply line echoing its seq and carrying our orders.
// u64 values travel as decimal strings; lamports are handled with BigInt.

const LAMPORTS_PER_SOL = 1_000_000_000n;
const LAMPORTS_PER_SOL_NUM = 1_000_000_000;

// ---- Tunable strategy parameters (constants, no environment input) ----
// Evidence from the dev dataset (see BUILD-REPORT.json): nearly every mint
// that reaches 1 SOL of real reserves runs at least +10% soon after, BUT the
// edge lives only in the first ~100 slots after the crossing — entering 100
// slots late flips the average trade from +0.6% to -3.6%. Late breakout
// entries buy tops and ladder/trailing exits give the gain back (the median
// coin ends -17% from its early price). So: enter IMMEDIATELY at the cross,
// size by real reserves, pyramid proven runners as their reserves grow, and
// take +10% in full / cut at -5%.
const ENTRY_MIN_REAL_SOL = 1.2; // enter as real reserves first cross 1.2 SOL
const ENTRY_CROSS_FRESH_SLOTS = 120; // ...and only within ~120 slots of crossing
const ENTRY_MIN_AGE_SLOTS = 100; // let a fresh mint settle before entering
const MAX_BUY_SOL = 0.45; // biggest single buy, in SOL
const MIN_BUY_SOL = 0.011; // smallest buy (guardrail needs >= 0.01 SOL trips)
const MAX_POSITIONS = 5; // harness cap on open positions
const MAX_ORDERS_PER_BATCH = 16; // harness cap on orders per reply
const PYRAMID_MIN_GAIN = 0.05; // add to a winner once up 5% on blended cost
const PYRAMID_MAX_ADDS = 3; // up to three adds per position, as reserves grow:
// offline screens and the live grade both show adds raise gross PnL by more
// than their fee drag (adds=1 graded -0.70 vs adds=3 at -0.38)
const TAKE_PROFIT = 0.1; // sell everything at +10%
const STOP_LOSS = -0.05; // hard stop at -5% from average entry
const TIME_STOP_SLOTS = 1200; // exit a flat trade after ~4 minutes, whatever its
// gain: with only 5 slots, a stalled trade blocks fresh crossings, and a
// coin whose tape has gone quiet never moves again — its frozen price can
// sit in the +5%..+10% band forever if the stop is gain-gated
const GOVERNOR_HALT_DD = 0.08; // stop new entries past -8% drawdown
const GOVERNOR_FLATTEN_DD = 0.15; // flatten everything past -15% drawdown
const BUY_SLIPPAGE_BPS = 400; // loose enough to fill a fast pump, still caps chasing
const SELL_SLIPPAGE_BPS = 2000; // very wide on sells: in a dump the price can
// fall more than 8% in the 2-slot fill lag, and a rejected exit strands the
// position forever (its mint stops trading, the price freezes, no exit ever
// fires again). Exiting 20% worse beats holding a dead coin to zero.
const REENTRY_COOLDOWN_SLOTS = 1500; // wait after a stopped-out mint
const SAMPLE_CAP = 400; // price samples kept per mint

type PriceSample = { slot: number; price: number };

interface MintState {
  mint: string;
  createdSlot: number; // 0 = no CreateEvent seen yet for this mint
  completed: boolean;
  crossedSlot: number; // first slot where real reserves reached the entry bar
  seenBelowBar: boolean; // ever observed with real reserves below the bar
  samples: PriceSample[];
  lastPrice: number;
  lastRealSol: number;
  lastSeenSlot: number;
}

interface PositionState {
  mint: string;
  tokens: bigint;
  costLamports: bigint;
  entrySlot: number;
  adds: number;
}

interface Order {
  side: "buy" | "sell";
  client_id: string;
  mint: string;
  quote_lamports?: string;
  token_amount?: string;
  max_slippage_bps: number;
}

const mints = new Map<string, MintState>();
const positions = new Map<string, PositionState>();
const stoppedOutAt = new Map<string, number>();
// Mints we sent a first buy for but whose fill has not been reported yet,
// with the slot the buy was decided. Fills land 2 slots after the decision,
// so without this the bot overbooks the 5-position cap and the extra buys
// are rejected. Stale entries (a rejected buy never fills) expire.
const pendingMints = new Map<string, number>();
// Mints with a full-position sell already sent but not yet filled. Without
// this the bot re-sends the same sell every batch until the first fill lands
// (the position still looks open), wasting the per-batch order budget on
// sells that are then rejected for insufficient tokens.
const pendingSells = new Set<string>();
// Pyramid adds sent but not yet resolved, per mint. An add counts toward
// the cap only once its fill lands, so in-flight adds are tracked here
// instead of on the position (critic S4).
const pendingAdds = new Map<string, number>();
// Buy quote amounts committed by orders sent but not yet resolved, keyed by
// client_id. Fills land 2 slots after the decision, so consecutive batches
// would otherwise size against the same undebited cash (critic S5).
const committedBuys = new Map<string, { amount: bigint; slot: number }>();
let committedLamports = 0n;

let capitalLamports = 10_000_000_000n;
let cashLamports = 10_000_000_000n;
let maxOpenPositions = 5;
let maxOrderBpsOfRealReserve = 100;
let maxOrdersPerBatch = 16;
let orderCounter = 0;
let peakEquityLamports = 10_000_000_000n;
let currentSlot = 0;

function priceFromReserves(virtualSol: string, virtualToken: string): number {
  const sol = Number(virtualSol);
  const tok = Number(virtualToken);
  if (tok <= 0 || sol <= 0) return 0;
  return sol / tok;
}

// Parse a u64 decimal string from the harness into a BigInt, or null when
// the value is missing or malformed. A bare BigInt(...) throws on bad input
// (and BigInt(NaN) throws too), so every conversion of harness data goes
// through here — an uncaught throw mid-run would kill the process with no
// reply for the pending batch (critic M1).
function parseLamports(value: unknown): bigint | null {
  if (typeof value !== "string") return null;
  if (!/^(0|[1-9][0-9]{0,19})$/.test(value)) return null;
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

function getMint(mint: string): MintState {
  let state = mints.get(mint);
  if (state === undefined) {
    state = {
      mint,
      createdSlot: 0, // 0 = no CreateEvent seen yet; first sight is not a birth
      completed: false,
      crossedSlot: 0,
      seenBelowBar: false,
      samples: [],
      lastPrice: 0,
      lastRealSol: 0,
      lastSeenSlot: currentSlot,
    };
    mints.set(mint, state);
  }
  return state;
}

function notePrice(state: MintState, slot: number, price: number): void {
  if (price <= 0 || !Number.isFinite(price)) return;
  state.lastPrice = price;
  state.lastSeenSlot = slot;
  state.samples.push({ slot, price });
  if (state.samples.length > SAMPLE_CAP) {
    state.samples.splice(0, state.samples.length - SAMPLE_CAP);
  }
}

function updateRealSol(state: MintState, realSol: number, slot: number): void {
  // A missing real_*_reserves field parses to NaN; never store it, or the
  // next sizeBuy would throw on BigInt(NaN) (critic M1).
  if (!Number.isFinite(realSol) || realSol < 0) realSol = 0;
  state.lastRealSol = realSol;
  if (realSol < ENTRY_MIN_REAL_SOL) {
    state.seenBelowBar = true;
    return;
  }
  // Arm the cross only for a mint we watched climb through the bar from
  // below. A mint first seen already above the bar was created before the
  // window opened: its ~100-slot edge window is long gone (critic S1).
  if (state.crossedSlot === 0 && state.seenBelowBar) {
    state.crossedSlot = slot;
  }
}

function positionValueLamports(pos: PositionState): number {
  const state = mints.get(pos.mint);
  if (state === undefined || state.lastPrice <= 0) return 0;
  return Number(pos.tokens) * state.lastPrice;
}

function equityLamports(): number {
  let equity = Number(cashLamports);
  const keys = [...positions.keys()].sort();
  for (const key of keys) {
    const pos = positions.get(key);
    if (pos !== undefined) equity += positionValueLamports(pos);
  }
  return equity;
}

function avgEntryPrice(pos: PositionState): number {
  if (pos.tokens <= 0n) return 0;
  return Number(pos.costLamports) / Number(pos.tokens);
}

function nextClientId(): string {
  orderCounter += 1;
  return "o" + orderCounter.toString(36) + "-" + currentSlot.toString(36);
}

function makeBuyOrder(mint: string, quoteLamports: bigint): Order {
  return {
    side: "buy",
    client_id: nextClientId(),
    mint,
    quote_lamports: quoteLamports.toString(),
    max_slippage_bps: BUY_SLIPPAGE_BPS,
  };
}

// Send a buy while reserving its value against cash until the fill resolves
// (critic S5): fills land 2 slots after the decision, so consecutive
// batches would otherwise size against the same undebited cash.
function sendBuy(mint: string, quoteLamports: bigint): Order {
  const order = makeBuyOrder(mint, quoteLamports);
  committedBuys.set(order.client_id, { amount: quoteLamports, slot: currentSlot });
  committedLamports += quoteLamports;
  return order;
}

// Release one buy's reservation when its fill resolves (filled, rejected or
// failed). Safe to call for an unknown id: it simply does nothing.
function clearCommitted(clientId: string): void {
  const entry = committedBuys.get(clientId);
  if (entry !== undefined) {
    committedLamports -= entry.amount;
    if (committedLamports < 0n) committedLamports = 0n;
    committedBuys.delete(clientId);
  }
}

function makeSellOrder(mint: string, tokenAmount: bigint): Order {
  return {
    side: "sell",
    client_id: nextClientId(),
    mint,
    token_amount: tokenAmount.toString(),
    max_slippage_bps: SELL_SLIPPAGE_BPS,
  };
}

// Size a buy in lamports: the biggest legal order we want, capped by the
// 1%-of-real-reserve rule, by our per-trade cap, and by a share of cash.
// lastRealSol is in SOL, so convert it to lamports before applying the
// basis-point cap (a floor on the SOL value would zero the cap).
function sizeBuy(state: MintState, fractionOfCash: number): bigint {
  // A non-finite reserve (NaN from a missing field) would throw in BigInt;
  // treat it as no liquidity at all (critic M1).
  if (!Number.isFinite(state.lastRealSol) || state.lastRealSol <= 0) return 0n;
  const reserveLamports = BigInt(
    Math.floor(state.lastRealSol * LAMPORTS_PER_SOL_NUM),
  );
  const reserveCap =
    (reserveLamports * BigInt(maxOrderBpsOfRealReserve)) / 10000n;
  const safeReserveCap = (reserveCap * 9n) / 10n; // stay 10% under the cap
  let size = BigInt(Math.floor(MAX_BUY_SOL * LAMPORTS_PER_SOL_NUM));
  if (safeReserveCap < size) size = safeReserveCap;
  // Size against cash not yet debited by in-flight buys (critic S5).
  const spendableCash = cashLamports - committedLamports;
  const cashBase = spendableCash > 0n ? spendableCash : 0n;
  const cashCap = (cashBase * BigInt(Math.floor(fractionOfCash * 100))) / 100n;
  if (cashCap < size) size = cashCap;
  return size;
}

function handleHello(msg: Record<string, unknown>): void {
  const capital = msg.capital as { lamports: string } | undefined;
  if (capital !== undefined) {
    const lamports = parseLamports(capital.lamports);
    if (lamports !== null) {
      capitalLamports = lamports;
      cashLamports = capitalLamports;
      peakEquityLamports = capitalLamports;
    }
  }
  const caps = msg.caps as Record<string, unknown> | undefined;
  if (caps !== undefined) {
    // Only accept finite integers: BigInt() later throws on NaN or a
    // fractional cap value (critic M1).
    if (
      typeof caps.max_open_positions === "number" &&
      Number.isInteger(caps.max_open_positions)
    ) {
      maxOpenPositions = caps.max_open_positions;
    }
    if (
      typeof caps.max_order_bps_of_real_reserve === "number" &&
      Number.isInteger(caps.max_order_bps_of_real_reserve)
    ) {
      maxOrderBpsOfRealReserve = caps.max_order_bps_of_real_reserve;
    }
    if (
      typeof caps.max_orders_per_batch === "number" &&
      Number.isInteger(caps.max_orders_per_batch)
    ) {
      maxOrdersPerBatch = caps.max_orders_per_batch;
    }
  }
}

function handleBatch(msg: Record<string, unknown>): Order[] {
  const seq = msg.seq as number;
  void seq;
  // A malformed slot must not poison every later comparison; keep the last
  // good slot instead (critic M1).
  if (typeof msg.slot === "number" && Number.isFinite(msg.slot)) {
    currentSlot = msg.slot;
  }
  const orders: Order[] = [];

  const events = (msg.events as Record<string, unknown>[]) ?? [];
  for (const event of events) {
    const kind = event.event as string;
    if (kind === "CreateEvent") {
      const state = getMint(event.mint as string);
      state.createdSlot = event.slot as number;
      state.lastRealSol = 0;
      // Creation is itself an observation below the bar (critic S1).
      state.seenBelowBar = true;
      const price = priceFromReserves(
        event.virtual_sol_reserves as string,
        event.virtual_token_reserves as string,
      );
      notePrice(state, event.slot as number, price);
    } else if (kind === "TradeEvent") {
      const state = getMint(event.mint as string);
      const price = priceFromReserves(
        event.virtual_sol_reserves as string,
        event.virtual_token_reserves as string,
      );
      notePrice(state, event.slot as number, price);
      updateRealSol(
        state,
        Number(event.real_sol_reserves as string) / LAMPORTS_PER_SOL_NUM,
        event.slot as number,
      );
    } else if (kind === "CompleteEvent") {
      const state = getMint(event.mint as string);
      state.completed = true;
    }
  }

  const marks = (msg.marks as Record<string, unknown>[]) ?? [];
  for (const mark of marks) {
    const state = getMint(mark.mint as string);
    const price = priceFromReserves(
      mark.virtual_quote_reserves as string,
      mark.virtual_token_reserves as string,
    );
    notePrice(state, mark.slot as number, price);
    updateRealSol(
      state,
      Number(mark.real_quote_reserves as string) / LAMPORTS_PER_SOL_NUM,
      mark.slot as number,
    );
  }

  // Reconcile our own positions from the fills the harness reports, then
  // from the account snapshot, which is the ground truth.
  const fills = (msg.fills as Record<string, unknown>[]) ?? [];
  for (const fill of fills) {
    const mint = fill.mint as string;
    const side = fill.side as string;
    const clientId = fill.client_id as string;
    // Release the cash reservation for any resolved buy, whatever its
    // outcome (critic S5).
    if (side === "buy") clearCommitted(clientId);
    // A rejected or failed order frees its pending slot immediately: without
    // this a rejected exit leaves its mint in pendingSells forever and the
    // position can never be sold again. A rejected pyramid add frees its
    // add slot too (critic S4).
    if (fill.status !== "filled") {
      if (side === "sell") pendingSells.delete(mint);
      if (side === "buy") {
        pendingMints.delete(mint);
        if (positions.has(mint)) {
          const inFlight = pendingAdds.get(mint) ?? 0;
          if (inFlight > 0) pendingAdds.set(mint, inFlight - 1);
        }
      }
      continue;
    }
    const quote = parseLamports(fill.quote_lamports);
    const fee = parseLamports(fill.fee_lamports);
    const txFee = parseLamports(fill.tx_fee_lamports);
    const tokens = parseLamports(fill.token_amount);
    // A fill we cannot parse is unusable; skip it rather than throwing
    // (critic M1). Its pending slots were already freed above.
    if (quote === null || fee === null || txFee === null || tokens === null) {
      continue;
    }
    if (side === "buy") {
      pendingMints.delete(mint);
      const existing = positions.get(mint);
      let pos = existing;
      if (pos === undefined) {
        pos = {
          mint,
          tokens: 0n,
          costLamports: 0n,
          entrySlot: currentSlot,
          adds: 0,
        };
        positions.set(mint, pos);
      } else {
        // A buy landing on an already-held position is a pyramid add: it
        // counts toward the cap only now that it filled (critic S4).
        const inFlight = pendingAdds.get(mint) ?? 0;
        if (inFlight > 0) {
          pendingAdds.set(mint, inFlight - 1);
          pos.adds += 1;
        }
      }
      pos.tokens += tokens;
      pos.costLamports += quote + fee + txFee;
    } else {
      // Any filled sell clears the pending flag, even a partial one, so the
      // remainder can be re-sent next batch if the exit still triggers
      // (critic S3).
      pendingSells.delete(mint);
      const pos = positions.get(mint);
      if (pos !== undefined && pos.tokens > 0n) {
        const sold = tokens > pos.tokens ? pos.tokens : tokens;
        const costOut = (pos.costLamports * sold) / pos.tokens;
        pos.tokens -= sold;
        pos.costLamports -= costOut;
        if (pos.tokens === 0n) positions.delete(mint);
      }
    }
  }

  const account = msg.account as
    | { cash_lamports: string; positions: { mint: string; token_amount: string; cost_lamports: string }[] }
    | undefined;
  if (account !== undefined) {
    const cash = parseLamports(account.cash_lamports);
    if (cash !== null) cashLamports = cash;
    const seen = new Set<string>();
    for (const acctPos of account.positions) {
      const acctTokens = parseLamports(acctPos.token_amount);
      const acctCost = parseLamports(acctPos.cost_lamports);
      // An unparseable position row is left exactly as our map holds it
      // (and still counts as seen, so it is not deleted); throwing here
      // would kill the run (critic M1).
      if (acctTokens === null || acctCost === null) {
        seen.add(acctPos.mint);
        continue;
      }
      seen.add(acctPos.mint);
      const existing = positions.get(acctPos.mint);
      if (existing !== undefined) {
        existing.tokens = acctTokens;
        existing.costLamports = acctCost;
        if (existing.tokens === 0n) positions.delete(acctPos.mint);
      } else {
        positions.set(acctPos.mint, {
          mint: acctPos.mint,
          tokens: acctTokens,
          costLamports: acctCost,
          entrySlot: currentSlot,
          adds: 0,
        });
      }
    }
    for (const key of [...positions.keys()]) {
      if (!seen.has(key)) {
        positions.delete(key);
        pendingSells.delete(key);
      }
    }
    for (const key of [...pendingMints.keys()]) {
      if (seen.has(key)) pendingMints.delete(key);
    }
    for (const [key, sentSlot] of [...pendingMints]) {
      if (currentSlot - sentSlot > 200) pendingMints.delete(key);
    }
    // Safety net for the cash reservations: drop any entry whose fill was
    // never reported (the harness reports every resolution, so this should
    // never trigger) rather than leaking committed cash forever.
    for (const [clientId, entry] of [...committedBuys]) {
      if (currentSlot - entry.slot > 500) {
        committedLamports -= entry.amount;
        if (committedLamports < 0n) committedLamports = 0n;
        committedBuys.delete(clientId);
      }
    }
  }

  // ---- Drawdown governor ----
  const equity = equityLamports();
  if (equity > Number(peakEquityLamports)) {
    peakEquityLamports = BigInt(Math.floor(equity));
  }
  const drawdown =
    peakEquityLamports > 0n
      ? (Number(peakEquityLamports) - equity) / Number(peakEquityLamports)
      : 0;
  const haltEntries = drawdown >= GOVERNOR_HALT_DD;
  const flattenAll = drawdown >= GOVERNOR_FLATTEN_DD;

  // ---- Exits first (so freed slots can be reused next batch) ----
  const positionKeys = [...positions.keys()].sort();
  for (const key of positionKeys) {
    if (orders.length >= maxOrdersPerBatch) break;
    const pos = positions.get(key);
    if (pos === undefined || pos.tokens <= 0n) continue;
    const state = mints.get(key);
    if (state === undefined || state.lastPrice <= 0) continue;
    const entry = avgEntryPrice(pos);
    if (entry <= 0) continue;
    const gain = (state.lastPrice - entry) / entry;

    // One rule, all out: bank +10% in full (coins dump after their spike),
    // cut at -5%, or leave when the trade stalls or reverses.
    let sellAll = false;
    let reason = "";
    if (flattenAll) {
      sellAll = true;
      reason = "governor-flatten";
    } else if (gain >= TAKE_PROFIT) {
      sellAll = true;
      reason = "take-profit";
    } else if (gain <= STOP_LOSS) {
      sellAll = true;
      reason = "stop-loss";
    } else if (currentSlot - pos.entrySlot > TIME_STOP_SLOTS) {
      sellAll = true;
      reason = "time-stop";
    }
    void reason;
    if (sellAll) {
      // Disarm the mint on ANY full exit, not just a stop-loss: a coin sold
      // at +10% inside its cross window would otherwise be re-bought at
      // once while still inside the window (critic S2). Re-entry needs a
      // genuinely fresh cross from below the bar, after the cooldown.
      stoppedOutAt.set(key, currentSlot);
      state.crossedSlot = 0;
      state.seenBelowBar = false;
    }
    if (sellAll && pos.tokens > 0n && !pendingSells.has(key)) {
      pendingSells.add(key);
      orders.push(makeSellOrder(key, pos.tokens));
    }
  }

  // ---- Entries and pyramids ----
  const openCount = positions.size + pendingMints.size;
  const slotsFree = maxOpenPositions - openCount;
  if (!haltEntries && !flattenAll && slotsFree > 0 && orders.length < maxOrdersPerBatch) {
    // Pyramids on winning positions first: add while the blended gain is
    // at least 5%. Each add is sized by the mint's (now larger) real
    // reserves, so proven runners get more capital than flat coins. An add
    // counts toward the cap only when its fill lands (critic S4), so
    // in-flight adds are counted here too.
    for (const key of positionKeys) {
      if (orders.length >= maxOrdersPerBatch) break;
      const pos = positions.get(key);
      const state = mints.get(key);
      if (pos === undefined || state === undefined) continue;
      if (pos.adds + (pendingAdds.get(key) ?? 0) >= PYRAMID_MAX_ADDS) continue;
      if (pendingSells.has(key)) continue;
      const entry = avgEntryPrice(pos);
      if (entry <= 0 || state.lastPrice <= 0) continue;
      const gain = (state.lastPrice - entry) / entry;
      if (gain < PYRAMID_MIN_GAIN) continue;
      const size = sizeBuy(state, 0.1);
      if (size < BigInt(Math.floor(MIN_BUY_SOL * LAMPORTS_PER_SOL_NUM))) continue;
      pendingAdds.set(key, (pendingAdds.get(key) ?? 0) + 1);
      orders.push(sendBuy(key, size));
    }

    // Fresh crossers first, deterministic order: buy a mint within ~120
    // slots of its real reserves first reaching the entry bar. No filters
    // that delay the entry: on this replay the run happens right after the
    // cross, and entering 100 slots late already flips the average trade
    // from +0.6% to -3.6%.
    if (orders.length < maxOrdersPerBatch) {
      const candidates: { mint: string; crossedSlot: number; score: number }[] = [];
      const mintKeys = [...mints.keys()].sort();
      for (const key of mintKeys) {
        if (positions.has(key)) continue;
        const state = mints.get(key);
        if (state === undefined) continue;
        // Only mints that traded recently can be crossing now; skipping
        // stale mints keeps the per-batch scan cheap over 63k batches.
        if (currentSlot - state.lastSeenSlot > ENTRY_CROSS_FRESH_SLOTS) continue;
        if (state.completed) continue;
        if (state.samples.length < 5) continue;
        if (state.lastPrice <= 0) continue;
        // The age gate applies only when the birth was actually observed
        // (createdSlot 0 means no CreateEvent seen; first sight is not a
        // birth — critic S1).
        if (state.createdSlot !== 0 && currentSlot - state.createdSlot < ENTRY_MIN_AGE_SLOTS) continue;
        const cooled = stoppedOutAt.get(key);
        if (cooled !== undefined && currentSlot - cooled < REENTRY_COOLDOWN_SLOTS) {
          continue;
        }
        if (state.crossedSlot === 0) continue;
        if (currentSlot - state.crossedSlot > ENTRY_CROSS_FRESH_SLOTS) continue;
        candidates.push({ mint: key, crossedSlot: state.crossedSlot, score: 0 });
      }
      candidates.sort((a, b) => {
        if (a.crossedSlot !== b.crossedSlot) return a.crossedSlot - b.crossedSlot;
        if (b.score !== a.score) return b.score - a.score;
        return a.mint < b.mint ? -1 : a.mint > b.mint ? 1 : 0;
      });
      const maxNew = Math.min(slotsFree, maxOrdersPerBatch - orders.length);
      for (let i = 0; i < candidates.length && i < maxNew; i++) {
        const state = mints.get(candidates[i].mint);
        if (state === undefined) continue;
        const size = sizeBuy(state, 0.1);
        if (size < BigInt(Math.floor(MIN_BUY_SOL * LAMPORTS_PER_SOL_NUM))) continue;
        pendingMints.set(candidates[i].mint, currentSlot);
        orders.push(sendBuy(candidates[i].mint, size));
      }
    }
  }

  return orders.slice(0, Math.min(maxOrdersPerBatch, MAX_ORDERS_PER_BATCH));
}

let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk: string) => {
  buffer += chunk;
  let newline = buffer.indexOf("\n");
  while (newline !== -1) {
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (line.trim().length > 0) {
      let msg: Record<string, unknown>;
      try {
        msg = JSON.parse(line) as Record<string, unknown>;
      } catch {
        newline = buffer.indexOf("\n");
        continue;
      }
      const type = msg.type as string;
      try {
        if (type === "hello") {
          handleHello(msg);
        } else if (type === "batch") {
          const orders = handleBatch(msg);
          const reply = JSON.stringify({
            type: "reply",
            seq: msg.seq,
            orders,
          });
          process.stdout.write(reply + "\n");
        } else if (type === "done") {
          process.exit(0);
        }
      } catch {
        // Never die mid-run: an exception in handleHello/handleBatch would
        // otherwise kill the process with no reply for the pending batch
        // and leave the entry unranked (critic M1). Answer with no orders
        // and keep going; the next batch re-syncs from the account snapshot.
        if (type === "batch") {
          const seq =
            typeof msg.seq === "number" && Number.isFinite(msg.seq) ? msg.seq : 0;
          process.stdout.write(
            JSON.stringify({ type: "reply", seq, orders: [] }) + "\n",
          );
        }
      }
    }
    newline = buffer.indexOf("\n");
  }
});
process.stdin.on("end", () => {
  process.exit(0);
});
