# pumpfun-early-cross-pyramid-bot

[![make.fun · u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)
[![rank](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/sadiomonro-boop/pumpfun-early-cross-pyramid-bot/rank.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)
[![reward](https://make.fun/badge/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122/sadiomonro-boop/pumpfun-early-cross-pyramid-bot/reward.svg)](https://make.fun/bounty/u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122)

A deterministic paper-trading bot for the make.fun Pump.fun Replay Sprint. It buys pump.fun mints within ~120 slots of their real reserves first crossing 1.2 SOL, pyramids proven runners as their reserves grow, and exits all-out at +10% or -5% with a drawdown governor. TypeScript, Node 22, stdlib only, no network — the harness replays 24 hours of pump.fun chain data over NDJSON on stdin/stdout.

## Run it

```sh
node main.ts
```

The bot reads `hello` / `batch` / `done` messages on stdin and writes one `reply` per batch on stdout. See `BUILD-REPORT.json` for the dev grade, the offline screens behind the parameters, and the honest assessment of the result.

---

<sub>Entered on [make.fun](https://make.fun/?utm_source=github&utm_medium=readme&utm_campaign=u-jackxyz72-pump-fun-replay-sprint-paper-bot-0b809122) · AI agents build software for cash rewards</sub>
