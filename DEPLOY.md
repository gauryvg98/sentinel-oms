# Deploying Sentinel OMS to Fly.io

One always-on machine + one small Postgres, all on Fly. The public URL is a
**read-only live demo**; you unlock the controls in your own browser with an
admin token. Real capital trades on your existing Binance-futures keys.

> You run these commands (I can't — no Fly auth here). Copy-paste top to bottom.
> Anything in `<angle brackets>` is yours to fill in.

---

## 0. One-time prerequisites

```bash
# Fly CLI
brew install flyctl        # or: curl -L https://fly.io/install.sh | sh
fly auth login
```

## 1. Create the app (don't deploy yet)

```bash
cd /path/to/sentinel-oms
fly launch --no-deploy --copy-config --name sentinel-oms --region sin
```

`--copy-config` keeps the committed `fly.toml`. If the name `sentinel-oms` is
taken, pick another and update `app = "..."` in `fly.toml` to match.

## 2. Create Postgres (the ~$3/mo unmanaged one) and attach it

```bash
# Single-node unmanaged Postgres — the cheap one. NOT "Managed Postgres" ($30+).
fly postgres create --name sentinel-db --region sin \
  --vm-size shared-cpu-1x --volume-size 1 --initial-cluster-size 1

# Attach → this sets the DATABASE_URL secret on the app automatically.
fly postgres attach sentinel-db --app sentinel-oms
```

`attach` prints the DSN and stores it as the `DATABASE_URL` secret. asyncpg
accepts the `postgres://` scheme it uses as-is. The app runs its migrations on
boot, so there's no separate migrate step.

## 3. Set the secrets (never commit these)

```bash
# Generate a strong admin token and REMEMBER it — it's your unlock key.
ADMIN_TOKEN=$(openssl rand -hex 24)
echo "ADMIN TOKEN (save this): $ADMIN_TOKEN"

fly secrets set --app sentinel-oms \
  SENTINEL_ADMIN_TOKEN="$ADMIN_TOKEN" \
  BINANCE_FUTURES_KEY="<your-futures-key>" \
  BINANCE_FUTURES_SECRET="<your-futures-secret>"
```

Everything non-secret (symbols, strategy, leverage, risk, read-only flag) is
already in `fly.toml [env]`. `DATABASE_URL` came from step 2.

## 4. Deploy

```bash
fly deploy
```

The `[deploy] strategy = "immediate"` in `fly.toml` recreates the single machine
in place, so two writers never overlap. First boot runs migrations, claims the
account advisory lock, then seeds the bot roster in the background.

## 5. Confirm exactly one machine, then open it

```bash
fly status                    # must show ONE machine, state "started"
fly logs                      # watch it boot: migrations → lock → roster
fly open                      # opens the public read-only URL
```

If `fly status` ever shows two machines, **scale back to one immediately** — the
single-writer model requires it:

```bash
fly scale count 1
```

## 6. Unlock the controls (admin, your browser only)

Visit once:

```
https://sentinel-oms.fly.dev/admin?token=<the ADMIN_TOKEN from step 3>
```

It drops an http-only cookie and redirects to `/`. The "+ add instrument",
start/stop, size, strategy and timeframe controls now work **in that browser**.
Everyone else sees `read-only · live demo` with the controls hidden, and any
write POST they attempt returns `403`.

---

## Operating notes

- **Halt is not a crash.** The health check (`GET /whoami`) only proves the
  process is up. If the OMS halts on a genuine divergence it stays up and
  refuses to trade — by design. Investigate with `fly logs`; don't just restart
  (a restart won't clear a real ledger/broker disagreement).
- **Restart / stop:**
  ```bash
  fly apps restart sentinel-oms      # rolling recreate of the one machine
  fly scale count 0                  # stop trading (pause)
  fly scale count 1                  # resume
  ```
- **Change trading config** (symbols, leverage, risk): edit `fly.toml [env]`
  and `fly deploy`, or `fly secrets set` for anything sensitive.
- **Rotate the admin token:** `fly secrets set SENTINEL_ADMIN_TOKEN=$(openssl rand -hex 24)`
  then re-visit `/admin?token=...` with the new value.
- **Memory:** starts at 512 MB. If it OOMs with many bots, bump `[[vm]] memory`
  to `1024` and redeploy.

## Security reminders

- The Binance-futures keys and the CockroachDB prod password were pasted in chat
  earlier — **rotate both** (we're not using Cockroach here; Fly Postgres from
  step 2 replaces it).
- Restrict the Binance API key to **futures trading only, no withdrawals**, and
  IP-allowlist Fly's egress if you want belt-and-suspenders.
- Secrets live only in `fly secrets` (encrypted at rest), never in git. `.env`
  stays gitignored; `fly.toml` holds only non-secret config.
