// conpty-host.mjs - the Windows terminal host. Drop-in replacement for tmux-host.mjs that talks to
// supermux-session-host (server/src/bin/session-host.rs) over its host_rpc named-pipe protocol
// instead of `tmux -CC`. It satisfies the SAME interface tmux-host exposes (see tmux-host.d.mts:
// TmuxHost) so terminal-manager.mjs / agent-runtime.mjs are unchanged. Why this backend:
//   * terminals SURVIVE a BlitzOS restart (the session-host is a long-lived per-user process, like a
//     tmux server; ListSessions re-adopts its windows via adoptExisting below),
//   * NO native addon (host_rpc is length-prefixed JSON over a Windows named pipe, the exact node:net
//     transport the computer-use helper port already uses), and
//   * the agent gets a real ConPTY TTY (the host owns the ConPTY + a vt100 grid), which claude/codex
//     need to render their TUI.
//
// Two planes (host_rpc.rs):
//   * CONTROL: length-prefixed (u32 LE len + serde_json) Request/Response over
//     \\.\pipe\supermux-host-<host_id>-v1. serde externally-tags enums: a request is
//     {"SpawnSession":{...}} (or the bare string "ListSessions"); a response is "Unit" | {"Bool":b} |
//     {"OptU32":n|null} | {"Text":s} | {"Sessions":[...]} | {"Err":s}. There are NO request ids, so
//     this client SERIALIZES calls (one in flight) and matches replies FIFO (host_client.rs's Mutex).
//   * STREAM: after a Subscribe{name,token} ack, the host serves RAW ConPTY bytes on a SEPARATE pipe
//     \\.\pipe\supermux-stream-<token> (host_server.rs spawn_stream) until the session ends. That feeds
//     onData + the scrollback ring (the M-c stream plane below).
//   * handshake Hello{protocol_version,client} -> HelloAck{protocol_version,host_pid,accepted,reason}.
import net from 'node:net'
import { randomUUID } from 'node:crypto'
import { StringDecoder } from 'node:string_decoder'
import { spawn as cpSpawn } from 'node:child_process' // aliased: createConptyHost has its own spawn() method
import { existsSync } from 'node:fs'
import { join } from 'node:path'

const PROTOCOL_VERSION = 1
const MAX_FRAME = 16 * 1024 * 1024
// Default capture depth + how often the background poll refreshes capture()/checks PaneDead. capture()
// must be SYNCHRONOUS (the wake watchdog calls it bare, terminal-manager.mjs:327), but a host_rpc
// CapturePane is an async round-trip, so a poll keeps a per-session cache fresh enough to diff across
// the watchdog's multi-second settle window. The poll also corroborates exit via PaneDead (the stream
// EOF is the primary exit signal; PaneDead is the backstop if the stream never connected).
const CAPTURE_LINES = 200
const POLL_MS = 1000
// Scrollback ring cap (matches tmux-host.mjs): the live stream fills the ring, capped so a long-lived
// agent cannot grow it unbounded. onData replays the ring to a late subscriber (the renderer xterm).
const SCROLLBACK_BYTES = 256 * 1024

/** host_id namespaces the control pipe: $SUPERMUX_HOST_ID -> $USERNAME -> "local". MUST match the
 *  host's resolve_host_id() (host_rpc.rs:52) or the pipe name will not match. cfg.hostId overrides. */
export function resolveHostId(cfg = {}) {
  const pick = (v) => (typeof v === 'string' && v.trim() ? v.trim() : null)
  return pick(cfg.hostId) || pick(process.env.SUPERMUX_HOST_ID) || pick(process.env.USERNAME) || 'local'
}
export const controlPipeName = (hostId) => `\\\\.\\pipe\\supermux-host-${hostId}-v${PROTOCOL_VERSION}`
// Per-subscription RAW byte stream pipe (host_rpc.rs:44). The host serves this after a Subscribe ack.
const streamPipeName = (token) => `\\\\.\\pipe\\supermux-stream-${token}`

/** Locate the session-host binary to auto-spawn when none is running: BLITZ_SESSION_HOST_BIN wins (dev
 *  override), then the packaged Resources/bin/session-host.exe (electron-builder extraResources). null =
 *  nothing to spawn, so start() degrades (like tmux-host with no tmux binary). Cached. */
let resolvedHostBin
function resolveSessionHostBin() {
  if (resolvedHostBin !== undefined) return resolvedHostBin
  const cands = [
    process.env.BLITZ_SESSION_HOST_BIN,
    typeof process.resourcesPath === 'string' ? join(process.resourcesPath, 'bin', 'session-host.exe') : null
  ].filter(Boolean)
  resolvedHostBin = cands.find((p) => { try { return existsSync(p) } catch { return false } }) || null
  return resolvedHostBin
}

// ---- the RPC client: one control connection, FIFO request/response, reconnect-once on IO error ----
class HostRpc {
  constructor(pipeName, clientName) {
    this.pipeName = pipeName
    this.clientName = clientName
    this.sock = null
    this.buf = Buffer.alloc(0)
    this.pending = [] // FIFO of { resolve, reject } awaiting the next frame, in send order
    this.chain = Promise.resolve() // serializes request() so only one round-trip is in flight
    this.hostPid = null
  }

  _wireReader(sock) {
    sock.on('data', (d) => {
      this.buf = Buffer.concat([this.buf, d])
      for (;;) {
        if (this.buf.length < 4) break
        const len = this.buf.readUInt32LE(0)
        if (len > MAX_FRAME) { this._fail(new Error(`control frame ${len} exceeds MAX_FRAME`)); return }
        if (this.buf.length < 4 + len) break
        const body = this.buf.subarray(4, 4 + len)
        this.buf = this.buf.subarray(4 + len)
        let obj
        try { obj = JSON.parse(body.toString('utf8')) } catch (e) { this._fail(e); return }
        const w = this.pending.shift()
        if (w) w.resolve(obj)
      }
    })
    const die = (e) => this._fail(e || new Error('control pipe closed'))
    sock.on('error', die)
    sock.on('close', die)
  }

  // Tear down the connection and reject everything in flight so request() can retry on a fresh pipe.
  _fail(err) {
    const s = this.sock
    this.sock = null
    this.buf = Buffer.alloc(0)
    const waiters = this.pending; this.pending = []
    for (const w of waiters) { try { w.reject(err) } catch { /* ignore */ } }
    if (s) { try { s.destroy() } catch { /* ignore */ } }
  }

  _writeFrame(obj) {
    const body = Buffer.from(JSON.stringify(obj), 'utf8')
    const len = Buffer.alloc(4); len.writeUInt32LE(body.length, 0)
    this.sock.write(Buffer.concat([len, body]))
  }

  // Push the waiter BEFORE writing so a reply that races back still finds its slot (FIFO order holds).
  _exchange(obj) {
    return new Promise((resolve, reject) => {
      if (!this.sock) { reject(new Error('not connected')); return }
      this.pending.push({ resolve, reject })
      try { this._writeFrame(obj) } catch (e) { reject(e) }
    })
  }

  async _connect() {
    if (this.sock) return
    const sock = await new Promise((resolve, reject) => {
      const s = net.connect(this.pipeName, () => resolve(s))
      s.once('error', reject)
    })
    this.sock = sock
    this._wireReader(sock)
    this._writeFrame({ protocol_version: PROTOCOL_VERSION, client: this.clientName })
    const ack = await new Promise((resolve, reject) => { this.pending.push({ resolve, reject }) })
    if (!ack || ack.accepted !== true) {
      const reason = (ack && ack.reason) || 'unknown'
      this._fail(new Error(`session-host rejected handshake: ${reason}`))
      throw new Error(`session-host handshake not accepted (${reason})`)
    }
    this.hostPid = ack.host_pid ?? null
  }

  // One request, serialized after all prior requests; reconnect+retry ONCE on a pipe IO error
  // (the host may have restarted out from under a cached connection; mirrors host_client.rs:66).
  request(obj) {
    const run = async () => {
      let lastErr
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          await this._connect()
          return await this._exchange(obj)
        } catch (e) {
          lastErr = e
          this._fail(e)
        }
      }
      throw lastErr || new Error('host_rpc request failed')
    }
    const result = this.chain.then(run, run)
    // Keep the chain alive regardless of this call's outcome so one failure cannot wedge the queue.
    this.chain = result.then(() => {}, () => {})
    return result
  }

  close() { this._fail(new Error('client closed')) }
}

// Unwrap a host_rpc Response, throwing on {"Err":...} or an unexpected variant.
const asUnit = (r) => { if (r === 'Unit') return; if (r && r.Err != null) throw new Error(r.Err); throw new Error(`unexpected response: ${JSON.stringify(r)}`) }
const asBool = (r) => { if (r && typeof r.Bool === 'boolean') return r.Bool; if (r && r.Err != null) throw new Error(r.Err); throw new Error(`expected Bool: ${JSON.stringify(r)}`) }
const asOptU32 = (r) => { if (r && 'OptU32' in r) return r.OptU32; if (r && r.Err != null) throw new Error(r.Err); throw new Error(`expected OptU32: ${JSON.stringify(r)}`) }
const asText = (r) => { if (r && typeof r.Text === 'string') return r.Text; if (r && r.Err != null) throw new Error(r.Err); throw new Error(`expected Text: ${JSON.stringify(r)}`) }
const asSessions = (r) => { if (r && Array.isArray(r.Sessions)) return r.Sessions; if (r && r.Err != null) throw new Error(r.Err); throw new Error(`expected Sessions: ${JSON.stringify(r)}`) }

/**
 * Windows terminal host over supermux-session-host's host_rpc.
 * @param {{ socketPath?:string, hostId?:string, cols?:number, rows?:number, defaultShell?:string }} cfg
 *   socketPath is accepted for tmux-host config parity but ignored (the pipe name is derived from hostId).
 * @returns {import('./tmux-host.d.mts').TmuxHost}
 */
export function createConptyHost(cfg = {}) {
  const HOST_ID = resolveHostId(cfg)
  const PIPE = controlPipeName(HOST_ID)
  const DEF_COLS = cfg.cols || 120
  const DEF_ROWS = cfg.rows || 40
  // A bare terminal (no agent command) runs the default Windows shell, mirroring tmux opening the
  // login shell when new-window gets no command. Agents always pass opts.command.
  const DEF_SHELL = cfg.defaultShell || process.env.ComSpec || 'cmd.exe'

  const rpc = new HostRpc(PIPE, 'blitzos-conpty-host')
  const terminals = new Map() // id -> rec
  let ready = null
  let pollTimer = null

  // --- local rec: answers the SYNCHRONOUS interface methods without a round-trip ---
  const mkRec = (id, opts = {}) => ({
    id,
    pid: null,
    // window/pane have no analogue here (no tmux); keep the TmuxSessionInfo shape with a stable
    // synthetic target so any consumer reading info().window/.pane gets a non-null string.
    window: `supermux-${id}`,
    pane: `supermux-${id}`,
    cols: opts.cols || DEF_COLS,
    rows: opts.rows || DEF_ROWS,
    exited: false,
    exitCode: null,
    startedAt: Date.now(),
    endedAt: null,
    ring: [], ringBytes: 0, // scrollback, filled by the live stream (subscribeStream)
    lastCapture: '', // refreshed by the poll loop; returned by capture() synchronously
    dataL: new Set(), exitL: new Set(),
    stream: null, // the live byte-stream socket
    _detaching: false // set by stop() so a client-initiated stream close is NOT read as a session exit
  })

  const info = (id) => {
    const r = terminals.get(id)
    return r ? { id: r.id, pid: r.pid, window: r.window, pane: r.pane, cols: r.cols, rows: r.rows, exited: r.exited, exitCode: r.exitCode, startedAt: r.startedAt, endedAt: r.endedAt || null } : null
  }

  // Mark a session dead once, close its stream, and fire its exit listeners (the windowClosed analogue).
  function markExited(rec, exitCode) {
    if (!rec || rec.exited) return
    rec.exited = true
    rec.exitCode = exitCode ?? rec.exitCode ?? 0
    rec.endedAt = Date.now()
    if (rec.stream) { try { rec.stream.destroy() } catch { /* ignore */ } rec.stream = null }
    for (const l of rec.exitL) { try { l({ exitCode: rec.exitCode, signal: null }) } catch { /* a bad listener must not break teardown */ } }
  }

  // Open the per-session live byte stream: Subscribe on the control plane, then connect the raw stream
  // pipe the host serves (host_server.rs spawn_stream). Incoming bytes feed the scrollback ring AND fan
  // out to onData listeners. Stream EOF means the session ended (corroborates the poll's PaneDead),
  // EXCEPT when WE closed it via stop() (rec._detaching) - a detach leaves the session alive in the host.
  async function subscribeStream(rec) {
    if (rec.stream || rec.exited) return
    const token = randomUUID()
    try { asUnit(await rpc.request({ Subscribe: { name: rec.id, token } })) } catch { return }
    // The host creates the stream pipe AFTER acking Subscribe; retry the connect until it appears.
    const pipe = streamPipeName(token)
    let sock = null
    for (let i = 0; i < 50 && !rec.exited; i++) {
      try {
        sock = await new Promise((res, rej) => { const s = net.connect(pipe, () => res(s)); s.once('error', rej) })
        break
      } catch { await new Promise((r) => setTimeout(r, 20)) }
    }
    if (!sock) return
    if (rec.exited) { try { sock.destroy() } catch { /* ignore */ } return }
    rec.stream = sock
    // Seed the ring with the current screen so a late onData subscriber repaints (bytes emitted before
    // this socket connected are not in the broadcast). Best-effort.
    try { const seed = asText(await rpc.request({ CaptureSeed: { name: rec.id } })); if (seed) { rec.ring.push(seed); rec.ringBytes += seed.length } } catch { /* ignore */ }
    const dec = new StringDecoder('utf8') // a multibyte char split across chunks must not corrupt the TUI
    sock.on('data', (d) => {
      const text = dec.write(d)
      if (!text) return
      rec.ring.push(text); rec.ringBytes += text.length
      while (rec.ringBytes > SCROLLBACK_BYTES && rec.ring.length > 1) rec.ringBytes -= rec.ring.shift().length
      for (const l of rec.dataL) { try { l(text) } catch { /* a bad listener must not kill the stream */ } }
    })
    const onEnd = () => { rec.stream = null; if (!rec._detaching) markExited(rec, 0) }
    sock.on('close', onEnd); sock.on('error', onEnd)
  }

  // One poll tick: for every LIVE session refresh lastCapture (sync capture() source) and check
  // PaneDead (exit backstop). Errors are swallowed per-session so one dead pipe cannot stop the loop.
  async function pollOnce() {
    for (const rec of [...terminals.values()]) {
      if (rec.exited) continue
      try {
        const dead = asBool(await rpc.request({ PaneDead: { name: rec.id } }))
        if (dead) { markExited(rec, 0); continue }
        rec.lastCapture = asText(await rpc.request({ CapturePane: { name: rec.id, lines: CAPTURE_LINES } }))
      } catch { /* transient pipe error; next tick retries */ }
    }
  }

  function startPoll() {
    if (pollTimer) return
    pollTimer = setInterval(() => { pollOnce() }, POLL_MS)
    if (pollTimer.unref) pollTimer.unref() // never keep the event loop alive just for polling
  }
  function stopPoll() { if (pollTimer) { clearInterval(pollTimer); pollTimer = null } }

  // ---- interface (matches tmux-host.d.mts TmuxHost) ----

  // Ensure a session-host is reachable: connect if one already runs (a prior BlitzOS run left it detached,
  // OR supermux runs one), else spawn the binary DETACHED so it OUTLIVES BlitzOS (that is what makes
  // sessions survive a restart), with SUPERMUX_HOST_ID pinned so its pipe name matches ours. Resolves
  // true once connected; false (degraded) when no host runs and no binary is bundled.
  async function ensureHostRunning() {
    try { await rpc._connect(); return true } catch { /* none running; try to spawn one */ }
    const bin = resolveSessionHostBin()
    if (!bin) return false
    try {
      const child = cpSpawn(bin, [], { detached: true, stdio: 'ignore', windowsHide: true, env: { ...process.env, SUPERMUX_HOST_ID: HOST_ID } })
      child.unref()
    } catch (e) { console.error('[conpty-host] failed to spawn session-host:', e?.message || e); return false }
    for (let i = 0; i < 30; i++) { // wait for it to listen on the pipe (150ms x 30 = 4.5s budget)
      await new Promise((r) => setTimeout(r, 150))
      try { await rpc._connect(); return true } catch { /* not listening yet */ }
    }
    return false
  }

  function start() {
    if (ready) return ready
    ready = ensureHostRunning().then((ok) => {
      if (ok) startPoll()
      // DEGRADE like tmux-host when tmux is missing: leave ready resolved, ops below no-op against null sock.
      else console.error('[conpty-host] session-host unavailable (no running host, no bundled binary to spawn)')
    })
    return ready
  }

  async function spawn(id, opts = {}) {
    const existing = terminals.get(id)
    if (existing && !existing.exited) return info(id)
    await start()
    if (!rpc.sock) return null // degraded (no host)
    const cols = opts.cols || DEF_COLS, rows = opts.rows || DEF_ROWS
    const rec = mkRec(id, { cols, rows })
    try {
      asUnit(await rpc.request({ SpawnSession: { name: id, dir: opts.cwd || process.cwd(), env: opts.env || {}, shell: opts.command || DEF_SHELL } }))
    } catch (e) {
      console.error('[conpty-host] spawn failed:', e?.message || e)
      return null
    }
    terminals.set(id, rec)
    // Geometry, then pid; best-effort, a failure here does not unmake a spawned session.
    try { await rpc.request({ Resize: { name: id, cols, rows } }) } catch { /* ignore */ }
    try { rec.pid = asOptU32(await rpc.request({ PanePid: { name: id } })) } catch { /* ignore */ }
    subscribeStream(rec) // background: connect the live byte stream (fills ring + drives onData/onExit)
    return info(id)
  }

  // write/resize/kill are SYNCHRONOUS (terminal-manager calls them bare). Fire the RPC and return the
  // boolean immediately; tmux-host's send-keys/refresh-client are likewise fire-and-forget (sendRaw).
  function write(id, data) {
    const rec = terminals.get(id); if (!rec || rec.exited) return false
    rpc.request({ SendText: { name: id, text: String(data ?? '') } }).catch(() => {})
    return true
  }
  function resize(id, cols, rows) {
    const rec = terminals.get(id); if (!rec || rec.exited) return false
    rec.cols = cols; rec.rows = rows
    rpc.request({ Resize: { name: id, cols: cols | 0, rows: rows | 0 } }).catch(() => {})
    return true
  }
  function kill(id) {
    const rec = terminals.get(id); if (!rec) return false
    rpc.request({ KillSession: { name: id } }).catch(() => {})
    markExited(rec, 0) // optimistic: the host tears down the ConPTY + child tree (also closes our stream)
    return true
  }
  function remove(id) { kill(id); terminals.delete(id) }

  function onData(id, cb, { replay = true } = {}) {
    const rec = terminals.get(id); if (!rec) return () => {}
    if (replay && rec.ring.length) { try { cb(rec.ring.join('')) } catch { /* ignore */ } }
    rec.dataL.add(cb); return () => rec.dataL.delete(cb)
  }
  function onExit(id, cb) {
    const rec = terminals.get(id); if (!rec) return () => {}
    if (rec.exited) { try { cb({ exitCode: rec.exitCode ?? 0, signal: null }) } catch { /* ignore */ } return () => {} }
    rec.exitL.add(cb); return () => rec.exitL.delete(cb)
  }
  const scrollback = (id) => { const r = terminals.get(id); return r ? r.ring.join('') : '' }
  const capture = (id) => { const r = terminals.get(id); return r ? r.lastCapture : '' }
  const has = (id) => terminals.has(id)
  const list = () => [...terminals.values()].map((r) => info(r.id))
  // No `tmux attach` analogue on Windows (the supermux PWA over the shared host is the convergence
  // path, not an external terminal). Return null so callers show a clean "not live" message.
  const attachSpec = () => null

  /** Reattach-on-boot: ask the host for its live sessions and re-register any we do not track. The host
   *  outlived BlitzOS, so this is the restart-survival path: re-register the rec, fetch its pid, seed
   *  lastCapture, and re-Subscribe the live stream so onData/scrollback resume. */
  async function adoptExisting() {
    await start()
    if (!rpc.sock) return []
    let names = []
    try { names = asSessions(await rpc.request('ListSessions')) } catch { return [] }
    const adopted = []
    for (const name of names) {
      if (terminals.has(name)) continue
      const rec = mkRec(name)
      terminals.set(name, rec)
      try { rec.pid = asOptU32(await rpc.request({ PanePid: { name } })) } catch { /* ignore */ }
      try { rec.lastCapture = asText(await rpc.request({ CaptureSeed: { name } })) } catch { /* ignore */ }
      subscribeStream(rec) // re-attach the live stream to the survivor
      adopted.push(name)
    }
    return adopted
  }

  // Close the control connection AND the per-session streams; sessions SURVIVE in the host (the whole
  // point). _detaching tells the stream close handler this is a detach, not a session exit. A later
  // start()/adoptExisting() reconnects (ready is cleared so it is not a stale memoized resolve).
  function stop() {
    stopPoll(); ready = null
    for (const rec of terminals.values()) {
      rec._detaching = true
      if (rec.stream) { try { rec.stream.destroy() } catch { /* ignore */ } rec.stream = null }
    }
    // Drain any in-flight request (a just-fired fire-and-forget kill) BEFORE closing the control socket,
    // so a kill()-then-stop() sequence cannot abort the kill mid-send (which left stale sessions in the
    // host). The chain always settles (request() swallows rejections), so close still runs.
    rpc.chain.finally(() => rpc.close())
  }
  // No kill-all RPC in v1: enumerate and kill each. (A Request::Shutdown is a possible later add.)
  function killServer() {
    rpc.request('ListSessions').then((r) => {
      for (const name of asSessions(r)) rpc.request({ KillSession: { name } }).catch(() => {})
    }).catch(() => {})
    for (const rec of terminals.values()) markExited(rec, 0)
  }
  function stopAll() { for (const id of [...terminals.keys()]) kill(id) }

  return { start, spawn, write, resize, kill, remove, onData, onExit, scrollback, capture, has, info, list, attachSpec, adoptExisting, stop, killServer, stopAll }
}
