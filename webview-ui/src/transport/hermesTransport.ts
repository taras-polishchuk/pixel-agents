/**
 * Hermes transport for the Pixel Agents webview.
 *
 * Replaces the VS Code postMessage / standalone WebSocket transports. The
 * webview runs inside an iframe of the Hermes dashboard plugin "pixel-office".
 *
 *   assets   -> pre-decoded JSON shipped next to index.html (assets/decoded/*.json)
 *   state    -> GET  /api/plugins/pixel-office/bootstrap
 *   activity -> GET  /api/plugins/pixel-office/snapshot   (polled, diffed here)
 *   writes   -> PUT  layout / seats / settings / area-mappings
 *   host     -> window.parent.postMessage({ ns: 'pixel-office', ... })
 *
 * The snapshot is a full description of every agent; this class turns
 * consecutive snapshots into the incremental ServerMessages the webview
 * already understands (agentCreated, agentToolStart, subagentToolStart, ...).
 */
import { TRANSPORT_STATE_CONNECTED, TRANSPORT_STATE_CONNECTING, TRANSPORT_STATE_RECONNECTING } from '../../../core/src/constants.js';
import type { ClientMessage, ServerMessage } from '../../../core/src/messages.js';
import type { MessageTransport, TransportState } from './types.js';

const API = '/api/plugins/pixel-office';
const NS = 'pixel-office';
const POLL_MS = 1500;
const POLL_HIDDEN_MS = 6000;
/** agentClosed plays a despawn effect; the id stays taken until it finishes. */
const RESPAWN_MS = 1200;

const READING_TOOLS = [
  'read_file', 'search_files', 'web_search', 'web_extract', 'skill_view', 'skills_list',
  'session_search', 'vision_analyze', 'browser_snapshot', 'browser_navigate', 'browser_exec',
  'honcho_search', 'honcho_context', 'honcho_profile', 'honcho_reasoning', 'kanban_show',
  'kanban_list', 'chat_history_lookup', 'tool_search', 'tool_describe',
];
const SUBAGENT_TOOLS = ['delegate_task', 'hermes_session'];

interface ToolSnap { tool_id: string; tool_name: string; status: string }
interface SubSnap { parent_tool_id: string; label: string; tools: { tool_id: string; status: string }[]; permission?: boolean }
export interface AgentSnap {
  id: number;
  key: string;
  name: string;
  team: string | null;
  is_lead: boolean;
  lead_id: number | null;
  status: 'active' | 'waiting' | 'idle';
  awaiting_input: boolean;
  permission: boolean;
  tools: ToolSnap[];
  subagents: SubSnap[];
  hidden: boolean;
}
interface Snapshot { agents: AgentSnap[]; teams: { id: string; name: string }[] }
type SeatMap = Record<string, { palette?: number; hueShift?: number; seatId?: string | null }>;
interface BootPayload {
  settings: Record<string, unknown>;
  layout: Record<string, unknown> | null;
  seats: SeatMap;
  area_mappings: Record<string, string[]>;
  teams: { id: string; name: string }[];
  version: string;
}

interface HermesSdk {
  fetchJSON?: <T>(url: string, init?: RequestInit) => Promise<T>;
}

function hostSdk(): HermesSdk | null {
  try {
    return ((window.parent as unknown as { __HERMES_PLUGIN_SDK__?: HermesSdk }).__HERMES_PLUGIN_SDK__) ?? null;
  } catch {
    return null;
  }
}

function hostToken(): string {
  try {
    const w = window.parent as unknown as { __HERMES_SESSION_TOKEN__?: string };
    return w.__HERMES_SESSION_TOKEN__ ?? '';
  } catch {
    return '';
  }
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json' };
  const sdk = hostSdk();
  if (sdk?.fetchJSON) {
    return sdk.fetchJSON<T>(API + path, { ...init, headers: { ...headers, ...(init?.headers as Record<string, string>) } });
  }
  const tok = hostToken();
  if (tok) headers['X-Hermes-Session-Token'] = tok;
  const res = await fetch(API + path, { ...init, headers });
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  return (await res.json()) as T;
}

async function asset<T>(rel: string): Promise<T | null> {
  try {
    const res = await fetch(`./assets/${rel}`);
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch {
    return null;
  }
}

function toHost(msg: Record<string, unknown>): void {
  try {
    window.parent?.postMessage({ ns: NS, ...msg }, window.location.origin);
  } catch {
    /* not embedded */
  }
}

function blank(a: AgentSnap): AgentSnap {
  return { ...a, tools: [], subagents: [], status: 'idle', permission: false, awaiting_input: false };
}

function visible(a: AgentSnap): boolean {
  return !a.hidden;
}

export class HermesTransport implements MessageTransport {
  private handlers: Array<(msg: ServerMessage) => void> = [];
  private stateHandlers: Array<(s: TransportState) => void> = [];
  private _state: TransportState = TRANSPORT_STATE_CONNECTING;
  private resolveReady!: () => void;
  readonly ready: Promise<void> = new Promise((r) => (this.resolveReady = r));
  private prev = new Map<number, AgentSnap>();
  private seats: SeatMap = {};
  private timer: ReturnType<typeof setTimeout> | null = null;
  private booted = false;
  private disposed = false;
  private failures = 0;
  private lastSnapshot: Snapshot | null = null;
  private respawning = new Set<number>();

  constructor() {
    window.addEventListener('message', this.onHostMessage);
    document.addEventListener('visibilitychange', () => {
      if (!document.hidden) this.schedule(50);
    });
  }

  get state(): TransportState {
    return this._state;
  }

  private setState(s: TransportState): void {
    if (s === this._state) return;
    this._state = s;
    for (const h of this.stateHandlers) h(s);
  }

  onMessage(handler: (message: ServerMessage) => void): () => void {
    this.handlers.push(handler);
    return () => {
      this.handlers = this.handlers.filter((h) => h !== handler);
    };
  }

  onStateChange(handler: (s: TransportState) => void): () => void {
    this.stateHandlers.push(handler);
    return () => {
      this.stateHandlers = this.stateHandlers.filter((h) => h !== handler);
    };
  }

  dispose(): void {
    this.disposed = true;
    if (this.timer) clearTimeout(this.timer);
    window.removeEventListener('message', this.onHostMessage);
  }

  private emit(msg: Record<string, unknown>): void {
    const m = msg as unknown as ServerMessage;
    for (const h of this.handlers) h(m);
  }

  // ── host (dashboard) <-> office bridge ────────────────────────────────────
  private onHostMessage = (e: MessageEvent): void => {
    if (e.origin !== window.location.origin) return;
    const d = e.data as { ns?: string; type?: string; id?: number } | null;
    if (!d || d.ns !== NS || d.type === undefined) return;
    if (d.type === 'select' && typeof d.id === 'number') {
      this.emit({ type: 'agentSelected', id: d.id });
    } else if (d.type === 'refresh') {
      this.schedule(0);
    }
  };

  // ── client -> server ──────────────────────────────────────────────────────
  send(message: ClientMessage): void {
    const m = message as unknown as Record<string, unknown>;
    switch (m.type) {
      case 'webviewReady':
        void this.boot();
        break;
      case 'saveLayout':
        void api('/layout', { method: 'PUT', body: JSON.stringify({ layout: m.layout }) }).catch(console.error);
        break;
      case 'saveAgentSeats':
        this.seats = { ...this.seats, ...(m.seats as SeatMap) };
        void api('/seats', { method: 'PUT', body: JSON.stringify({ seats: m.seats }) }).catch(console.error);
        break;
      case 'saveAreaMappings':
        void api('/area-mappings', { method: 'PUT', body: JSON.stringify({ mappings: m.mappings }) }).catch(console.error);
        break;
      case 'setSoundEnabled':
        void this.putSetting({ soundEnabled: m.enabled });
        break;
      case 'setAlwaysShowLabels':
        void this.putSetting({ alwaysShowLabels: m.enabled });
        break;
      case 'setShowAreas':
        void this.putSetting({ showAreas: m.enabled });
        break;
      case 'setLastSeenVersion':
        void this.putSetting({ lastSeenVersion: m.version });
        break;
      case 'focusAgent': {
        const a = this.prev.get(m.id as number);
        toHost({ type: 'focus', id: m.id, profile: a?.key ?? null });
        break;
      }
      case 'closeAgent': {
        const a = this.prev.get(m.id as number);
        toHost({ type: 'close', id: m.id, profile: a?.key ?? null });
        break;
      }
      case 'launchAgent':
        toHost({ type: 'launch', folderPath: m.folderPath ?? null });
        break;
      case 'requestDiagnostics':
        this.emit({ type: 'agentDiagnostics', agents: [...this.prev.values()] as unknown as Record<string, unknown>[] });
        break;
      default:
        // hooks / external assets / sessions folder: not applicable in Hermes.
        break;
    }
  }

  private async putSetting(patch: Record<string, unknown>): Promise<void> {
    try {
      await api('/settings', { method: 'PUT', body: JSON.stringify(patch) });
    } catch (err) {
      console.error('[pixel-office] settings', err);
    }
  }

  // ── boot sequence (mirrors server/src/clientMessageHandler handleWebviewReady) ─
  private async boot(): Promise<void> {
    if (this.booted) return;
    this.booted = true;
    this.emit({ type: 'providerCapabilities', readingTools: READING_TOOLS, subagentToolNames: SUBAGENT_TOOLS });

    const [characters, pets, floors, walls, carpets, furniture, defaultLayout] = await Promise.all([
      asset<unknown[]>('decoded/characters.json'),
      asset<{ pets: unknown[]; petNames: string[] }>('decoded/pets.json'),
      asset<unknown[]>('decoded/floors.json'),
      asset<unknown[]>('decoded/walls.json'),
      asset<unknown[]>('decoded/carpets.json'),
      asset<{ catalog: unknown[]; sprites: Record<string, unknown> }>('decoded/furniture.json'),
      asset<Record<string, unknown>>('default-layout.json'),
    ]);
    if (characters) this.emit({ type: 'characterSpritesLoaded', characters });
    if (pets) this.emit({ type: 'petSpritesLoaded', pets: pets.pets, petNames: pets.petNames });
    if (floors) this.emit({ type: 'floorTilesLoaded', sprites: floors });
    if (walls) this.emit({ type: 'wallTilesLoaded', sets: walls });
    if (carpets) this.emit({ type: 'carpetTilesLoaded', sets: carpets });
    if (furniture) this.emit({ type: 'furnitureAssetsLoaded', catalog: furniture.catalog, sprites: furniture.sprites });

    let boot: BootPayload | null = null;
    let snap: Snapshot | null = null;
    for (;;) {
      try {
        [boot, snap] = await Promise.all([api<BootPayload>('/bootstrap'), api<Snapshot>('/snapshot')]);
        break;
      } catch (err) {
        console.error('[pixel-office] bootstrap failed, retrying', err);
        this.setState(TRANSPORT_STATE_RECONNECTING);
        await new Promise((r) => setTimeout(r, 3000));
      }
    }
    if (!boot || !snap) return;
    this.seats = boot.seats || {};
    const s = boot.settings || {};
    this.emit({
      type: 'settingsLoaded',
      soundEnabled: s.soundEnabled === true,
      lastSeenVersion: (s.lastSeenVersion as string) || boot.version,
      extensionVersion: boot.version,
      watchAllSessions: false,
      alwaysShowLabels: s.alwaysShowLabels !== false,
      ghostHeadlessAgents: false,
      hooksEnabled: false,
      hooksInfoShown: true,
      externalAssetDirectories: [],
      showAreas: s.showAreas !== false,
    });
    this.emit({ type: 'workspaceFolders', folders: this.teamFolders(snap) });
    this.emit({ type: 'areaMappingsLoaded', mappings: boot.area_mappings || {} });

    const agents = snap.agents.filter(visible);
    const agentMeta: Record<string, unknown> = {};
    const folderNames: Record<string, string> = {};
    for (const a of agents) {
      const seat = this.seats[String(a.id)];
      if (seat) agentMeta[String(a.id)] = { palette: seat.palette ?? undefined, hueShift: seat.hueShift ?? undefined, seatId: seat.seatId ?? undefined };
      if (a.team) folderNames[String(a.id)] = a.team;
    }
    this.emit({ type: 'existingAgents', agents: agents.map((a) => a.id), agentMeta, folderNames, externalAgents: {} });
    this.emit({ type: 'layoutLoaded', layout: boot.layout ?? defaultLayout ?? null });

    // Replay current activity onto the freshly placed characters.
    for (const a of agents) {
      this.prev.set(a.id, blank(a));
      this.emitTeam(a);
    }
    this.apply(snap, true);
    this.setState(TRANSPORT_STATE_CONNECTED);
    this.resolveReady();
    toHost({ type: 'ready' });
    this.schedule(POLL_MS);
  }

  private teamFolders(snap: Snapshot): { name: string; path: string }[] {
    return (snap.teams || []).map((t) => ({ name: t.name, path: `team:${t.id}` }));
  }

  private schedule(ms: number): void {
    if (this.disposed) return;
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => void this.poll(), ms);
  }

  private async poll(): Promise<void> {
    try {
      const snap = await api<Snapshot>('/snapshot');
      this.failures = 0;
      this.setState(TRANSPORT_STATE_CONNECTED);
      this.apply(snap, false);
    } catch (err) {
      this.failures += 1;
      if (this.failures > 1) this.setState(TRANSPORT_STATE_RECONNECTING);
      console.warn('[pixel-office] poll failed', err);
    }
    this.schedule(document.hidden ? POLL_HIDDEN_MS : POLL_MS * Math.min(4, 1 + this.failures));
  }

  private emitTeam(a: AgentSnap): void {
    this.emit({
      type: 'agentTeamInfo',
      id: a.id,
      teamName: a.team ?? undefined,
      agentName: a.name,
      isTeamLead: a.team ? a.is_lead : undefined,
      leadAgentId: a.team && !a.is_lead ? (a.lead_id ?? undefined) : undefined,
    });
  }

  private create(a: AgentSnap): void {
    const seat = this.seats[String(a.id)];
    this.emit({
      type: 'agentCreated',
      id: a.id,
      folderName: a.team ?? undefined,
      palette: seat?.palette ?? undefined,
      hueShift: seat?.hueShift ?? undefined,
    });
    this.emitTeam(a);
  }

  /** Diff `snap` against the previous snapshot and emit incremental messages. */
  private apply(snap: Snapshot, initial: boolean): void {
    const prevTeams = JSON.stringify(this.lastSnapshot?.teams?.map((t) => t.name) ?? []);
    const nextTeams = JSON.stringify(snap.teams?.map((t) => t.name) ?? []);
    if (!initial && prevTeams !== nextTeams) {
      this.emit({ type: 'workspaceFolders', folders: this.teamFolders(snap) });
    }
    this.lastSnapshot = snap;
    toHost({ type: 'snapshot' });

    const next = new Map<number, AgentSnap>();
    for (const a of snap.agents) if (visible(a)) next.set(a.id, a);

    // Removed agents.
    for (const [id] of this.prev) {
      if (!next.has(id)) this.emit({ type: 'agentClosed', id });
    }

    for (const [id, cur] of next) {
      if (this.respawning.has(id)) {
        next.set(id, this.prev.get(id) ?? blank(cur));
        continue;
      }
      let old = this.prev.get(id);
      if (!old) {
        this.create(cur);
        old = blank(cur);
      } else if ((old.team ?? null) !== (cur.team ?? null)) {
        // folderName is fixed at creation: despawn, then re-create in the new team.
        this.emit({ type: 'agentClosed', id });
        this.respawning.add(id);
        next.set(id, blank(cur));
        setTimeout(() => this.respawn(id), RESPAWN_MS);
        continue;
      } else if (old.is_lead !== cur.is_lead || old.lead_id !== cur.lead_id || old.name !== cur.name) {
        this.emitTeam(cur);
      }
      this.diffAgent(old, cur);
    }
    this.prev = next;
  }

  private respawn(id: number): void {
    this.respawning.delete(id);
    const cur = this.lastSnapshot?.agents.find((a) => a.id === id);
    if (!cur || !visible(cur)) {
      this.prev.delete(id);
      return;
    }
    this.create(cur);
    this.diffAgent(blank(cur), cur);
    this.prev.set(id, cur);
  }

  private diffAgent(old: AgentSnap, cur: AgentSnap): void {
    const id = cur.id;
    const oldTools = new Map(old.tools.map((t) => [t.tool_id, t]));
    const curTools = new Map(cur.tools.map((t) => [t.tool_id, t]));
    const oldSubs = new Map(old.subagents.map((s) => [s.parent_tool_id, s]));
    const curSubs = new Map(cur.subagents.map((s) => [s.parent_tool_id, s]));

    // Sub-agent tool rows first (their parents may be about to finish).
    for (const [pid, sub] of curSubs) {
      const prevRows = new Set((oldSubs.get(pid)?.tools ?? []).map((t) => t.tool_id));
      const curRows = new Set(sub.tools.map((t) => t.tool_id));
      for (const t of oldSubs.get(pid)?.tools ?? []) {
        if (!curRows.has(t.tool_id)) this.emit({ type: 'subagentToolDone', id, parentToolId: pid, toolId: t.tool_id });
      }
      // Parent already running: stream new rows now. A brand-new parent gets its
      // rows right after its own agentToolStart below.
      if (oldTools.has(pid) && curTools.has(pid)) {
        for (const t of sub.tools) {
          if (!prevRows.has(t.tool_id)) this.emit({ type: 'subagentToolStart', id, parentToolId: pid, toolId: t.tool_id, status: t.status });
        }
      }
      if (sub.permission && !oldSubs.get(pid)?.permission) this.emit({ type: 'subagentToolPermission', id, parentToolId: pid });
    }

    // Finished tools.
    for (const [tid] of oldTools) {
      if (!curTools.has(tid)) {
        this.emit({ type: 'agentToolDone', id, toolId: tid });
        if (oldSubs.has(tid)) this.emit({ type: 'subagentClear', id, parentToolId: tid });
      }
    }
    // New tools: sub-agent spawners first so the parent's own latest tool drives its animation.
    const fresh = cur.tools.filter((t) => !oldTools.has(t.tool_id));
    fresh.sort((a, b) => Number(SUBAGENT_TOOLS.includes(b.tool_name)) - Number(SUBAGENT_TOOLS.includes(a.tool_name)));
    for (const t of fresh) {
      this.emit({
        type: 'agentToolStart',
        id,
        toolId: t.tool_id,
        status: t.status,
        toolName: t.tool_name,
        permissionActive: cur.permission && t.tool_name === 'clarify',
      });
      const sub = curSubs.get(t.tool_id);
      for (const row of sub?.tools ?? []) {
        this.emit({ type: 'subagentToolStart', id, parentToolId: t.tool_id, toolId: row.tool_id, status: row.status });
      }
    }
    if (cur.tools.length === 0 && old.tools.length > 0) this.emit({ type: 'agentToolsClear', id });

    // Status.
    if (cur.status !== old.status || cur.awaiting_input !== old.awaiting_input) {
      if (cur.status === 'active') {
        this.emit({ type: 'agentStatus', id, status: 'active' });
      } else if (cur.status === 'waiting') {
        this.emit({ type: 'agentStatus', id, status: 'waiting', awaitingInput: cur.awaiting_input });
      } else if (old.status === 'active') {
        // Went quiet without a clean turn end: stop typing, let them wander.
        this.emit({ type: 'agentToolsClear', id });
        this.emit({ type: 'agentStatus', id, status: 'waiting', awaitingInput: false });
      }
    }

    // Permission ("needs you") bubble.
    if (cur.permission && !old.permission) this.emit({ type: 'agentToolPermission', id });
    if (!cur.permission && old.permission) this.emit({ type: 'agentToolPermissionClear', id });
  }
}
