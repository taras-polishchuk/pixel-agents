// Hermes Pixel Office: the webview always talks to the Hermes dashboard plugin
// backend. Replaces upstream's VS Code / standalone-WebSocket selection.
import { HermesTransport } from './hermesTransport.js';
import type { MessageTransport } from './types.js';

/** Singleton transport instance. Import this everywhere instead of vscodeApi. */
export const transport: MessageTransport = new HermesTransport();
export type { MessageTransport } from './types.js';
