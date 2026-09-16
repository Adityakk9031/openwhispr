const test = require("node:test");
const assert = require("node:assert/strict");
const Module = require("node:module");

// #2050: the dictation language must reach the realtime session on every path
// the dictation-realtime-* handlers own — the BYOK session.update, the cloud
// token request, and a warm connection minted for a different language.

const handlersModulePath = require.resolve("../../src/helpers/ipcHandlers");
const originalLoad = Module._load;
const handlers = new Map();
const listeners = new Map();
const electronStub = {
  app: {
    getPath: () => "/tmp",
    getName: () => "test",
    getVersion: () => "0.0.0",
    isPackaged: false,
    on() {},
    requestSingleInstanceLock: () => true,
  },
  ipcMain: {
    handle: (channel, handler) => handlers.set(channel, handler),
    on: (channel, handler) => listeners.set(channel, handler),
    removeHandler() {},
  },
  net: { fetch: async () => ({ ok: true, status: 200, json: async () => ({}) }) },
  BrowserWindow: class {
    static getAllWindows() {
      return [];
    }
    static fromWebContents() {
      return null;
    }
  },
  shell: {},
  dialog: {},
  screen: { getPrimaryDisplay: () => ({ workAreaSize: { width: 0, height: 0 } }) },
  systemPreferences: { getMediaAccessStatus: () => "granted" },
  session: { fromPartition: () => ({}) },
  clipboard: {},
  nativeImage: {},
  globalShortcut: {},
  utilityProcess: {},
  MessageChannelMain: class {},
};

const calls = { connects: [], disconnects: [], tokens: [] };
let nextInstance = 0;

class FakeRealtimeStreaming {
  constructor() {
    this.id = ++nextInstance;
    this.isConnected = false;
    this.options = null;
  }
  beginConnecting() {}
  async connect(options) {
    this.options = options;
    this.isConnected = true;
    calls.connects.push({ id: this.id, options });
  }
  async disconnect() {
    this.isConnected = false;
    calls.disconnects.push(this.id);
    return { text: "" };
  }
  sendAudio() {
    return true;
  }
}
FakeRealtimeStreaming.normalizeLanguage =
  require("../../src/helpers/openaiRealtimeStreaming").normalizeLanguage;

Module._load = function loadWithMocks(request, parent, isMain) {
  if (request === "electron") return electronStub;
  if (parent?.filename === handlersModulePath) {
    if (request === "./debugLogger") return new Proxy({}, { get: () => () => {} });
    if (request === "./openaiRealtimeStreaming") return FakeRealtimeStreaming;
    if (request === "./realtimeTokenProviders") {
      return {
        fetchRealtimeTokenForProvider: async (provider, _deps, options) => {
          calls.tokens.push({ provider, options });
          return "secret";
        },
      };
    }
  }
  return originalLoad.call(this, request, parent, isMain);
};
test.after(() => {
  Module._load = originalLoad;
});

function anything() {
  return new Proxy(function () {}, {
    get: (_target, property) => {
      if (property === Symbol.toPrimitive || property === "toString") return () => "";
      if (property === "then") return undefined;
      return anything();
    },
    apply: () => anything(),
  });
}

function scenario(t) {
  calls.connects.length = 0;
  calls.disconnects.length = 0;
  calls.tokens.length = 0;
  // Seed the constructor's dictation fields: the proxy's catch-all would make
  // `this._dictationStreaming?.isConnected` truthy on a cold start.
  const target = {
    _dictationStreaming: null,
    _dictationConnectPromise: null,
    _dictationIdleTimer: null,
    _dictationPreviewEnabled: false,
    windowManager: { hideTranscriptionPreview() {}, showTranscriptionPreview() {} },
  };
  const IPCHandlers = require(handlersModulePath);
  IPCHandlers.prototype.setupHandlers.call(
    new Proxy(target, {
      get: (value, property) => (property in value ? value[property] : anything()),
    })
  );
  const event = { sender: { send() {} } };
  const invoke = (channel, options) => handlers.get(channel)(event, options);
  t.after(() => invoke("dictation-realtime-stop"));
  return { invoke };
}

const byok = (overrides) => ({
  provider: "openai-realtime",
  mode: "byok",
  model: "gpt-4o-mini-transcribe",
  language: "de",
  ...overrides,
});

test("a cold BYOK start passes the dictation language to the realtime session", async (t) => {
  const s = scenario(t);
  assert.deepEqual(await s.invoke("dictation-realtime-start", byok()), { success: true });
  assert.equal(calls.connects.length, 1);
  assert.equal(calls.connects[0].options.language, "de");
  assert.equal(calls.connects[0].options.model, "gpt-4o-mini-transcribe");
  assert.equal(calls.connects[0].options.preconfigured, false);
});

test("a cloud start sends the language and model with the token request", async (t) => {
  const s = scenario(t);
  await s.invoke(
    "dictation-realtime-start",
    byok({ mode: "openwhispr", model: "gpt-4o-transcribe", language: "de" })
  );
  assert.equal(calls.tokens.length, 1);
  assert.equal(calls.tokens[0].provider, "openai-realtime");
  assert.equal(calls.tokens[0].options.language, "de");
  assert.equal(calls.tokens[0].options.model, "gpt-4o-transcribe");
  assert.equal(calls.connects[0].options.preconfigured, true);
});

test("the cloud token request carries the base language code and omits it for auto", async (t) => {
  const s = scenario(t);
  await s.invoke("dictation-realtime-start", byok({ mode: "openwhispr", language: "zh-TW" }));
  await s.invoke("dictation-realtime-stop");
  await s.invoke("dictation-realtime-start", byok({ mode: "openwhispr", language: "auto" }));
  assert.equal(calls.tokens[0].options.language, "zh");
  assert.equal("language" in calls.tokens[1].options, true);
  assert.equal(calls.tokens[1].options.language, undefined);
});

test("the tinfoil branch keeps its language-free connect (voxtral support is unverified)", async (t) => {
  const s = scenario(t);
  await s.invoke("dictation-realtime-start", byok({ provider: "tinfoil-realtime" }));
  assert.equal(calls.connects.length, 1);
  assert.equal("language" in calls.connects[0].options, false);
});

test("a start reuses a warm connection minted for the same language and model", async (t) => {
  const s = scenario(t);
  await s.invoke("dictation-realtime-warmup", byok());
  await s.invoke("dictation-realtime-start", byok());
  assert.equal(calls.connects.length, 1, "no redial");
  assert.deepEqual(calls.disconnects, []);
});

test("a start redials when the warm connection was minted for another language", async (t) => {
  const s = scenario(t);
  await s.invoke("dictation-realtime-warmup", byok({ language: "de" }));
  await s.invoke("dictation-realtime-start", byok({ language: undefined }));
  assert.equal(calls.connects.length, 2, "warm German session cannot serve an Auto dictation");
  assert.deepEqual(calls.disconnects, [calls.connects[0].id]);
  assert.equal("language" in calls.connects[1].options, true);
  assert.equal(calls.connects[1].options.language, undefined);
});

test("a start redials when the warm connection was minted for another model", async (t) => {
  const s = scenario(t);
  await s.invoke("dictation-realtime-warmup", byok({ model: "gpt-4o-mini-transcribe" }));
  await s.invoke("dictation-realtime-start", byok({ model: "gpt-4o-transcribe" }));
  assert.equal(calls.connects.length, 2);
  assert.equal(calls.connects[1].options.model, "gpt-4o-transcribe");
});

test("a start redials when the warm connection belongs to another provider or mode", async (t) => {
  const s = scenario(t);
  await s.invoke("dictation-realtime-warmup", byok({ mode: "openwhispr" }));
  await s.invoke("dictation-realtime-start", byok({ mode: "byok" }));
  assert.equal(calls.connects.length, 2);
  assert.equal(calls.connects[1].options.preconfigured, false);
});

test("after stop, the next start dials fresh instead of matching stale options", async (t) => {
  const s = scenario(t);
  await s.invoke("dictation-realtime-start", byok());
  await s.invoke("dictation-realtime-stop");
  await s.invoke("dictation-realtime-start", byok());
  assert.equal(calls.connects.length, 2);
});
