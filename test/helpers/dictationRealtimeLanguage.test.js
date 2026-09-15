const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const { EventEmitter } = require("node:events");
const OpenAIRealtimeStreaming = require("../../src/helpers/openaiRealtimeStreaming");

const source = fs.readFileSync(require.resolve("../../src/helpers/ipcHandlers"), "utf8");

function section(from, to) {
  const start = source.indexOf(from);
  const end = source.indexOf(to, start);
  assert.ok(start >= 0 && end > start, "IPC handler section must exist");
  return source.slice(start, end);
}

function createHarness(t) {
  const handlers = new Map();
  const sockets = [];
  const context = {
    Buffer,
    setTimeout,
    clearTimeout,
    debugLogger: { debug() {} },
    windowManager: { showTranscriptionPreview() {}, hideTranscriptionPreview() {} },
    ipcMain: {
      handle: (name, handler) => handlers.set(name, handler),
      on() {},
    },
    fetchRealtimeToken: async () => "test-key",
    streamingStartFailure: (error) => ({ success: false, error: error.message }),
    OpenAIRealtimeStreaming: class extends OpenAIRealtimeStreaming {
      connect(options) {
        const socket = new EventEmitter();
        socket.readyState = 0;
        socket.sent = [];
        socket.ping = () => {};
        socket.close = () => {
          socket.readyState = 3;
        };
        socket.send = (raw) => {
          const message = JSON.parse(raw);
          socket.sent.push(message);
          if (message.type === "session.update") {
            setImmediate(() => socket.emit("message", JSON.stringify({ type: "session.updated" })));
          }
        };
        sockets.push(socket);
        const connected = super.connect({ ...options, createSocket: async () => socket });
        setImmediate(() => {
          socket.readyState = 1;
          socket.emit("message", JSON.stringify({ type: "session.created" }));
        });
        return connected;
      }
    },
  };
  // Run the real warmup/connect/start closures, replacing only Electron and the network.
  vm.runInNewContext(
    section("const setupDictationCallbacks =", "// Pre-warm: fetch tokens") +
      section(
        'ipcMain.handle("dictation-realtime-warmup"',
        'ipcMain.handle(\n      "start-dictation-preview"'
      ),
    context
  );
  t.after(() => {
    clearTimeout(context._dictationIdleTimer);
    context._dictationStreaming?.cleanup();
  });
  const event = { sender: { send() {} } };
  return {
    context,
    sockets,
    warmup: (options) => handlers.get("dictation-realtime-warmup")(event, options),
    start: (options) => handlers.get("dictation-realtime-start")(event, options),
  };
}

async function sessionOptions(language, mode = "byok") {
  const { buildStreamingSessionOptions } =
    await import("../../src/helpers/dictationStreamingRouting.js");
  return buildStreamingSessionOptions({
    providerName: "openai-realtime",
    settings: { cloudTranscriptionModel: "gpt-4o-transcribe", cloudTranscriptionMode: mode },
    language,
    keyterms: ["OpenWhispr", "Arzt"],
  });
}

test("Auto translation replaces a warm pinned-language session with automatic detection", async (t) => {
  const { context, sockets, warmup, start } = createHarness(t);
  assert.equal((await warmup(await sessionOptions("de"))).success, true);
  assert.equal((await start(await sessionOptions("auto"))).success, true);
  assert.equal(context._dictationStreaming.language, null);
  assert.equal(sockets.length, 2);
  assert.equal(sockets[0].readyState, 3);
  assert.deepEqual(sockets[1].sent[0].session.audio.input.transcription, {
    model: "gpt-4o-transcribe",
    prompt: "OpenWhispr, Arzt",
  });
  assert.equal(context._dictationStreaming.captureRate, 16000);
  assert.equal(context._dictationStreaming.inputRate, 24000);
});

test("an explicit Auto start also resets a pinned warm session", async (t) => {
  const { context, sockets, warmup, start } = createHarness(t);
  await warmup(await sessionOptions("de"));
  const options = { ...(await sessionOptions("auto")), language: "auto" };
  assert.equal((await start(options)).success, true);
  assert.equal(context._dictationStreaming.language, null);
  assert.equal(sockets.length, 2);
});

test("a failed Auto reconnect reports failure instead of reusing the pinned session", async (t) => {
  const { context, sockets, warmup, start } = createHarness(t);
  await warmup(await sessionOptions("de"));
  context.fetchRealtimeToken = async () => {
    throw new Error("Token unavailable");
  };
  const result = await start(await sessionOptions("auto"));
  assert.equal(result.success, false);
  assert.equal(result.error, "Token unavailable");
  assert.equal(context._dictationStreaming, null);
  assert.equal(sockets[0].readyState, 3);
});

for (const [warmLanguage, startLanguage, mode, expectedLanguage] of [
  ["de", "fr", "byok", "fr"],
  ["de", "de", "byok", "de"],
  ["auto", "auto", "byok", null],
  ["de", "auto", "openwhispr", "de"],
]) {
  test(`${mode} warm session ${warmLanguage} -> ${startLanguage} still reuses its socket`, async (t) => {
    const { context, sockets, warmup, start } = createHarness(t);
    assert.equal((await warmup(await sessionOptions(warmLanguage, mode))).success, true);
    assert.equal((await start(await sessionOptions(startLanguage, mode))).success, true);
    assert.equal(sockets.length, 1);
    assert.equal(context._dictationStreaming.language, expectedLanguage);
    if (mode === "openwhispr") assert.equal(sockets[0].sent.length, 0);
  });
}
