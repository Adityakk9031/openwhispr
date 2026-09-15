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

function deferred() {
  let resolve;
  const promise = new Promise((accept) => {
    resolve = accept;
  });
  return { promise, resolve };
}

async function until(predicate) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.fail("Expected asynchronous connection state was not reached");
}

function createHarness(t, { automaticHandshake = true, fetchToken = async () => "test-key" } = {}) {
  const handlers = new Map();
  const listeners = new Map();
  const sockets = [];
  const clients = [];
  const events = [];
  let tokenCalls = 0;
  const context = {
    Buffer,
    setTimeout,
    clearTimeout,
    debugLogger: { debug() {} },
    windowManager: { showTranscriptionPreview() {}, hideTranscriptionPreview() {} },
    TINFOIL_REALTIME_MODEL: "voxtral-mini-4b-realtime",
    ipcMain: {
      handle: (name, handler) => handlers.set(name, handler),
      on: (name, handler) => listeners.set(name, handler),
    },
    fetchRealtimeToken: (...args) => {
      tokenCalls += 1;
      return fetchToken(...args);
    },
    streamingStartFailure: (error) => ({ success: false, error: error.message }),
    OpenAIRealtimeStreaming: class extends OpenAIRealtimeStreaming {
      constructor() {
        super();
        clients.push(this);
      }

      connect(options) {
        const socket = new EventEmitter();
        socket.readyState = 0;
        socket.options = options;
        socket.sent = [];
        socket.autoAck = automaticHandshake;
        socket.ping = () => {};
        socket.close = () => {
          socket.readyState = 3;
        };
        socket.acknowledge = () => {
          socket.emit("message", JSON.stringify({ type: "session.updated" }));
        };
        socket.created = () => {
          socket.readyState = 1;
          socket.emit("message", JSON.stringify({ type: "session.created" }));
        };
        socket.send = (raw) => {
          const message = JSON.parse(raw);
          socket.sent.push(message);
          if (message.type === "session.update" && socket.autoAck) {
            setImmediate(socket.acknowledge);
          }
        };
        sockets.push(socket);
        const connected = super.connect({ ...options, createSocket: async () => socket });
        if (automaticHandshake) setImmediate(socket.created);
        return connected;
      }
    },
  };
  // Run the real IPC closures and streaming client; replace only Electron and transport.
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
    for (const client of clients) {
      client.pendingReject?.(new Error("Test connection cleanup"));
      client.cleanup();
    }
  });
  const event = { sender: { send: (...args) => events.push(args) } };
  const invoke = (channel, options) => {
    const result = handlers.get(channel)(event, options);
    result.catch(() => {});
    return result;
  };
  return {
    context,
    sockets,
    events,
    tokenCalls: () => tokenCalls,
    warmup: (options) => invoke("dictation-realtime-warmup", options),
    start: (options) => invoke("dictation-realtime-start", options),
    stop: () => invoke("dictation-realtime-stop"),
    send: (audio) => listeners.get("dictation-realtime-send")(event, audio),
  };
}

async function sessionOptions(language, mode = "byok", provider = "openai-realtime") {
  const { buildStreamingSessionOptions } =
    await import("../../src/helpers/dictationStreamingRouting.js");
  return buildStreamingSessionOptions({
    providerName: provider,
    settings: {
      cloudTranscriptionModel:
        provider === "tinfoil-realtime" ? "voxtral-mini-4b-realtime" : "gpt-4o-transcribe",
      cloudTranscriptionMode: mode,
    },
    language,
    keyterms: ["OpenWhispr", "Arzt"],
  });
}

function updates(socket) {
  return socket.sent.filter((message) => message.type === "session.update");
}

function assertConfiguredBeforeAudio(socket, expectedTranscription) {
  const audioIndex = socket.sent.findIndex(
    (message) => message.type === "input_audio_buffer.append"
  );
  assert.ok(audioIndex >= 0, "queued recording audio must reach the socket");
  const precedingUpdates = socket.sent
    .slice(0, audioIndex)
    .filter((message) => message.type === "session.update");
  assert.deepEqual(
    precedingUpdates.at(-1).session.audio.input.transcription,
    expectedTranscription,
    "the latest recording settings must precede the first audio append"
  );
}

for (const [provider, mode] of [
  ["openai-realtime", "byok"],
  ["tinfoil-realtime", "byok"],
  ["tinfoil-realtime", "openwhispr"],
]) {
  for (const language of ["auto", "fr"]) {
    test(`${provider} ${mode} warms without a language hint and reuses its socket for ${language}`, async (t) => {
      const harness = createHarness(t);
      const warmOptions = await sessionOptions("de", mode, provider);
      assert.equal((await harness.warmup(warmOptions)).success, true);
      const socket = harness.sockets[0];
      assert.deepEqual(updates(socket)[0].session.audio.input.transcription, {
        model: warmOptions.model,
        prompt: "OpenWhispr, Arzt",
      });
      const startOptions = await sessionOptions(language, mode, provider);
      startOptions.prompt = "Recording-specific terms";
      assert.equal((await harness.start(startOptions)).success, true);
      harness.send(Buffer.from([1, 0, 2, 0]));
      assert.equal(harness.sockets.length, 1);
      assert.equal(harness.tokenCalls(), 1);
      assert.equal(socket.readyState, 1);
      assertConfiguredBeforeAudio(socket, {
        model: warmOptions.model,
        ...(language === "auto" ? {} : { language }),
        prompt: "Recording-specific terms",
      });
      assert.ok(!harness.context._dictationIdleTimer);
    });
  }
}

test("cloud warmup remains preconfigured and start never sends a client session update", async (t) => {
  const harness = createHarness(t);
  await harness.warmup(await sessionOptions("de", "openwhispr"));
  assert.equal((await harness.start(await sessionOptions("auto", "openwhispr"))).success, true);
  assert.equal(harness.sockets.length, 1);
  assert.equal(harness.context._dictationStreaming.preconfigured, true);
  assert.deepEqual(updates(harness.sockets[0]), []);
});

for (const phase of ["token fetch", "session configuration"]) {
  test(`start during warmup ${phase} keeps its socket and latest settings before queued audio`, async (t) => {
    const token = deferred();
    const harness = createHarness(t, {
      automaticHandshake: false,
      fetchToken: () => token.promise,
    });
    const warming = harness.warmup(await sessionOptions("de"));
    if (phase === "session configuration") {
      token.resolve("test-key");
      await until(() => harness.sockets.length === 1 && harness.context._dictationStreaming.ws);
      harness.sockets[0].created();
      assert.equal(updates(harness.sockets[0]).length, 1);
    }
    const startOptions = { ...(await sessionOptions("fr")), prompt: "Latest recording prompt" };
    const starting = harness.start(startOptions);
    harness.send(Buffer.from([1, 0, 2, 0]));
    if (phase === "token fetch") {
      token.resolve("test-key");
      await until(() => harness.sockets.length === 1 && harness.context._dictationStreaming.ws);
      harness.sockets[0].autoAck = true;
      harness.sockets[0].created();
    } else {
      assertConfiguredBeforeAudio(harness.sockets[0], {
        model: "gpt-4o-transcribe",
        language: "fr",
        prompt: "Latest recording prompt",
      });
      harness.sockets[0].autoAck = true;
      harness.sockets[0].acknowledge();
    }
    assert.equal((await warming).success, true);
    assert.equal((await starting).success, true);
    harness.send(Buffer.from([3, 0, 4, 0]));
    assert.equal(harness.sockets.length, 1, "start must join the warmup connection");
    assert.equal(harness.tokenCalls(), 1);
    assertConfiguredBeforeAudio(harness.sockets[0], {
      model: "gpt-4o-transcribe",
      language: "fr",
      prompt: "Latest recording prompt",
    });
    assert.ok(!harness.context._dictationIdleTimer, "warmup must not idle-close recording");
  });
}

test("warmup requested while recording preserves the live socket and settings", async (t) => {
  const harness = createHarness(t);
  await harness.start(await sessionOptions("fr"));
  const socket = harness.sockets[0];
  const sentBefore = socket.sent.length;
  assert.equal((await harness.warmup(await sessionOptions("de"))).success, true);
  assert.equal(harness.sockets.length, 1);
  assert.equal(harness.tokenCalls(), 1);
  assert.equal(socket.sent.length, sentBefore);
  assert.equal(harness.context._dictationStreaming.language, "fr");
  assert.ok(!harness.context._dictationIdleTimer);
});

test("failed cold start clears recording state so the next warmup can connect", async (t) => {
  let failNextToken = true;
  const harness = createHarness(t, {
    fetchToken: async () => {
      if (failNextToken) {
        failNextToken = false;
        throw new Error("Token unavailable");
      }
      return "test-key";
    },
  });
  const failed = await harness.start(await sessionOptions("fr"));
  assert.equal(failed.success, false);
  assert.equal(failed.error, "Token unavailable");
  assert.equal((await harness.warmup(await sessionOptions("de"))).success, true);
  assert.equal(harness.tokenCalls(), 2);
  assert.equal(harness.sockets.length, 1);
  assert.equal(harness.context._dictationStreaming.language, null);
  assert.ok(harness.context._dictationIdleTimer, "an idle warmup should retain its expiry timer");
});

test("overlapping warmups and recording start share one pending connection", async (t) => {
  const token = deferred();
  const harness = createHarness(t, { fetchToken: () => token.promise });
  const firstWarmup = harness.warmup(await sessionOptions("de"));
  const secondWarmup = harness.warmup(await sessionOptions("es"));
  const starting = harness.start({ ...(await sessionOptions("fr")), prompt: "Recording terms" });
  harness.send(Buffer.from([1, 0, 2, 0]));
  token.resolve("test-key");
  for (const result of await Promise.all([firstWarmup, secondWarmup, starting])) {
    assert.equal(result.success, true);
  }
  harness.send(Buffer.from([3, 0, 4, 0]));
  assert.equal(harness.tokenCalls(), 1);
  assert.equal(harness.sockets.length, 1);
  assertConfiguredBeforeAudio(harness.sockets[0], {
    model: "gpt-4o-transcribe",
    language: "fr",
    prompt: "Recording terms",
  });
  assert.ok(!harness.context._dictationIdleTimer);
});

for (const change of ["provider", "mode"]) {
  for (const phase of ["token fetch", "session configuration"]) {
    test(`${change} switch during warmup ${phase} immediately owns a fresh connection`, async (t) => {
      const oldToken = deferred();
      let tokenRequests = 0;
      const harness = createHarness(t, {
        automaticHandshake: false,
        fetchToken: () => {
          tokenRequests += 1;
          return tokenRequests === 1 ? oldToken.promise : Promise.resolve("recording-key");
        },
      });
      const warmOptions = await sessionOptions("de", "byok");
      const startOptions = {
        ...(await sessionOptions(
          "fr",
          change === "mode" ? "openwhispr" : "byok",
          change === "provider" ? "tinfoil-realtime" : "openai-realtime"
        )),
        prompt: "Current recording terms",
      };
      const warming = harness.warmup(warmOptions);
      await until(() => harness.tokenCalls() === 1);
      const oldClient = harness.context._dictationStreaming;
      let oldSocket;
      if (phase === "session configuration") {
        oldToken.resolve("warmup-key");
        await until(() => harness.sockets.length === 1 && oldClient.ws);
        oldSocket = harness.sockets[0];
        oldSocket.created();
        assert.equal(updates(oldSocket).length, 1);
      }
      const starting = harness.start(startOptions);
      const recordingClient = harness.context._dictationStreaming;
      assert.notEqual(recordingClient, oldClient, "replacement must own opening audio immediately");
      harness.send(Buffer.from([1, 0, 2, 0]));
      assert.equal(oldClient.coldStartBufferSize, 0, "opening audio must not enter the old warmup");
      await until(() => harness.sockets.length === (oldSocket ? 2 : 1) && recordingClient.ws);
      const recordingSocket = harness.sockets.at(-1);
      assert.equal(recordingSocket.options.apiKey, "recording-key");
      assert.equal(recordingSocket.options.model, startOptions.model);
      assert.equal(recordingSocket.options.preconfigured, change === "provider" ? undefined : true);
      recordingSocket.autoAck = true;
      recordingSocket.created();
      assert.equal((await starting).success, true);
      harness.send(Buffer.from([3, 0, 4, 0]));
      if (change === "provider") {
        assertConfiguredBeforeAudio(recordingSocket, {
          model: startOptions.model,
          language: "fr",
          prompt: "Current recording terms",
        });
      } else {
        assert.deepEqual(updates(recordingSocket), [], "cloud configuration remains server-owned");
      }
      assert.equal(
        recordingSocket.sent.filter((message) => message.type === "input_audio_buffer.append")
          .length,
        2,
        "both opening and live audio must reach the replacement"
      );
      // A stale token/handshake completes after recording has begun.
      if (oldSocket) {
        oldSocket.autoAck = true;
        oldSocket.acknowledge();
      } else {
        oldToken.resolve("warmup-key");
      }
      await warming;
      oldClient.onError?.(new Error("Stale warmup error"));
      oldClient.onPartialTranscript?.("stale partial");
      oldClient.onFinalTranscript?.("stale final");
      oldClient.onSessionEnd?.({ text: "stale completed text" });
      assert.equal(harness.context._dictationStreaming, recordingClient);
      assert.equal(recordingSocket.readyState, 1, "stale completion must not disconnect recording");
      assert.equal(harness.tokenCalls(), 2);
      assert.equal(harness.sockets.length, oldSocket ? 2 : 1);
      assert.ok(!harness.context._dictationIdleTimer);
      assert.deepEqual(harness.events, [], "stale warmup callbacks must not reach the renderer");
    });
  }
}

for (const change of ["provider", "mode"]) {
  test(`${change} switch replaces an already-ready warmup before recording audio`, async (t) => {
    const harness = createHarness(t);
    await harness.warmup(await sessionOptions("de"));
    const oldClient = harness.context._dictationStreaming;
    const oldSocket = harness.sockets[0];
    const startOptions = await sessionOptions(
      "fr",
      change === "mode" ? "openwhispr" : "byok",
      change === "provider" ? "tinfoil-realtime" : "openai-realtime"
    );
    const starting = harness.start(startOptions);
    assert.notEqual(harness.context._dictationStreaming, oldClient);
    harness.send(Buffer.from([1, 0, 2, 0]));
    assert.equal((await starting).success, true);
    harness.send(Buffer.from([3, 0, 4, 0]));
    assert.equal(harness.sockets.length, 2);
    assert.equal(harness.tokenCalls(), 2);
    assert.equal(oldSocket.readyState, 3);
    assert.equal(
      oldSocket.sent.some((message) => message.type === "input_audio_buffer.append"),
      false
    );
    assert.equal(
      harness.sockets[1].sent.filter((message) => message.type === "input_audio_buffer.append")
        .length,
      2
    );
    assert.ok(!harness.context._dictationIdleTimer);
  });
}

for (const nextAction of ["start", "warmup"]) {
  test(`stopping a token-pending warmup lets a fresh ${nextAction} connect`, async (t) => {
    const oldToken = deferred();
    let tokenRequests = 0;
    const harness = createHarness(t, {
      fetchToken: () => {
        tokenRequests += 1;
        return tokenRequests === 1 ? oldToken.promise : Promise.resolve("fresh-key");
      },
    });
    const options = await sessionOptions("fr");
    const warming = harness.warmup(options);
    assert.equal((await harness.stop()).success, true);
    const next = harness[nextAction](options);
    oldToken.resolve("stopped-key");
    const [warmResult, nextResult] = await Promise.all([warming, next]);
    assert.equal(warmResult.success, false);
    assert.equal(nextResult.success, true, "stopped warmup must not own the new request");
    assert.equal(harness.tokenCalls(), 2);
    assert.equal(harness.sockets.length, 1);
    assert.equal(harness.sockets[0].options.apiKey, "fresh-key");
    assert.equal(Boolean(harness.context._dictationIdleTimer), nextAction === "warmup");
  });
}

test("a delayed stop cannot clear a newer recording or hide its preview", async (t) => {
  const harness = createHarness(t);
  const options = await sessionOptions("fr");
  await harness.warmup(options);
  const stoppingClient = harness.context._dictationStreaming;
  const drain = deferred();
  stoppingClient.disconnect = () => drain.promise;
  let previewHides = 0;
  harness.context.windowManager.hideTranscriptionPreview = () => {
    previewHides += 1;
  };
  const stopping = harness.stop();
  const started = await harness.start({ ...options, preview: true });
  assert.equal(started.success, true);
  const recordingClient = harness.context._dictationStreaming;
  drain.resolve({ text: "" });
  await stopping;
  assert.notEqual(recordingClient, stoppingClient);
  assert.equal(harness.context._dictationStreaming, recordingClient);
  assert.equal(recordingClient.isConnected, true);
  assert.equal(harness.context._dictationPreviewEnabled, true);
  assert.equal(previewHides, 0);
});

test("a stopped start failing late cannot permit warmup to replace a newer recording", async (t) => {
  const oldToken = deferred();
  let tokenRequests = 0;
  const harness = createHarness(t, {
    fetchToken: () => {
      tokenRequests += 1;
      return tokenRequests === 1 ? oldToken.promise : Promise.resolve("fresh-key");
    },
  });
  const options = await sessionOptions("fr");
  const oldStart = harness.start(options);
  await harness.stop();
  const nextStart = harness.start(options);
  oldToken.resolve("stopped-key");
  const [oldResult, nextResult] = await Promise.all([oldStart, nextStart]);
  assert.equal(oldResult.success, false);
  assert.equal(nextResult.success, true);
  const recordingClient = harness.context._dictationStreaming;
  assert.equal((await harness.warmup(options)).success, true);
  assert.equal(harness.context._dictationStreaming, recordingClient);
  assert.equal(harness.tokenCalls(), 2, "late failure must not clear the active recording flag");
  assert.ok(!harness.context._dictationIdleTimer);
});
