const test = require("node:test");
const assert = require("node:assert/strict");
const { loadAudioManager } = require("./harness/audioManager");
const { deferred } = require("./harness/deferred");

async function loadManagerClass(t) {
  const { AudioManager } = await loadAudioManager(t, {
    cachePrefix: "openwhispr-streaming-finalization-test-",
    settingsKey: "__streamingFinalizationSettings",
    settings: {
      useLocalWhisper: false,
      transcriptionMode: "providers",
      cloudTranscriptionMode: "byok",
      cloudTranscriptionProvider: "openai",
    },
  });
  return AudioManager;
}

function createFinalizingManager(AudioManager) {
  const states = [];
  let providerStopCalls = 0;
  const manager = Object.assign(Object.create(AudioManager.prototype), {
    isRecording: true,
    isProcessing: false,
    isStreaming: true,
    streamingStartInProgress: false,
    _streamingStartSettlementWaiters: [],
    stopRequestedDuringStreamingStart: false,
    recordingStartTime: Date.now(),
    _streamingStopPromise: null,
    _streamingStopMode: null,
    _streamingCancellationGeneration: 0,
    _activeTranscriptionAbortController: null,
    _streamingSessionGeneration: 7,
    _activeStreamingSessionId: 7,
    _streamingMicSwapPromise: null,
    streamingFinalText: "",
    streamingPartialText: "",
    streamingTextBump: null,
    streamingTextDebounce: null,
    streamingCleanupFns: [],
    streamingProcessor: null,
    streamingSource: null,
    streamingAnalyser: null,
    streamingAudioContext: null,
    streamingStream: null,
    streamingFallbackRecorder: null,
    streamingFallbackChunks: [],
    _streamingFallbackSegments: [],
    pendingAssistantConversation: null,
    pendingSelectionEdit: null,
    micRecovery: { stop() {} },
    finishStreamingFallbackSegment: async () => null,
    mergeRecordedSegments: async () => null,
    getLargestRecordedSegment: () => null,
    awaitStreamingTextSettled: async () => {},
    getStreamingProvider: () => ({
      awaitsFinalTranscript: true,
      finalize() {},
      async stop() {
        providerStopCalls += 1;
        return { success: true };
      },
    }),
    getEffectiveSttLanguage: () => "auto",
    getStreamingProviderName: () => "openai",
    shouldUseStreaming: () => false,
    isRecordingAllowedByPolicy: () => true,
    onStateChange: (state) => states.push(state),
    onTranscriptionComplete() {},
  });
  return { manager, states, getProviderStopCalls: () => providerStopCalls };
}

test("streaming finalization is immediately processing and cannot start another session", async (t) => {
  const AudioManager = await loadManagerClass(t);
  const { manager, states, getProviderStopCalls } = createFinalizingManager(AudioManager);

  const firstStop = manager.stopStreamingRecording();

  assert.equal(manager.isProcessing, true);
  assert.equal(manager.getState().isFinalizingStreaming, true);
  assert.deepEqual(states[0], {
    isRecording: false,
    isProcessing: true,
    isStreaming: false,
  });

  assert.equal(await manager.startStreamingRecording(), false);
  const duplicateStop = manager.stopStreamingRecording();
  assert.deepEqual(await Promise.all([firstStop, duplicateStop]), [true, true]);

  assert.equal(getProviderStopCalls(), 1);
  assert.equal(manager.isProcessing, false);
  assert.equal(manager.getState().isFinalizingStreaming, false);
  assert.equal(states.filter((state) => state.isProcessing).length, 1);
  assert.deepEqual(states.at(-1), {
    isRecording: false,
    isProcessing: false,
    isStreaming: false,
  });
});

test("streaming silence publishes its empty outcome only after processing settles", async (t) => {
  const AudioManager = await loadManagerClass(t);
  const { manager } = createFinalizingManager(AudioManager);
  const order = [];
  manager.onStateChange = (state) => {
    order.push(state.isProcessing ? "processing" : "idle");
  };
  manager.onTranscriptionComplete = (result) => {
    order.push(result.text === "" ? "empty" : "transcript");
  };

  await manager.stopStreamingRecording();

  assert.deepEqual(order, ["processing", "idle", "empty"]);
});

test("streaming completion keeps the recording occurrence time", async (t) => {
  const AudioManager = await loadManagerClass(t);
  const { manager } = createFinalizingManager(AudioManager);
  globalThis.window.dispatchEvent = () => true;
  const recordingStartedAt = Date.parse("2026-09-02T14:00:00.000Z");
  let completion;
  manager.recordingStartTime = recordingStartedAt;
  manager.streamingFinalText = "same event";
  manager.finalizeChineseScript = async (text) => text;
  manager.onTranscriptionComplete = (result) => {
    completion = result;
  };

  await manager.stopStreamingRecording();

  assert.equal(completion.analyticsOccurredAt, new Date(recordingStartedAt).toISOString());
});

test("cancelling an active streaming recording discards it without publishing text", async (t) => {
  const AudioManager = await loadManagerClass(t);
  const { manager, states, getProviderStopCalls } = createFinalizingManager(AudioManager);
  const completions = [];
  manager.streamingFinalText = "discard me";
  manager.cleanupPreview = async () => null;
  manager.onTranscriptionComplete = (result) => completions.push(result);

  assert.equal(await manager.cancelStreamingRecording(), true);

  assert.equal(getProviderStopCalls(), 1);
  assert.deepEqual(completions, []);
  assert.equal(manager._activeStreamingSessionId, null);
  assert.equal(manager.isRecording, false);
  assert.equal(manager.isProcessing, false);
  assert.equal(manager.isStreaming, false);
  assert.deepEqual(states.at(-1), {
    isRecording: false,
    isProcessing: false,
    isStreaming: false,
  });
});

test("streaming discard blocks restart until the provider disconnects", async (t) => {
  const AudioManager = await loadManagerClass(t);
  const { manager } = createFinalizingManager(AudioManager);
  let resolveProviderStop;
  let providerStopStarted = false;
  const providerStop = new Promise((resolve) => {
    resolveProviderStop = resolve;
  });
  manager.cleanupPreview = async () => null;
  manager.getStreamingProvider = () => ({
    stop: async () => {
      providerStopStarted = true;
      await providerStop;
      return { success: true };
    },
  });

  const cancel = manager.cancelStreamingRecording();
  while (!providerStopStarted) await new Promise((resolve) => setImmediate(resolve));

  assert.equal(manager.isProcessing, true);
  assert.equal(manager.getState().isFinalizingStreaming, true);
  assert.equal(await manager.startStreamingRecording(), false);

  resolveProviderStop();
  assert.equal(await cancel, true);
  assert.equal(manager.isProcessing, false);
  assert.equal(manager.getState().isFinalizingStreaming, false);
});

test("streaming discard waits for an in-progress provider start before disconnecting", async (t) => {
  const AudioManager = await loadManagerClass(t);
  const { manager } = createFinalizingManager(AudioManager);
  let providerStopCalls = 0;
  manager.streamingStartInProgress = true;
  manager.cleanupPreview = async () => null;
  manager.getStreamingProvider = () => ({
    stop: async () => {
      providerStopCalls += 1;
      return { success: true };
    },
  });

  const cancel = manager.cancelStreamingRecording();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(providerStopCalls, 0);
  assert.equal(manager.isProcessing, true);

  manager._settleStreamingStart();
  assert.equal(await cancel, true);
  assert.equal(providerStopCalls, 1);
  assert.equal(manager.isProcessing, false);
});

test("cancelling while the streaming microphone opens never enters recording", async (t) => {
  const AudioManager = await loadManagerClass(t);
  const states = [];
  let resolveMicOpen;
  let micOpenStarted = false;
  const micOpen = new Promise((resolve) => {
    resolveMicOpen = resolve;
  });
  const previousAudioWorkletNode = globalThis.AudioWorkletNode;
  globalThis.AudioWorkletNode = class {
    constructor() {
      this.port = { postMessage() {} };
    }

    disconnect() {}
  };
  t.after(() => {
    if (previousAudioWorkletNode === undefined) delete globalThis.AudioWorkletNode;
    else globalThis.AudioWorkletNode = previousAudioWorkletNode;
  });

  const stream = {
    getAudioTracks: () => [{ getSettings: () => ({}) }],
    getTracks: () => [{ stop() {} }],
  };
  const source = { connect() {}, disconnect() {} };
  const provider = {
    onPartial: () => () => {},
    onFinal: () => () => {},
    onError: () => () => {},
    onSessionEnd: () => () => {},
    start: async () => ({ success: true }),
    stop: async () => ({ success: true }),
  };
  const manager = Object.assign(Object.create(AudioManager.prototype), {
    isRecording: false,
    isProcessing: false,
    isStreaming: false,
    streamingStartInProgress: false,
    _streamingStartSettlementWaiters: [],
    stopRequestedDuringStreamingStart: false,
    _streamingStopPromise: null,
    _streamingStopMode: null,
    _streamingCancellationGeneration: 0,
    _activeTranscriptionAbortController: null,
    _streamingSessionGeneration: 0,
    _activeStreamingSessionId: null,
    streamingCleanupFns: [],
    streamingFallbackRecorder: null,
    streamingFallbackChunks: [],
    _streamingFallbackSegments: [],
    streamingTextDebounce: null,
    preparedMicCapture: { take: async () => null },
    micRecovery: { stop() {} },
    isRecordingAllowedByPolicy: () => true,
    getAudioConstraints: async () => ({}),
    _acquireCaptureStream: async () => {
      micOpenStarted = true;
      return micOpen;
    },
    startStreamingFallbackRecorder() {},
    getOrCreateAudioContext: async () => ({
      createMediaStreamSource: () => source,
      createAnalyser: () => ({}),
      audioWorklet: { addModule: async () => {} },
    }),
    getWorkletBlobUrl: () => "",
    getStreamingProvider: () => provider,
    getStreamingProviderName: () => "openai",
    getEffectiveSttLanguage: () => "auto",
    getKeyterms: () => [],
    beginMicRecovery: async () => {},
    cleanupPreview: async () => null,
    _markCaptureStreamReleased() {},
    onStateChange: (state) => states.push(state),
  });

  const start = manager.startStreamingRecording();
  while (!micOpenStarted) await new Promise((resolve) => setImmediate(resolve));
  const cancel = manager.cancelStreamingRecording();
  resolveMicOpen(stream);

  assert.deepEqual(await Promise.all([start, cancel]), [false, true]);
  assert.equal(manager.isRecording, false);
  assert.equal(manager.isStreaming, false);
  assert.equal(manager.streamingStartInProgress, false);
  assert.equal(
    states.some((state) => state.isRecording),
    false,
    "a cancelled start must not publish a recording state"
  );
});

test("cancel overrides a normal streaming stop before it can publish text", async (t) => {
  const AudioManager = await loadManagerClass(t);
  const { manager } = createFinalizingManager(AudioManager);
  const completions = [];
  manager.streamingFinalText = "do not paste";
  manager.screenContextPromise = Promise.resolve({ data: "stale-screen" });
  manager.selectionCapturePromise = Promise.resolve({ text: "stale-selection" });
  manager.assistantSelectionContext = { text: "stale-assistant-selection" };
  manager.onTranscriptionComplete = (result) => completions.push(result);

  const stop = manager.stopStreamingRecording();
  const cancel = manager.cancelStreamingRecording();

  assert.equal(await stop, true);
  assert.equal(await cancel, true);
  assert.deepEqual(completions, []);
  assert.equal(manager.screenContextPromise, null);
  assert.equal(manager.selectionCapturePromise, null);
  assert.equal(manager.assistantSelectionContext, null);
  assert.equal(manager._streamingStopPromise, null);
  assert.equal(manager._streamingStopMode, null);
});

test("streaming cancellation aborts a BYOK fallback transcription request", async (t) => {
  const AudioManager = await loadManagerClass(t);
  const { manager } = createFinalizingManager(AudioManager);
  let abortCalls = 0;
  manager._activeTranscriptionAbortController = {
    abort() {
      abortCalls += 1;
    },
  };

  manager._requestStreamingCancellation();

  assert.equal(abortCalls, 1);
  assert.equal(manager._activeTranscriptionAbortController, null);
});

async function createStartingManager(t, providerName) {
  const AudioManager = await loadManagerClass(t);
  const { manager } = createFinalizingManager(AudioManager);
  const micRecovery = deferred();
  const startReached = deferred();
  const connection = deferred();
  const sent = [];
  const api = globalThis.window.electronAPI;
  const realtime = providerName.endsWith("-realtime");
  const prefix = realtime ? "dictationRealtime" : "deepgramStreaming";
  api[`${prefix}Start`] = () => {
    startReached.resolve();
    return connection.promise;
  };
  api[`${prefix}Send`] = (frame) => sent.push(frame);
  api[`${prefix}Stop`] = async () => ({ success: true });
  const eventNames = realtime
    ? [
        "DictationRealtimePartial",
        "DictationRealtimeFinal",
        "DictationRealtimeError",
        "DictationRealtimeSessionEnd",
      ]
    : [
        "DeepgramPartialTranscript",
        "DeepgramFinalTranscript",
        "DeepgramError",
        "DeepgramSessionEnd",
      ];
  for (const name of eventNames) api[`on${name}`] = () => () => {};

  const previousWorklet = globalThis.AudioWorkletNode;
  globalThis.AudioWorkletNode = class {
    constructor() {
      this.port = { postMessage() {} };
    }
    disconnect() {}
  };
  t.after(() => {
    if (previousWorklet === undefined) delete globalThis.AudioWorkletNode;
    else globalThis.AudioWorkletNode = previousWorklet;
  });
  const stream = {
    getAudioTracks: () => [{ getSettings: () => ({}) }],
    getTracks: () => [{ stop() {} }],
  };
  Object.assign(manager, {
    isRecording: false,
    isStreaming: false,
    preparedMicCapture: { take: async () => null },
    getAudioConstraints: async () => ({}),
    _acquireCaptureStream: async () => stream,
    startStreamingFallbackRecorder() {},
    getOrCreateAudioContext: async () => ({
      createMediaStreamSource: () => ({ connect() {}, disconnect() {} }),
      createAnalyser: () => ({}),
      audioWorklet: { addModule: async () => {} },
    }),
    getWorkletBlobUrl: () => "",
    getStreamingProvider: AudioManager.prototype.getStreamingProvider,
    getStreamingProviderName: () => providerName,
    getKeyterms: () => [],
    beginMicRecovery: () => micRecovery.promise,
    cleanupStreaming: async () => {
      manager.isStreaming = false;
    },
    cleanupPreview: async () => null,
    _markCaptureStreamReleased() {},
  });
  const start = manager.startStreamingRecording();
  while (!manager.streamingProcessor) await new Promise((resolve) => setImmediate(resolve));
  const processor = manager.streamingProcessor;
  return {
    manager,
    start,
    sent,
    connection,
    micRecovery,
    startReached,
    emit: (frame) => processor.port.onmessage({ data: frame }),
  };
}

for (const providerName of ["openai-realtime", "tinfoil-realtime"]) {
  test(`${providerName} holds opening audio until configuration completes, then flushes in order`, async (t) => {
    const harness = await createStartingManager(t, providerName);
    const frames = [
      new Int16Array([1]).buffer,
      new Int16Array([2]).buffer,
      new Int16Array([3]).buffer,
    ];
    harness.emit(frames[0]);
    assert.deepEqual(harness.sent, [], "mic recovery must not send audio to the warm socket");
    harness.micRecovery.resolve();
    await harness.startReached.promise;
    harness.emit(frames[1]);
    harness.emit("flushed");
    assert.deepEqual(harness.sent, [], "configuration/reconnect must finish before audio is sent");
    harness.connection.resolve({ success: true });
    assert.equal(await harness.start, true);
    harness.emit(frames[2]);
    assert.deepEqual(harness.sent, frames);
  });

  for (const outcome of ["cancel", "failure"]) {
    test(`${providerName} discards queued opening audio after ${outcome}`, async (t) => {
      const harness = await createStartingManager(t, providerName);
      harness.emit(new Int16Array([1]).buffer);
      harness.micRecovery.resolve();
      await harness.startReached.promise;
      harness.emit(new Int16Array([2]).buffer);
      const cancellation = outcome === "cancel" ? harness.manager.cancelStreamingRecording() : null;
      harness.connection.resolve(
        outcome === "cancel" ? { success: true } : { success: false, error: "Connection failed" }
      );
      assert.equal(await harness.start, false);
      if (cancellation) await cancellation;
      harness.emit(new Int16Array([3]).buffer);
      assert.deepEqual(harness.sent, [], "abandoned audio must never reach either socket");
    });
  }
}

test("Deepgram keeps sending audio eagerly while start is pending", async (t) => {
  const harness = await createStartingManager(t, "deepgram");
  const frame = new Int16Array([1]).buffer;
  harness.emit(frame);
  assert.deepEqual(harness.sent, [frame]);
  harness.micRecovery.resolve();
  await harness.startReached.promise;
  harness.connection.resolve({ success: true });
  assert.equal(await harness.start, true);
});

test("realtime startup buffers at most three seconds of opening audio", async (t) => {
  const harness = await createStartingManager(t, "openai-realtime");
  const frames = Array.from({ length: 4 }, () => new ArrayBuffer(16000 * 2));
  for (const frame of frames) harness.emit(frame);
  assert.deepEqual(harness.sent, []);
  harness.micRecovery.resolve();
  await harness.startReached.promise;
  harness.connection.resolve({ success: true });
  assert.equal(await harness.start, true);
  assert.deepEqual(harness.sent, frames.slice(0, 3));
  const liveFrame = new Int16Array([5]).buffer;
  harness.emit(liveFrame);
  assert.equal(
    harness.sent.at(-1),
    liveFrame,
    "reaching the queue limit must not block live audio"
  );
});

test("realtime batch fallback discards opening audio without flushing the old socket", async (t) => {
  const harness = await createStartingManager(t, "tinfoil-realtime");
  let batchStarts = 0;
  harness.manager.startRecording = async () => {
    batchStarts += 1;
    return true;
  };
  harness.emit(new Int16Array([1]).buffer);
  harness.micRecovery.resolve();
  await harness.startReached.promise;
  harness.connection.resolve({ success: false, code: "NO_API" });
  assert.equal(await harness.start, true);
  assert.equal(batchStarts, 1);
  harness.emit(new Int16Array([2]).buffer);
  assert.deepEqual(harness.sent, []);
});

test("cancelling streaming processing stays busy until an awaited transform exits", async (t) => {
  const AudioManager = await loadManagerClass(t);
  const { manager } = createFinalizingManager(AudioManager);
  const completions = [];
  let resolveTransform;
  let transformStarted = false;
  const transform = new Promise((resolve) => {
    resolveTransform = resolve;
  });
  manager.streamingFinalText = "raw transcript";
  manager.finalizeChineseScript = async () => {
    transformStarted = true;
    return transform;
  };
  manager.onTranscriptionComplete = (result) => completions.push(result);

  const stop = manager.stopStreamingRecording();
  while (!transformStarted) await new Promise((resolve) => setImmediate(resolve));

  assert.equal(manager.cancelProcessing(), true);
  assert.equal(manager.isProcessing, true);
  assert.equal(manager.getState().isFinalizingStreaming, true);
  resolveTransform("transformed transcript");
  assert.equal(await stop, true);

  assert.deepEqual(completions, []);
  assert.equal(manager.isProcessing, false);
  assert.equal(manager.getState().isFinalizingStreaming, false);
});

test("an older streaming session cannot clean up the active session listeners", async (t) => {
  const AudioManager = await loadManagerClass(t);
  const manager = Object.assign(Object.create(AudioManager.prototype), {
    _activeStreamingSessionId: 12,
    streamingCleanupFns: [() => assert.fail("stale cleanup ran")],
    streamingFinalText: "current transcript",
    streamingPartialText: "current partial",
    streamingTextBump: null,
    streamingTextDebounce: null,
  });

  manager.cleanupStreamingListeners(11);

  assert.equal(manager.streamingCleanupFns.length, 1);
  assert.equal(manager.streamingFinalText, "current transcript");
  assert.equal(manager.streamingPartialText, "current partial");
});
