"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const { resolveListenConfig } = require("../config.js");

test("defaults to 0.0.0.0:3000 when no HOST or PORT is set", () => {
  assert.deepEqual(resolveListenConfig({}), {
    host: "0.0.0.0",
    port: 3000,
  });
});

test("reads HOST and PORT from the environment", () => {
  assert.deepEqual(resolveListenConfig({ HOST: "127.0.0.1", PORT: "8080" }), {
    host: "127.0.0.1",
    port: 8080,
  });
});

test("falls back to the default port when PORT is invalid", () => {
  for (const port of ["abc", "-1", "70000", "80.5", "0x10", ""]) {
    assert.deepEqual(resolveListenConfig({ PORT: port }), {
      host: "0.0.0.0",
      port: 3000,
    });
  }
});

test("allows port 0 to request an ephemeral port", () => {
  assert.deepEqual(resolveListenConfig({ PORT: "0" }), {
    host: "0.0.0.0",
    port: 0,
  });
});

test("falls back to the default host when HOST is empty", () => {
  assert.deepEqual(resolveListenConfig({ HOST: "" }), {
    host: "0.0.0.0",
    port: 3000,
  });
});
