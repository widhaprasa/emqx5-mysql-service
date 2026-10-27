"use strict";

const DEFAULT_HOST = "0.0.0.0";
const DEFAULT_PORT = 3000;

function parsePort(value) {
  if (typeof value !== "string" || !/^\d+$/.test(value)) {
    return DEFAULT_PORT;
  }

  const port = Number(value);
  if (port > 65535) {
    return DEFAULT_PORT;
  }
  return port;
}

function resolveListenConfig(env) {
  const source = env != null ? env : process.env;
  const host =
    typeof source.HOST === "string" && source.HOST !== ""
      ? source.HOST
      : DEFAULT_HOST;

  return { host, port: parsePort(source.PORT) };
}

module.exports = {
  resolveListenConfig,
  DEFAULT_HOST,
  DEFAULT_PORT,
};
