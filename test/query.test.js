"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");
const path = require("node:path");

const query = require("../query.js");

const PROJECT_ROOT = path.resolve(__dirname, "..");

const SECRET_SHA256 =
  "2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b";

function makeConnection(selectResult = []) {
  const calls = [];

  const connection = {
    calls,
    query(sql, values, callback) {
      if (typeof values === "function") {
        callback = values;
        values = [];
      }

      calls.push({ sql, values });

      let result;
      if (/^\s*SELECT/i.test(sql)) {
        result =
          typeof selectResult === "function"
            ? selectResult(sql)
            : selectResult;
      } else {
        result = { affectedRows: 1 };
      }

      process.nextTick(() => callback(null, result));
    },
  };

  return connection;
}

function run(executor) {
  return new Promise((resolve) => {
    executor((value) => resolve(value));
  });
}

function assertParameterized(call, expectedValues) {
  assert.deepEqual(call.values, expectedValues);
  assert.ok(call.sql.includes("?"), `expected placeholders in: ${call.sql}`);

  for (const value of expectedValues) {
    if (typeof value === "string" && value.length > 0) {
      assert.ok(
        !call.sql.includes(value),
        `SQL must not contain value ${JSON.stringify(value)}: ${call.sql}`
      );
    }
  }
}

test("countAccount filters by listener and group using placeholders", async () => {
  const connection = makeConnection([{ "count(username)": 7 }]);

  const count = await run((done) =>
    query.countAccount(connection, "l1", "g1", done)
  );

  assert.equal(count, 7);
  assert.equal(connection.calls.length, 1);
  assertParameterized(connection.calls[0], ["l1", "g1"]);
});

test("countAccount omits the group predicate when group is null", async () => {
  const connection = makeConnection([{ "count(username)": 2 }]);

  const count = await run((done) =>
    query.countAccount(connection, "l1", null, done)
  );

  assert.equal(count, 2);
  assert.deepEqual(connection.calls[0].values, ["l1"]);
  assert.ok(!connection.calls[0].sql.includes("group_"));
});

test("user input cannot alter SQL (injection payload stays in values)", async () => {
  const payload = "' OR '1'='1";
  const connection = makeConnection([]);

  await run((done) => query.accountExist(connection, payload, "alice", done));

  const call = connection.calls[0];
  assert.deepEqual(call.values, [payload, "alice"]);
  assert.ok(!call.sql.includes(payload));
});

test("accountExist trims username before querying", async () => {
  const connection = makeConnection([{}]);

  const code = await run((done) =>
    query.accountExist(connection, "l1", "  alice  ", done)
  );

  assert.equal(code, 0);
  assert.deepEqual(connection.calls[0].values, ["l1", "alice"]);
});

test("listAccount returns rows and parameterizes the group", async () => {
  const rows = [{ username: "alice" }];
  const connection = makeConnection(rows);

  const result = await run((done) =>
    query.listAccount(connection, "l1", "g1", done)
  );

  assert.deepEqual(result, rows);
  assertParameterized(connection.calls[0], ["l1", "g1"]);
});

test("updatePasswordAccount stores the trimmed password hash as a parameter", async () => {
  const connection = makeConnection([]);

  const code = await run((done) =>
    query.updatePasswordAccount(connection, "l1", "alice", " secret ", done)
  );

  assert.equal(code, 0);
  const call = connection.calls[0];
  assert.deepEqual(call.values, [SECRET_SHA256, "l1", "alice"]);
  assert.ok(!call.sql.includes("secret"));
});

test("deleteAccount deletes the user and ACL rows by listener and username", async () => {
  const connection = makeConnection([]);

  const code = await run((done) =>
    query.deleteAccount(connection, "l1", "  alice  ", done)
  );

  assert.equal(code, 0);
  assert.equal(connection.calls.length, 2);
  assertParameterized(connection.calls[0], ["l1", "alice"]);
  assertParameterized(connection.calls[1], ["l1", "alice"]);
});

test("deleteAccountByGroup deletes users and ACLs by group", async () => {
  const connection = makeConnection([]);

  const code = await run((done) =>
    query.deleteAccountByGroup(connection, "l1", "  g1  ", done)
  );

  assert.equal(code, 0);
  assert.equal(connection.calls.length, 2);
  assertParameterized(connection.calls[0], ["l1", "g1"]);
  assertParameterized(connection.calls[1], ["l1", "g1"]);
});

test("clearAccount deletes all users and ACLs for a listener", async () => {
  const connection = makeConnection([]);

  const code = await run((done) =>
    query.clearAccount(connection, "l1", done)
  );

  assert.equal(code, 0);
  assert.equal(connection.calls.length, 2);
  assertParameterized(connection.calls[0], ["l1"]);
  assertParameterized(connection.calls[1], ["l1"]);
});

test("listSU lists superusers for a listener", async () => {
  const rows = [{ username: "root" }];
  const connection = makeConnection(rows);

  const result = await run((done) =>
    query.listSU(connection, "l1", done)
  );

  assert.deepEqual(result, rows);
  assertParameterized(connection.calls[0], ["l1"]);
});

test("listUser lists non-superusers for a listener", async () => {
  const rows = [{ username: "alice" }];
  const connection = makeConnection(rows);

  const result = await run((done) =>
    query.listUser(connection, "l1", done)
  );

  assert.deepEqual(result, rows);
  assertParameterized(connection.calls[0], ["l1"]);
});

test("createSU inserts the superuser with parameterized values", async () => {
  const connection = makeConnection([]);

  const code = await run((done) =>
    query.createSU(connection, "l1", "root", "secret", done)
  );

  assert.equal(code, 0);
  assert.equal(connection.calls.length, 2);
  assertParameterized(connection.calls[0], ["l1", "root"]);
  assertParameterized(connection.calls[1], ["l1", "root", SECRET_SHA256]);
});

test("createUser inserts the user and deduplicated ACL rows as parameters", async () => {
  const connection = makeConnection([]);

  const code = await run((done) =>
    query.createUser(
      connection,
      "l1",
      "alice",
      "g1",
      "secret",
      ["t/a", "t/a", "t/b"],
      ["t/c"],
      done
    )
  );

  assert.equal(code, 0);
  assert.equal(connection.calls.length, 4);

  assertParameterized(connection.calls[0], ["l1", "alice"]);
  assertParameterized(connection.calls[1], [
    "l1",
    "g1",
    "alice",
    SECRET_SHA256,
  ]);
  assertParameterized(connection.calls[2], ["l1", "alice"]);
  assertParameterized(connection.calls[3], [
    "l1",
    "g1",
    "alice",
    "t/a",
    "l1",
    "g1",
    "alice",
    "t/b",
    "l1",
    "g1",
    "alice",
    "t/c",
  ]);
});

test("ACL topics are inserted as data, never as SQL", async () => {
  const topic = "x'; DROP TABLE mqtt_acl; --";
  const connection = makeConnection([]);

  const code = await run((done) =>
    query.createUser(
      connection,
      "l1",
      "alice",
      "g1",
      "secret",
      [topic],
      null,
      done
    )
  );

  assert.equal(code, 0);
  const aclCall = connection.calls[3];
  assert.ok(!aclCall.sql.includes(topic));
  assert.ok(aclCall.values.includes(topic));
});

test("updateAclUser replaces the user's ACL rows using parameters", async () => {
  const connection = makeConnection([{ group_: "g1" }]);

  const code = await run((done) =>
    query.updateAclUser(
      connection,
      "l1",
      "alice",
      ["t/a"],
      ["t/c"],
      done
    )
  );

  assert.equal(code, 0);
  assert.equal(connection.calls.length, 3);

  assertParameterized(connection.calls[0], ["l1", "alice"]);
  assertParameterized(connection.calls[1], ["l1", "alice"]);
  assertParameterized(connection.calls[2], [
    "l1",
    "g1",
    "alice",
    "t/a",
    "l1",
    "g1",
    "alice",
    "t/c",
  ]);
});

test("updateAclUser scopes the ACL delete to the changed action with a parameter", async () => {
  const connection = makeConnection([{ group_: "g1" }]);

  const code = await run((done) =>
    query.updateAclUser(connection, "l1", "alice", null, ["t/c"], done)
  );

  assert.equal(code, 0);
  assert.equal(connection.calls.length, 3);

  const deleteCall = connection.calls[1];
  assert.match(deleteCall.sql, /action = \?/);
  assert.deepEqual(deleteCall.values, ["l1", "alice", "subscribe"]);

  assertParameterized(connection.calls[2], [
    "l1",
    "g1",
    "alice",
    "t/c",
  ]);
});

test("rejects an invalid MySQL table name from the environment", () => {
  let error;

  try {
    execFileSync(process.execPath, ["-e", 'require("./query.js")'], {
      cwd: PROJECT_ROOT,
      env: { ...process.env, MYSQL_USER_TABLE: "mqtt_user; DROP TABLE x" },
      stdio: "pipe",
    });
  } catch (err) {
    error = err;
  }

  assert.ok(error, "expected query.js to reject the table name");
  assert.match(error.stderr.toString(), /Invalid MySQL table name/);
});

test("uses valid custom table names from the environment", () => {
  const output = execFileSync(
    process.execPath,
    [
      "-e",
      'const q = require("./query.js");' +
        'const conn = { query(sql) { console.log(sql); } };' +
        'q.clearAccount(conn, "l1", function () {});',
    ],
    {
      cwd: PROJECT_ROOT,
      env: {
        ...process.env,
        MYSQL_USER_TABLE: "custom_users",
        MYSQL_ACL_TABLE: "custom_acls",
      },
    }
  );

  assert.match(output.toString(), /DELETE FROM custom_users/);
});
