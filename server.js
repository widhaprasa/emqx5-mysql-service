"use strict";

// Express
const _express = require("express");
const EXPRESS_PORT = 3000;
const EXPRESS_HOST = "0.0.0.0";

// Underscore
const _ = require("underscore");

// Env
const _listener =
  process.env.DEFAULT_LISTENER != null ? process.env.DEFAULT_LISTENER : "";
const _authUsername = process.env.AUTH_USERNAME;
const _authPassword = process.env.AUTH_PASSWORD;

// Env MySQL
const _mysql = require("mysql");
const _mysqlConfig = {};
_mysqlConfig.host =
  process.env.MYSQL_HOST != null ? process.env.MYSQL_HOST : "localhost";
_mysqlConfig.port =
  process.env.MYSQL_PORT != null ? process.env.MYSQL_PORT : 3306;
_mysqlConfig.database =
  process.env.MYSQL_DB != null ? process.env.MYSQL_DB : "emqx_mysql";
_mysqlConfig.user =
  process.env.MYSQL_USER != null ? process.env.MYSQL_USER : "emqx_mysql";
_mysqlConfig.password =
  process.env.MYSQL_PASSWORD != null
    ? process.env.MYSQL_PASSWORD
    : "emqx_mysql";

// MySQL connection
const _mysqlPool = _mysql.createPool(_mysqlConfig);
_mysqlPool.getConnection((err, connection) => {
  if (err) {
    console.error("Error connecting to MySQL:", err.message);
    process.exit(1);
  }
});

// Query
const _query = require("./query.js");

// Parse
const _parse = require("./parse.js");

// App
const _app = _express();

// Add authentication
const auth = (req, res, next) => {
  if (req.path === "/health") {
    return next();
  }

  if (!_.isString(_authUsername) && !_.isString(_authPassword)) {
    return next();
  }

  const authHeader = req.headers["Authorization"];
  if (!authHeader) {
    return res.sendStatus(401);
  }

  const base64Credentials = authHeader.split(" ")[1];
  const credentials = Buffer.from(base64Credentials, "base64").toString("utf");
  const [username, password] = credentials.split(":");

  if (username === _authUsername && password === _authPassword) {
    return next();
  } else {
    return res.sendStatus(401);
  }
};
_app.use(auth);

_app.use(_express.json());

_app.get("/health", (req, res) => {
  res.send("ok");
});

_app.get("/account/count", (req, res) => {
  const query = req.query;
  let listener = _listener;
  if (_.isString(query.listener)) {
    listener = query.listener;
  }

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }

    _query.countAccount(connection, listener, function (result) {
      connection.release();
      res.send("" + result);
    });
  });
});

_app.get("/account/list", (req, res) => {
  const query = req.query;
  let listener = _listener;
  if (_.isString(query.listener)) {
    listener = query.listener;
  }

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }
    _query.listAccount(connection, listener, function (result) {
      connection.release();
      const arr = [];
      for (let i in result) {
        arr.push(result[i].username);
      }
      res.send(arr);
    });
  });
});

_app.post("/account/exist", (req, res) => {
  const body = req.body;
  let listener = _listener;
  if (_.isString(body.listener)) {
    listener = body.listener;
  }

  if (!_.isString(body.username)) {
    res.sendStatus(400);
    return;
  }
  const username = body.username;

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }

    _query.accountExist(connection, listener, username, function (code) {
      connection.release();
      res.send(code == 0);
    });
  });
});

_app.post("/account/update/password", (req, res) => {
  const body = req.body;
  let listener = _listener;
  if (_.isString(body.listener)) {
    listener = body.listener;
  }

  if (!_.isString(body.username) || !_.isString(body.password)) {
    res.sendStatus(400);
    return;
  }
  const username = body.username;
  const password = body.password;

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }
    _query.updatePasswordAccount(
      connection,
      listener,
      username,
      password,
      function (code) {
        connection.release();
        if (code == 0) {
          res.sendStatus(200);
        } else {
          res.status(500).json({ code });
        }
      }
    );
  });
});

_app.post("/account/delete", (req, res) => {
  const body = req.body;
  let listener = _listener;
  if (_.isString(body.listener)) {
    listener = body.listener;
  }

  if (!_.isString(body.username)) {
    res.sendStatus(400);
    return;
  }
  const username = body.username;

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }
    _query.deleteAccount(connection, listener, username, function (code) {
      connection.release();
      if (code == 0) {
        res.sendStatus(200);
      } else {
        res.status(500).json({ code });
      }
    });
  });
});

_app.post("/account/delete/group", (req, res) => {
  const body = req.body;
  let listener = _listener;
  if (_.isString(body.listener)) {
    listener = body.listener;
  }

  if (!_.isString(body.group)) {
    res.sendStatus(400);
    return;
  }
  const group = body.group;

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }
    _query.deleteAccountByGroup(connection, listener, group, function (code) {
      connection.release();
      if (code == 0) {
        res.sendStatus(200);
      } else {
        res.status(500).json({ code });
      }
    });
  });
});

_app.post("/account/clear", (req, res) => {
  const body = req.body;
  let listener = _listener;
  if (_.isString(body.listener)) {
    listener = body.listener;
  }

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }
    _query.clearAccount(connection, listener, function (code) {
      connection.release();
      if (code == 0) {
        res.sendStatus(200);
      } else {
        res.status(500).json({ code });
      }
    });
  });
});

_app.get("/su/list", (req, res) => {
  const query = req.query;
  let listener = _listener;
  if (_.isString(query.listener)) {
    listener = query.listener;
  }

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }
    _query.listSU(connection, listener, function (result) {
      connection.release();
      const arr = [];
      for (let i in result) {
        arr.push(result[i].username);
      }
      res.send(arr);
    });
  });
});

_app.post("/su/create", (req, res) => {
  const body = req.body;
  let listener = _listener;
  if (_.isString(body.listener)) {
    listener = body.listener;
  }

  if (!_.isString(body.username) || !_.isString(body.password)) {
    res.sendStatus(400);
    return;
  }
  const username = body.username;
  const password = body.password;

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }
    _query.createSU(connection, listener, username, password, function (code) {
      connection.release();
      if (code == 0) {
        res.sendStatus(200);
      } else {
        res.status(500).json({ code });
      }
    });
  });
});

_app.get("/user/list", (req, res) => {
  const query = req.query;
  let listener = _listener;
  if (_.isString(query.listener)) {
    listener = query.listener;
  }

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }
    _query.listUser(connection, listener, function (result) {
      connection.release();
      const arr = [];
      for (let i in result) {
        arr.push(result[i].username);
      }
      res.send(arr);
    });
  });
});

_app.post("/user/create", (req, res) => {
  const body = req.body;
  let listener = _listener;
  if (_.isString(body.listener)) {
    listener = body.listener;
  }

  if (
    !_.isString(body.username) ||
    !_.isString(body.group) ||
    !_.isString(body.password) ||
    !_.isArray(body.publish_acl) ||
    !_.isArray(body.subscribe_acl)
  ) {
    res.sendStatus(400);
    return;
  }
  const username = body.username;
  const group = body.group;
  const password = body.password;

  // Validate acls
  const publishAcl = _parse.parseAcl(body.publish_acl);
  const subscribeAcl = _parse.parseAcl(body.subscribe_acl);
  if (publishAcl == null || subscribeAcl == null) {
    res.sendStatus(400);
    return;
  }

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }
    _query.createUser(
      connection,
      listener,
      username,
      group,
      password,
      publishAcl,
      subscribeAcl,
      function (code) {
        connection.release();
        if (code == 0) {
          res.sendStatus(200);
        } else {
          res.status(500).json({ code });
        }
      }
    );
  });
});

_app.post("/user/update/acl", (req, res) => {
  const body = req.body;
  let listener = _listener;
  if (_.isString(body.listener)) {
    listener = body.listener;
  }

  if (!_.isString(body.username)) {
    res.sendStatus(400);
    return;
  }
  const username = body.username;

  // Validate acls
  const publishAcl = _parse.parseAcl(body.publish_acl);
  const subscribeAcl = _parse.parseAcl(body.subscribe_acl);

  _mysqlPool.getConnection((err, connection) => {
    if (err) {
      res.sendStatus(500);
      return;
    }
    _query.updateAclUser(
      connection,
      listener,
      username,
      publishAcl,
      subscribeAcl,
      function (code) {
        connection.release();
        if (code == 0) {
          res.sendStatus(200);
        } else {
          res.status(500).json({ code });
        }
      }
    );
  });
});

// Main
_app.listen(EXPRESS_PORT, EXPRESS_HOST, function () {
  console.log("##################################################");
  console.log("");
  console.log("Listening on " + EXPRESS_HOST + ":" + EXPRESS_PORT);
  console.log("");
  console.log("##################################################");
});
