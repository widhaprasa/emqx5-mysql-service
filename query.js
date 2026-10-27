const crypto = require("crypto");

function resolveTableName(value, fallback) {
  const name = value != null ? value : fallback;
  if (!/^[A-Za-z0-9_]+$/.test(name)) {
    throw new Error("Invalid MySQL table name: " + name);
  }
  return name;
}

// Env
const _userTable = resolveTableName(
  process.env.MYSQL_USER_TABLE,
  "mqtt_user"
);
const _aclTable = resolveTableName(process.env.MYSQL_ACL_TABLE, "mqtt_acl");

// SHA256
function buildSha256(password) {
  return crypto.createHash("sha256").update(password).digest("hex");
}

// Count Account
function countAccount(mysqlConnection, listener, group, callback) {
  let searchSql;
  let values;
  if (group != null) {
    searchSql = `SELECT count(username) FROM ${_userTable} WHERE listener = ? AND group_ = ?`;
    values = [listener, group];
  } else {
    searchSql = `SELECT count(username) FROM ${_userTable} WHERE listener = ?`;
    values = [listener];
  }
  mysqlConnection.query(searchSql, values, function (err, result) {
    if (err) {
      callback(0);
      return;
    }
    callback(result[0]["count(username)"]);
  });
}

// List Account
function listAccount(mysqlConnection, listener, group, callback) {
  let searchSql;
  let values;
  if (group != null) {
    searchSql = `SELECT username FROM ${_userTable} WHERE listener = ? AND group_ = ?`;
    values = [listener, group];
  } else {
    searchSql = `SELECT username FROM ${_userTable} WHERE listener = ?`;
    values = [listener];
  }
  mysqlConnection.query(searchSql, values, function (err, result) {
    if (err) {
      callback([]);
      return;
    }
    callback(result);
  });
}

// Account Exist
function accountExist(mysqlConnection, listener, username, callback) {
  username = username.trim();
  const searchSql = `SELECT listener, username FROM ${_userTable} WHERE listener = ? AND username = ?`;
  mysqlConnection.query(searchSql, [listener, username], function (
    err,
    result
  ) {
    if (err) {
      callback(-1);
      return;
    }
    callback(result.length > 0 ? 0 : -2);
  });
}

// Update Password Account
function updatePasswordAccount(
  mysqlConnection,
  listener,
  username,
  password,
  callback
) {
  username = username.trim();
  password = password.trim();

  const sha256 = buildSha256(password);
  const updateSql = `UPDATE ${_userTable} SET password_hash = ? WHERE listener = ? AND username = ?`;

  mysqlConnection.query(
    updateSql,
    [sha256, listener, username],
    function (err, result) {
      if (err) {
        callback(-1);
        return;
      }

      if (result.affectedRows != 1) {
        callback(-2);
        return;
      }
      callback(0);
    }
  );
}

// Delete Account
function deleteAccount(mysqlConnection, listener, username, callback) {
  username = username.trim();

  const deleteSql = `DELETE FROM ${_userTable} WHERE listener = ? AND username = ?`;
  mysqlConnection.query(deleteSql, [listener, username], function (err) {
    if (err) {
      callback(-1);
      return;
    }

    const deleteAclSql = `DELETE FROM ${_aclTable} WHERE listener = ? AND username = ?`;
    mysqlConnection.query(deleteAclSql, [listener, username], function (err) {
      if (err) {
        callback(-2);
        return;
      }
      callback(0);
    });
  });
}

// Delete Account by group
function deleteAccountByGroup(mysqlConnection, listener, group, callback) {
  group = group.trim();

  const deleteSql = `DELETE FROM ${_userTable} WHERE listener = ? AND group_ = ?`;
  mysqlConnection.query(deleteSql, [listener, group], function (err) {
    if (err) {
      callback(-1);
      return;
    }

    const deleteAclSql = `DELETE FROM ${_aclTable} WHERE listener = ? AND group_ = ?`;
    mysqlConnection.query(deleteAclSql, [listener, group], function (err) {
      if (err) {
        callback(-2);
        return;
      }
      callback(0);
    });
  });
}

// Clear Account
function clearAccount(mysqlConnection, listener, callback) {
  const deleteSql = `DELETE FROM ${_userTable} WHERE listener = ?`;
  mysqlConnection.query(deleteSql, [listener], function (err) {
    if (err) {
      callback(-1);
      return;
    }

    const deleteAclSql = `DELETE FROM ${_aclTable} WHERE listener = ?`;
    mysqlConnection.query(deleteAclSql, [listener], function (err) {
      if (err) {
        callback(-2);
        return;
      }
      callback(0);
    });
  });
}

// List Super User
function listSU(mysqlConnection, listener, callback) {
  const searchSql = `SELECT username FROM ${_userTable} WHERE listener = ? AND group_ = 'su'`;
  mysqlConnection.query(searchSql, [listener], function (err, result) {
    if (err) {
      callback([]);
      return;
    }
    callback(result);
  });
}

// Create Super User
function createSU(mysqlConnection, listener, username, password, callback) {
  username = username.trim();
  password = password.trim();

  const searchSql = `SELECT * FROM ${_userTable} WHERE listener = ? AND username = ?`;
  mysqlConnection.query(searchSql, [listener, username], function (
    err,
    result
  ) {
    if (err) {
      callback(-1);
      return;
    }

    if (result.length != 0) {
      callback(-2);
      return;
    }

    const sha256 = buildSha256(password);
    const insertSql =
      `INSERT INTO ${_userTable} (listener, group_, username, password_hash, is_superuser, created) ` +
      `VALUES (?, 'su', ?, ?, 1, NOW())`;

    mysqlConnection.query(
      insertSql,
      [listener, username, sha256],
      function (err) {
        if (err) {
          callback(-3);
          return;
        }
        callback(0);
      }
    );
  });
}

// List User
function listUser(mysqlConnection, listener, callback) {
  const searchSql = `SELECT username FROM ${_userTable} WHERE listener = ? AND group_ != 'su'`;
  mysqlConnection.query(searchSql, [listener], function (err, result) {
    if (err) {
      callback([]);
      return;
    }
    callback(result);
  });
}

// Create User
function createUser(
  mysqlConnection,
  listener,
  username,
  group,
  password,
  publishAcl,
  subscribeAcl,
  callback
) {
  username = username.trim();
  group = group.trim();
  password = password.trim();

  const searchSql = `SELECT * FROM ${_userTable} WHERE listener = ? AND username = ?`;
  mysqlConnection.query(searchSql, [listener, username], function (
    err,
    result
  ) {
    if (err) {
      callback(-1);
      return;
    }

    if (result.length != 0) {
      callback(-2);
      return;
    }

    const sha256 = buildSha256(password);
    const insertSql =
      `INSERT INTO ${_userTable} (listener, group_, username, password_hash, created) ` +
      `VALUES (?, ?, ?, ?, NOW())`;

    mysqlConnection.query(
      insertSql,
      [listener, group, username, sha256],
      function (err) {
        if (err) {
          callback(-3);
          return;
        }

        // Clear acls first
        const deleteAclSql = `DELETE FROM ${_aclTable} WHERE listener = ? AND username = ?`;
        mysqlConnection.query(deleteAclSql, [listener, username], function (
          err
        ) {
          if (err) {
            callback(-4);
            return;
          }

          // Add new acls
          const insertAcls = buildInsertAclSql(
            listener,
            username,
            group,
            publishAcl,
            subscribeAcl
          );
          if (insertAcls == null) {
            callback(0);
            return;
          }

          mysqlConnection.query(insertAcls.sql, insertAcls.values, function (
            err
          ) {
            if (err) {
              callback(-5);
              return;
            }
            callback(0);
          });
        });
      }
    );
  });
}

// Update Acl User
function updateAclUser(
  mysqlConnection,
  listener,
  username,
  publishAcl,
  subscribeAcl,
  callback
) {
  username = username.trim();

  const searchSql = `SELECT * FROM ${_userTable} WHERE listener = ? AND username = ?`;
  mysqlConnection.query(searchSql, [listener, username], function (
    err,
    result
  ) {
    if (err) {
      callback(-1);
      return;
    }

    if (result.length == 0) {
      callback(-2);
      return;
    }
    const group = result[0].group_;

    const deleteAcl = buildDeleteAclSql(
      listener,
      username,
      publishAcl,
      subscribeAcl
    );
    if (deleteAcl == null) {
      callback(0);
      return;
    }

    // Clear acls first
    mysqlConnection.query(deleteAcl.sql, deleteAcl.values, function (err) {
      if (err) {
        callback(-3);
        return;
      }

      // Add new acls
      const insertAcls = buildInsertAclSql(
        listener,
        username,
        group,
        publishAcl,
        subscribeAcl
      );
      if (insertAcls == null) {
        callback(0);
        return;
      }

      mysqlConnection.query(insertAcls.sql, insertAcls.values, function (
        err
      ) {
        if (err) {
          callback(-4);
          return;
        }
        callback(0);
      });
    });
  });
}

function buildInsertAclSql(
  listener,
  username,
  group,
  publishAcl,
  subscribeAcl
) {
  const rows = [];
  const values = [];

  // Publish
  if (publishAcl != null) {
    const publishTopicSet = new Set();
    for (let i = 0; i < publishAcl.length; ++i) {
      const topic = publishAcl[i];
      if (publishTopicSet.has(topic)) {
        continue;
      }
      publishTopicSet.add(topic);

      rows.push("(?, ?, ?, 'allow', 'publish', ?)");
      values.push(listener, group, username, topic);
    }
  }

  // Subscribe
  if (subscribeAcl != null) {
    const subscribeTopicSet = new Set();
    for (let i = 0; i < subscribeAcl.length; ++i) {
      const topic = subscribeAcl[i];
      if (subscribeTopicSet.has(topic)) {
        continue;
      }
      subscribeTopicSet.add(topic);

      rows.push("(?, ?, ?, 'allow', 'subscribe', ?)");
      values.push(listener, group, username, topic);
    }
  }

  if (rows.length === 0) {
    return null;
  }

  return {
    sql:
      `INSERT INTO ${_aclTable} (listener, group_, username, permission, action, topic) VALUES ` +
      rows.join(", "),
    values,
  };
}

function buildDeleteAclSql(listener, username, publishAcl, subscribeAcl) {
  const baseSql = `DELETE FROM ${_aclTable} WHERE listener = ? AND username = ?`;

  if (publishAcl != null && subscribeAcl != null) {
    return { sql: baseSql, values: [listener, username] };
  } else if (publishAcl != null) {
    return {
      sql: baseSql + " AND action = ?",
      values: [listener, username, "publish"],
    };
  } else if (subscribeAcl != null) {
    return {
      sql: baseSql + " AND action = ?",
      values: [listener, username, "subscribe"],
    };
  }
  return null;
}

module.exports = {
  countAccount,
  listAccount,
  accountExist,
  updatePasswordAccount,
  deleteAccount,
  deleteAccountByGroup,
  clearAccount,
  listSU,
  createSU,
  listUser,
  createUser,
  updateAclUser,
};
