const crypto = require("crypto");

// Env
const _userTable =
  process.env.MYSQL_USER_TABLE != null
    ? process.env.MYSQL_USER_TABLE
    : "mqtt_user";
const _aclTable =
  process.env.MYSQL_ACL_TABLE != null
    ? process.env.MYSQL_ACL_TABLE
    : "mqtt_acl";

// SHA256
function buildSha256(password) {
  return crypto.createHash("sha256").update(password).digest("hex");
}

// Count Account
function countAccount(mysqlConnection, listener, callback) {
  const searchSql = `SELECT count(username) FROM ${_userTable} WHERE listener = '${listener}'`;
  mysqlConnection.query(searchSql, function (err, result) {
    if (err) {
      callback(0);
      return;
    }
    callback(result[0]["count(username)"]);
  });
}

// List Account
function listAccount(mysqlConnection, listener, callback) {
  const searchSql = `SELECT username FROM ${_userTable} WHERE listener = '${listener}'`;
  mysqlConnection.query(searchSql, function (err, result) {
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
  const searchSql = `SELECT listener, username FROM ${_userTable} WHERE listener = '${listener}' AND username = '${username}'`;
  mysqlConnection.query(searchSql, function (err, result) {
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
  const updateSql = `UPDATE ${_userTable} SET password = '${sha256}' WHERE listener = '${listener}' AND username = '${username}'`;

  mysqlConnection.query(updateSql, function (err, result) {
    if (err) {
      callback(-1);
      return;
    }

    if (result.affectedRows != 1) {
      callback(-2);
      return;
    }
    callback(0);
  });
}

// Delete Account
function deleteAccount(mysqlConnection, listener, username, callback) {
  username = username.trim();

  const deleteSql = `DELETE FROM ${_userTable} WHERE listener = '${listener}' AND username = '${username}'`;
  mysqlConnection.query(deleteSql, function (err, result) {
    if (err) {
      callback(-1);
      return;
    }

    const deleteAclSql = `DELETE FROM ${_aclTable} WHERE listener = '${listener}' AND username = '${username}'`;
    mysqlConnection.query(deleteAclSql, function (err, result) {
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

  const deleteSql = `DELETE FROM ${_userTable} WHERE listener = '${listener}' AND group_ = '${group}'`;
  mysqlConnection.query(deleteSql, function (err, result) {
    if (err) {
      callback(-1);
      return;
    }

    const deleteAclSql = `DELETE FROM ${_aclTable} WHERE listener = '${listener}' AND group_ = '${group}'`;
    mysqlConnection.query(deleteAclSql, function (err, result) {
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
  const deleteSql = `DELETE FROM ${_userTable} WHERE listener = '${listener}'`;
  mysqlConnection.query(deleteSql, function (err, result) {
    if (err) {
      callback(-1);
      return;
    }

    const deleteAclSql = `DELETE FROM ${_aclTable} WHERE listener = '${listener}'`;
    mysqlConnection.query(deleteAclSql, function (err, result) {
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
  const searchSql = `SELECT username FROM ${_userTable} WHERE listener = '${listener}' AND group_ = 'su'`;
  mysqlConnection.query(searchSql, function (err, result) {
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

  const searchSql = `SELECT * FROM ${_userTable} WHERE listener = '${listener}' AND username = '${username}'`;
  mysqlConnection.query(searchSql, function (err, result) {
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
      `VALUES ('${listener}', 'su', '${username}', '${sha256}', 1, NOW())`;

    mysqlConnection.query(insertSql, function (err, result) {
      if (err) {
        callback(-3);
        return;
      }
      callback(0);
    });
  });
}

// List User
function listUser(mysqlConnection, listener, callback) {
  const searchSql = `SELECT username FROM ${_userTable} WHERE listener = '${listener}' AND group_ != 'su'`;
  mysqlConnection.query(searchSql, function (err, result) {
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

  const searchSql = `SELECT * FROM ${_userTable} WHERE listener = '${listener}' AND username = '${username}'`;
  mysqlConnection.query(searchSql, function (err, result) {
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
      `VALUES ('${listener}', '${group}', '${username}', '${sha256}', NOW())`;

    mysqlConnection.query(insertSql, function (err, result) {
      if (err) {
        callback(-3);
        return;
      }

      // Clear acls first
      const deleteAclSql = `DELETE FROM ${_aclTable} WHERE listener = '${listener}' AND username = '${username}'`;
      mysqlConnection.query(deleteAclSql, function (err, result) {
        if (err) {
          callback(-4);
          return;
        }

        // Add new acls
        const insertAclsSql = buildInsertAclSql(
          listener,
          username,
          group,
          publishAcl,
          subscribeAcl
        );
        if (insertAclsSql == null) {
          callback(0);
          return;
        }

        mysqlConnection.query(insertAclsSql, function (err, result) {
          if (err) {
            callback(-5);
            return;
          }
          callback(0);
        });
      });
    });
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

  const searchSql = `SELECT * FROM ${_userTable} WHERE listener = '${listener}' AND username = '${username}'`;
  mysqlConnection.query(searchSql, function (err, result) {
    if (err) {
      callback(-1);
      return;
    }

    if (result.length == 0) {
      callback(-2);
      return;
    }
    const group = result[0].group_;

    const deleteAclSql = buildDeleteAclSql(
      listener,
      username,
      publishAcl,
      subscribeAcl
    );
    if (deleteAclSql == null) {
      callback(0);
      return;
    }

    // Clear acls first
    mysqlConnection.query(deleteAclSql, function (err, result) {
      if (err) {
        callback(-3);
        return;
      }

      // Add new acls
      const insertAclsSql = buildInsertAclSql(
        listener,
        username,
        group,
        publishAcl,
        subscribeAcl
      );
      if (insertAclsSql == null) {
        callback(0);
        return;
      }

      mysqlConnection.query(insertAclsSql, function (err, result) {
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
  let values = "";

  // Publish
  if (publishAcl != null) {
    const publishTopicSet = new Set();
    for (let i = 0; i < publishAcl.length; ++i) {
      const topic = publishAcl[i];
      if (publishTopicSet.has(topic)) {
        continue;
      }
      publishTopicSet.add(topic);

      if ("" !== values) {
        values += ", ";
      }
      values += `('${listener}', '${group}', '${username}', 'allow', 'publish', '${topic}')`;
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

      if ("" !== values) {
        values += ", ";
      }
      values += `('${listener}', '${group}', '${username}', 'allow', 'subscribe', '${topic}')`;
    }
  }

  if ("" === values) {
    return null;
  }

  return (
    `INSERT INTO ${_aclTable} (listener, group_, username, permission, action, topic) VALUES ` +
    values
  );
}

function buildDeleteAclSql(listener, username, publishAcl, subscribeAcl) {
  if (publishAcl != null && subscribeAcl != null) {
    return `DELETE FROM ${_aclTable} WHERE listener = '${listener}' AND username = '${username}'`;
  } else if (publishAcl != null) {
    return `DELETE FROM ${_aclTable} WHERE listener = '${listener}' AND username = '${username}' AND action = 'publish'`;
  } else if (subscribeAcl != null) {
    return `DELETE FROM ${_aclTable} WHERE listener = '${listener}' AND username = '${username}' AND action = 'subscribe'`;
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
