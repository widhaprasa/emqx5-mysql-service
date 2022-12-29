const _ = require("underscore");

function parseAcl(input) {
  if (!_.isArray(input)) {
    return null;
  }

  const result = [];
  for (let i in input) {
    const acl = input[i];
    if (_.isString(acl)) {
      result.push(acl);
    } else if (_.isObject(acl)) {
      const topic = acl["topic"];
      if (_.isString(topic)) {
        result.push(topic);
      }
    }
  }
  return result;
}

module.exports = {
  parseAcl,
};
