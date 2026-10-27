# emqx5-mysql-service

HTTP service for managing EMQX 5 MQTT accounts, ACLs and superusers backed by MySQL or MariaDB.

## Requirements

- Node.js 20 or newer
- MySQL 8 or newer, or MariaDB 10.6 or newer
- An EMQX-compatible `mqtt_user` and `mqtt_acl` schema

## Run

```sh
npm ci
npm start
```

The HTTP listener defaults to `0.0.0.0:3000`. Override it with `HOST` and `PORT`:

```sh
HOST=127.0.0.1 PORT=8080 npm start
```

## Configuration

| Variable           | Default      | Description                                              |
| ------------------ | ------------ | -------------------------------------------------------- |
| `HOST`             | `0.0.0.0`    | Address the HTTP server listens on                       |
| `PORT`             | `3000`       | Port the HTTP server listens on                          |
| `DEFAULT_LISTENER` | empty        | Listener used when a request does not supply one         |
| `AUTH_USERNAME`    | unset        | Basic auth username (set with `AUTH_PASSWORD` to enable) |
| `AUTH_PASSWORD`    | unset        | Basic auth password                                      |
| `MYSQL_HOST`       | `localhost`  | MySQL/MariaDB host                                       |
| `MYSQL_PORT`       | `3306`       | MySQL/MariaDB port                                       |
| `MYSQL_DB`         | `emqx_mysql` | Database name                                            |
| `MYSQL_USER`       | `emqx_mysql` | Database user                                            |
| `MYSQL_PASSWORD`   | `emqx_mysql` | Database password                                        |
| `MYSQL_USER_TABLE` | `mqtt_user`  | User table name                                          |
| `MYSQL_ACL_TABLE`  | `mqtt_acl`   | ACL table name                                           |

`GET /health` is always available without authentication.

## Docker

```sh
docker build -t emqx5-mysql-service .
docker run --rm -p 8080:3000 -e HOST=0.0.0.0 -e PORT=3000 \
  -e MYSQL_HOST=mysql -e MYSQL_DB=emqx_mysql \
  -e MYSQL_USER=emqx_mysql -e MYSQL_PASSWORD=secret \
  emqx5-mysql-service
```

## Test

```sh
npm test
```
