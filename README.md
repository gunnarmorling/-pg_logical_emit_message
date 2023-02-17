# Logical Decoding Message Examples

*NOTE:* This project has been moved over to the [Decodable examples repository](https://github.com/decodableco/examples/tree/main/postgres-logical-decoding) and this repository is not maintained any longer.

Several examples demonstrating the usage of Postgres logical decoding messages,
as emitted using the [pg_logical_emit_message()](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-REPLICATION) function.

## Requirements

* Java 11
* Docker

## Build

Run the following command to build this project:

```
mvn clean verify
```

Pass the `-Dquick` option to skip all non-essential plug-ins and create the output artifact as quickly as possible:

```
mvn clean verify -Dquick
```

Run the following command to format the source code and organize the imports as per the project's conventions:

```
mvn process-sources
```

This code base is available under the Apache License, version 2.

## Running the Examples

Start a Postgres database using the Docker Compose file in this directory:

```
docker compose up
```

Start a Postgres client session:

```
docker run --tty --rm -i \
    --network logical-decoding-network \
    quay.io/debezium/tooling:1.2 \
    bash -c 'pgcli postgresql://postgresuser:postgrespw@order-db:5432/orderdb'
```

Import the project into your IDE and run the main class `dev.morling.demos.logicaldecoding.Main`. 

In the Postgres client sesion run:

```
CREATE TABLE data(id serial primary key, data text);

BEGIN;
SELECT * FROM pg_logical_emit_message(true, 'audit', '{ "user" : "John", "client_ip" : "192.168.1.1" }');
INSERT INTO data(data) VALUES('aaa');
INSERT INTO data(data) VALUES('bbb');
COMMIT;
```

In the logs of the main class observe how the cange events for the two rows in the `data` table are enriched with the audit metadata provided via `pg_logical_emit_message()` as part of the same transaction before.

## License

This code base is available under the Apache License, version 2.
