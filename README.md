# BDDA_SGBDR — Mini DBMS (Java)

Academic project: a **simplified DBMS** implemented in **Java** (disk storage + buffer management + tables/records + SQL-like commands + query operators).  
Built as part of a BDDA / SGBDR course.

---

## Features (Mini-DBMS scope)
- **Table management**: create / describe / drop
- **Insert / append** records
- **Queries** (SQL-like `SELECT`) using query operators (scan, selection, projection, …)
- **DELETE / UPDATE**
- Persistent state handled through the project managers (depending on config)

> The system runs in an interactive console loop (CLI).

---

## Project Structure
Typical folders in this repository:
- `src/bdda/core` : low-level components (disk / pages / buffers / config)
- `src/bdda/storage` : relations, records, schema, storage layer
- `src/bdda/query` : query operators (scan/select/project/iterators…)
- `src/bdda/manager` : global managers (catalog / state)
- `src/bdda/sgbd` : entry point (command loop)
- `tests/bdda` and/or `src/bdda/tests` : test code (depending on organization)
- `config/` : configuration files
- `libs/junit` : JUnit library (if used)
- `BinData/` : generated binary/data files

---

## Requirements
- **JDK installed** (Java 11+ recommended)
- On Windows: quick run via `run.bat`
- (Optional) JUnit if you want to run tests

---

## Run (Windows)
A script is provided at the root of the repository:

```bat
run.bat
```
It compiles the project and starts the main class bdda.sgbd.SGBD using config/config.txt.

Supported Commands (overview)

The command parser includes (at least):

CREATE TABLE ...

DROP TABLE ... / DROP TABLES

DESCRIBE TABLE ... / DESCRIBE TABLES

INSERT INTO ...

APPEND INTO ...

SELECT ...

DELETE ...

UPDATE ...

EXIT

Example (indicative)
CREATE TABLE R (X:INT, C3:FLOAT, BLA:CHAR(10))
DESCRIBE TABLES
INSERT INTO R VALUES (1, 3.14, "hello")
SELECT * FROM R
EXIT

Exact syntax depends on the parser implementation in src/bdda/sgbd/SGBD.java.

.gitignore

Build artifacts and generated files are ignored (e.g. bin/, *.class, etc.).
