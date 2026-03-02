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
It compiles the source code and then launches the interactive DBMS console by running the `bdda.sgbd.SGBD` entry point with the configuration file `config/config.txt`.

---

## Supported Commands (Overview)

The CLI accepts a set of SQL-like commands, including:

- `CREATE TABLE ...`
- `DROP TABLE ...` / `DROP TABLES`
- `DESCRIBE TABLE ...` / `DESCRIBE TABLES`
- `INSERT INTO ...`
- `APPEND INTO ...`
- `SELECT ...`
- `DELETE ...`
- `UPDATE ...`
- `EXIT`

## Example CLI Commands (English dataset)

> This project provides a SQL-like CLI. The following script demonstrates table creation, inserts, selects, bulk loading from CSV, updates with complex predicates, and table management.
>  
> Each `EXIT` marks the end of an interactive session.

```sql
-- =========================================================
-- BDDA_SGBDR — Example Command Script (English naming)
-- Covers: CREATE / INSERT / SELECT / APPEND / UPDATE / DROP / DESCRIBE / EXIT
-- =========================================================


-- =========================
-- Session 1 — Basic usage
-- =========================

CREATE TABLE Singers (id:INT, name:VARCHAR(100), rating:FLOAT, review:CHAR(5))
SELECT s.name FROM Singers s

INSERT INTO Singers VALUES (1,"TaylorSwift",18.0,"Great")
INSERT INTO Singers VALUES (2,"BadBunny",15.8,"Avg")
SELECT * FROM Singers ss

INSERT INTO Singers VALUES (3,"JustinBieber",10,"Awful")
EXIT


-- =====================================
-- Session 2 — Filters and projections
-- =====================================

INSERT INTO Singers VALUES (4,"RodStewart",19.5,"Avg")

SELECT singers.id
FROM Singers singers
WHERE singers.review="Avg"

SELECT f.name, f.rating
FROM Singers f
WHERE f.rating>15 AND "Avg"<>f.review

SELECT A1.name, A1.name
FROM Singers A1
WHERE A1.review<"Great" AND 10>=A1.rating
EXIT


-- ==========================================
-- Session 3 — Create / describe / drop tables
-- ==========================================

CREATE TABLE Albums (id:INT, name:VARCHAR(100), singerId:INT, prodCode:CHAR(5), year:CHAR(4))
DESCRIBE TABLES

DROP TABLE Albums
DESCRIBE TABLES
EXIT


-- ==================================================
-- Session 4 — CSV bulk load + complex UPDATE + SELECT
-- ==================================================

CREATE TABLE T (C1:CHAR(3), C2:INT, C3:INT, C4:INT, C5:VARCHAR(3))

-- Bulk load: append all records from a CSV file into table T
APPEND INTO T ALLRECORDS(T.csv)

-- Update with a complex predicate (numeric + string comparisons)
UPDATE T t
SET C1="xxx", C5="yb", C2=1
WHERE
  t.C2>=61
  AND 32600>t.C3
  AND t.C3>=t.C4
  AND t.C2<=t.C3
  AND "ter">=t.C1
  AND t.C1>=t.C5
  AND t.C1<>"krw"
  AND t.C2>=75
  AND 1243>=t.C4
  AND t.C1>"egw"

-- Projection + predicate
SELECT t.C1, t.C2 FROM T t WHERE C1>="xxx"
EXIT


-- ==========================================
-- Session 5 — Another UPDATE + SELECT *
-- ==========================================

UPDATE T t
SET C3=1999, C4=660
WHERE
  t.C2<=t.C4
  AND t.C5="egw"
  AND t.C1>"egw"
  AND 660<=t.C3
  AND "ewr"=t.C1
  AND 893>t.C4
  AND 64=t.C2
  AND t.C2<=75
  AND t.C1>=t.C5

SELECT * FROM T t WHERE t.C3<=765
EXIT
```
