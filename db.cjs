// SQLite database for the bank-dashboard server.
// File: data/banks-dashboard.db (gitignored).
// Read this once on demand via getDb(); the connection is cached.

const path = require('path');
const fs = require('fs');
const Database = require('better-sqlite3');

const DB_DIR = path.resolve(__dirname, 'data');
const DB_PATH = path.join(DB_DIR, 'banks-dashboard.db');

let db = null;

function getDb() {
  if (db) return db;
  if (!fs.existsSync(DB_DIR)) fs.mkdirSync(DB_DIR, { recursive: true });
  db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');
  migrate(db);
  return db;
}

function migrate(d) {
  d.exec(`
    CREATE TABLE IF NOT EXISTS FCT_BUDGET_TARGET_BY_DEPT_ACCT (
      FISCAL_YEAR                  INTEGER NOT NULL,
      DEPARTMENT                   TEXT,
      LOCATION                     TEXT,
      CURRENCY                     TEXT,
      ACCOUNT_NUMBER               TEXT,
      ACCOUNT_NAME                 TEXT,
      NETSUITE_INTERNAL_NUMBER     INTEGER,
      ANNUAL_BUDGET_TARGET_AMOUNT  REAL,
      SUBSIDIARY_ID                INTEGER NOT NULL,
      LOADED_AT                    TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (FISCAL_YEAR, SUBSIDIARY_ID, DEPARTMENT, LOCATION, ACCOUNT_NUMBER, CURRENCY)
    ) STRICT;

    CREATE INDEX IF NOT EXISTS IDX_BUDGET_TARGET_YEAR_SUB
      ON FCT_BUDGET_TARGET_BY_DEPT_ACCT (FISCAL_YEAR, SUBSIDIARY_ID);
  `);
}

module.exports = { getDb, DB_PATH };
