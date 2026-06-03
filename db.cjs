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
  // If a previous version of the table exists without the SOURCE_AMOUNT_ILS column
  // (pre-overrides schema), drop it. There is no production data to preserve yet —
  // the live server hasn't run Sync against this schema.
  const tbl = d
    .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='FCT_BUDGET_TARGET_BY_DEPT_ACCT'")
    .get();
  if (tbl) {
    const cols = d.prepare("PRAGMA table_info('FCT_BUDGET_TARGET_BY_DEPT_ACCT')").all();
    const hasNew = cols.some((c) => c.name === 'SOURCE_AMOUNT_ILS');
    if (!hasNew) {
      console.warn('[db] Dropping old-schema FCT_BUDGET_TARGET_BY_DEPT_ACCT (no user overrides existed). Re-run Sync to repopulate.');
      d.exec('DROP TABLE FCT_BUDGET_TARGET_BY_DEPT_ACCT');
    }
  }

  d.exec(`
    CREATE TABLE IF NOT EXISTS FCT_BUDGET_TARGET_BY_DEPT_ACCT (
      FISCAL_YEAR                  INTEGER NOT NULL,
      DEPARTMENT                   TEXT,
      LOCATION                     TEXT,
      CURRENCY                     TEXT,
      ACCOUNT_NUMBER               TEXT,
      ACCOUNT_NAME                 TEXT,
      NETSUITE_INTERNAL_NUMBER     INTEGER,
      SOURCE_AMOUNT_ILS            REAL,
      USER_OVERRIDE_AMOUNT_ILS     REAL,
      USER_OVERRIDE_PCT            REAL,
      USER_EDITED_BY               TEXT,
      USER_EDITED_AT               TEXT,
      ANNUAL_BUDGET_TARGET_AMOUNT  REAL GENERATED ALWAYS AS (
        COALESCE(
          USER_OVERRIDE_AMOUNT_ILS,
          SOURCE_AMOUNT_ILS * (1 + COALESCE(USER_OVERRIDE_PCT, 0) / 100.0),
          SOURCE_AMOUNT_ILS
        )
      ) STORED,
      SUBSIDIARY_ID                INTEGER NOT NULL,
      SOURCE_SYNCED_AT             TEXT NOT NULL DEFAULT (datetime('now')),
      LOADED_AT                    TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (FISCAL_YEAR, SUBSIDIARY_ID, DEPARTMENT, LOCATION, ACCOUNT_NUMBER, CURRENCY)
    ) STRICT;

    CREATE INDEX IF NOT EXISTS IDX_BUDGET_TARGET_YEAR_SUB
      ON FCT_BUDGET_TARGET_BY_DEPT_ACCT (FISCAL_YEAR, SUBSIDIARY_ID);

    CREATE TABLE IF NOT EXISTS BUDGET_TARGET_EDIT_LOG (
      ID              INTEGER PRIMARY KEY AUTOINCREMENT,
      EDITED_AT       TEXT    NOT NULL DEFAULT (datetime('now')),
      EDITED_BY       TEXT    NOT NULL,
      FISCAL_YEAR     INTEGER NOT NULL,
      SUBSIDIARY_ID   INTEGER NOT NULL,
      DEPARTMENT      TEXT,
      LOCATION        TEXT,
      ACCOUNT_NUMBER  TEXT,
      CURRENCY        TEXT,
      FIELD_NAME      TEXT    NOT NULL,
      OLD_VALUE       TEXT,
      NEW_VALUE       TEXT
    ) STRICT;

    CREATE INDEX IF NOT EXISTS IDX_EDIT_LOG_AT
      ON BUDGET_TARGET_EDIT_LOG (EDITED_AT);
    CREATE INDEX IF NOT EXISTS IDX_EDIT_LOG_SCOPE
      ON BUDGET_TARGET_EDIT_LOG (FISCAL_YEAR, SUBSIDIARY_ID, EDITED_AT);
  `);

  // Idempotent migration: add monthly breakdown column (JSON string in SQLite).
  const cols2 = d.prepare("PRAGMA table_info('FCT_BUDGET_TARGET_BY_DEPT_ACCT')").all();
  if (!cols2.some((c) => c.name === 'MONTHLY_SOURCE_ILS')) {
    d.exec(`ALTER TABLE FCT_BUDGET_TARGET_BY_DEPT_ACCT ADD COLUMN MONTHLY_SOURCE_ILS TEXT;`);
  }
}

module.exports = { getDb, DB_PATH };
