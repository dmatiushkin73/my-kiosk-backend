from pathlib import Path
from core.appmodule import AppModule
from core.logger import Logger
from core import utils
import sqlite3
import time
from db.model import AccessLevel, User


class Database(AppModule):
    """Implements infrastructure and logic layer between the application modules and SQLite3 database"""
    MYNAME = 'db'
    SCHEMA_V1 = 'schema.v1.txt'
    REQ_CFG_OPTIONS = ['name', 'schema_version']

    def __init__(self, config_data: dict, logger: Logger, data_dir: Path, schema_dir: Path):
        super().__init__(Database.MYNAME, config_data, logger)
        self._data_dir = data_dir
        self._schema_dir = schema_dir
        self._db = None

    def _get_my_required_cfg_options(self) -> list:
        return Database.REQ_CFG_OPTIONS

    def start(self):
        """First method to be invoked after creation, opens connection to the database, initializes it if needed"""
        dbfile = self._data_dir.joinpath(self._config['name'])
        try:
            self._db = sqlite3.connect(dbfile)
        except sqlite3.Error as e:
            self._logger.critical(f"Failed to connect to the database - {str(e)}")
            raise utils.DbBroken("Failed to connect to the database")
        cur = self._db.cursor()
        cur.execute("PRAGMA user_version")
        schema_version = cur.fetchone()[0]
        self._logger.info(f"Database schema version={schema_version}, configured={self._config['schema_version']}")
        if schema_version < 1:
            self._logger.info("Database is empty, creating the schema")
            v1file = Path(self._schema_dir).joinpath(Database.SCHEMA_V1)
            self._load_and_apply_schema(cur, v1file, 1)
            cur.execute("PRAGMA foreign_keys = true")
            cur.execute("PRAGMA ignore_check_constraints = false")
        elif schema_version < self._config['schema_version']:
            for v in range(schema_version+1, self._config['schema_version']+1):
                vfile = Path(self._schema_dir).joinpath(Database.SCHEMA_V1.replace('v1', f"v{v}", 1))
                self._load_and_apply_schema(cur, vfile, v)

    def stop(self):
        if self._db:
            self._db.close()

    def _load_and_apply_schema(self, cur: sqlite3.Cursor, schema_file: Path, v: int):
        """Tries to load the schema SQL sequence from the text file and execute it"""
        if not schema_file.exists():
            self._logger.critical(f"FIle with database schema v{v} is not found")
            raise utils.DbBroken(f"Schema v{v} not found")
        with open(schema_file) as f:
            schema = f.read()
            try:
                cur.executescript(schema)
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.critical(f"Failed to apply schema v{v} - {str(e)}")
                raise utils.DbBroken(f"Failed to apply schema v{v}")

    def add_user(self, name: str, passw: bytes, lvl: AccessLevel):
        try:
            self._db.execute("INSERT INTO users VALUES (?,?,?,?)", (name, passw, lvl.value, time.time()))
            self._db.commit()
        except sqlite3.DatabaseError as e:
            self._logger.error(f"Failed to add user - {str(e)}")
            raise utils.DbError(__qualname__, "Failed to add a new user", str(e))

    def get_user(self, name: str) -> User | None:
        try:
            cur = self._db.execute("SELECT * FROM users WHERE name=?", name)
            row = cur.fetchone()
            if row is None:
                return None
            else:
                return User._make(row)
        except sqlite3.DatabaseError as e:
            self._logger.error(f"Failed to get user - {str(e)}")
            raise utils.DbError(__qualname__, "Failed to get user", str(e))

    def update_user(self, user: User):
        try:
            self._db.execute("UPDATE users SET last_logged_in=? WHERE name=?", (user.last_logged_in, user.name))
            self._db.commit()
        except sqlite3.DatabaseError as e:
            self._logger.error(f"Failed to update user - {str(e)}")
            raise utils.DbError(__qualname__, "Failed to update user", str(e))
