from pathlib import Path
from core.appmodule import AppModule
from core.logger import Logger
from core import utils
from core.utils import _myname_
import sqlite3
import time
from threading import Lock
from db import model


class Database(AppModule):
    """Implements infrastructure and logic layer between the application modules and SQLite3 database"""
    MYNAME = 'db'
    SCHEMA_V1 = 'schema.v1.txt'
    REQ_CFG_OPTIONS = ['name', 'schema_version']

    def __init__(self, config_data: dict, logger: Logger, data_dir: Path, schema_dir: Path, lang: str):
        super().__init__(Database.MYNAME, config_data, logger)
        self._data_dir = data_dir
        self._schema_dir = schema_dir
        self._lang = lang
        self._lock = Lock()
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
            for v in range(schema_version + 1, self._config['schema_version'] + 1):
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

    def add_user(self, name: str, passw: bytes, lvl: model.AccessLevel):
        with self._lock:
            try:
                self._db.execute("INSERT INTO users (name, password, access_level, last_logged_in) VALUES (?,?,?,?)",
                                 (name, passw, lvl.value, time.time()))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to add user - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to add a new user", str(e))

    def get_user(self, name: str) -> model.User | None:
        with self._lock:
            try:
                cur = self._db.execute("SELECT * FROM users WHERE name=?", (name,))
                row = cur.fetchone()
                if row is None:
                    return None
                else:
                    return model.User._make(row)
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get user - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get user", str(e))

    def update_user(self, user: model.User):
        with self._lock:
            try:
                self._db.execute("UPDATE users SET last_logged_in=? WHERE name=?", (user.last_logged_in, user.name))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to update user - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to update user", str(e))

    def add_media(self, m: model.Media) -> int:
        with self._lock:
            try:
                cur = self._db.execute("INSERT INTO media (filename, last_update) VALUES (?,?)",
                                       (m.filename, m.last_update))
                media_id = cur.lastrowid
                self._db.commit()
                return media_id
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to add media {m.filename} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to add media", str(e))

    def get_media(self, media_id: int) -> model.Media | None:
        with self._lock:
            try:
                cur = self._db.execute("SELECT filename, last_update FROM media WHERE media_id=?", (media_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                else:
                    m = model.Media(row[0], row[1])
                    return m
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get media - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get media", str(e))

    def remove_media(self, obj_id: int):
        with self._lock:
            try:
                self._db.execute("DELETE FROM media WHERE id=?", (obj_id,))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to remove media {obj_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to remove media", str(e))

    def add_collection(self, coll: model.Collection):
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("INSERT INTO collection (id, last_update, media_id) VALUES (?,?,?)",
                            (coll.obj_id, coll.last_update, coll.media_id))
                for lang, info in coll.info.items():
                    cur.execute("INSERT INTO collection_info (collection_id, language, name, description) "
                                "VALUES(?,?,?,?)", (coll.obj_id, lang, info.name, info.description))
                for prod_id in coll.products:
                    cur.execute("INSERT INTO product_collection (product_id, collection_id) VALUES(?,?)",
                                (prod_id, coll.obj_id))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to add collection - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to add a new collection", str(e))

    def get_collection(self, obj_id: int) -> model.Collection | None:
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("SELECT * FROM collection WHERE id=?", (obj_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                coll = model.Collection(obj_id, row['last_update'], row['media_id'])
                if coll.media_id is not None:
                    coll.set_media(self.get_media(coll.media_id))
                cur.execute("SELECT * FROM collection_info WHERE collection_id=? AND language=?", (obj_id, self._lang))
                row = cur.fetchone()
                if row is None:
                    self._logger.warning(f"Info for collection {obj_id} for language {self._lang} is absent in the DB")
                else:
                    info = model.ObjectInfo(row['name'], row['description'])
                    coll.add_info(self._lang, info)
                cur.execute("SELECT product_id FROM product_collection WHERE collection_id=?", (obj_id,))
                for row in cur.fetchall():
                    coll.add_product(row[0])
                return coll
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get collection or its properties - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get collection", str(e))

    def update_collection(self, coll: model.Collection):
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("UPDATE collection SET last_update=?, media_id=? WHERE id=?",
                            (coll.last_update, coll.media_id, coll.obj_id))
                cur.execute("DELETE FROM collection_info WHERE collection_id=?", (coll.obj_id,))
                for lang, info in coll.info.items():
                    cur.execute("INSERT INTO collection_info (collection_id, language, name, description) "
                                "VALUES(?,?,?,?)", (coll.obj_id, lang, info.name, info.description))
                cur.execute("DELETE FROM product_collection WHERE collection_id=?", (coll.obj_id,))
                for prod_id in coll.products:
                    cur.execute("INSERT INTO product_collection (product_id, collection_id) VALUES(?,?)",
                                (prod_id, coll.obj_id))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to update collection {coll.obj_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to update collection", str(e))

    def remove_collection(self, obj_id: int):
        with self._lock:
            try:
                self._db.execute("DELETE FROM collection WHERE id=?", (obj_id,))
                self._db.commit()
                # connected records in collection_info and product_collection should be removed automatically
                # connected media should be removed manually
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to remove collection {obj_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to remove collection", str(e))

    def get_collections(self) -> list[model.Collection]:
        with self._lock:
            collections = list()
            try:
                cur = self._db.cursor()
                cur.execute("SELECT id FROM collection")
                for row in cur.fetchall():
                    collections.append(self.get_collection(row[0]))
                return collections
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get all collections - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get all collections", str(e))

    def get_collection_ids(self) -> list[int]:
        with self._lock:
            ids = list()
            try:
                cur = self._db.cursor()
                cur.execute("SELECT id FROM collection")
                for row in cur.fetchall():
                    ids.append(row[0])
                return ids
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get all collection IDs - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get all collection IDs", str(e))

    def add_product(self, prod: model.Product):
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("INSERT INTO product (id, last_update, type, tags) VALUES(?,?,?,?)",
                            (prod.obj_id, prod.last_update, prod.prod_type, prod.tags))
                for lang, info in prod.info.items():
                    cur.execute("INSERT INTO product_info (product_id, language, name, description) VALUES(?,?,?,?)",
                                (prod.obj_id, lang, info.name, info.description))
                for lang, prop in prod.props.items():
                    cur.execute("INSERT INTO product_property (product_id, language, type, name, value) "
                                "VALUES(?,?,?,?,?)", (prod.obj_id, lang, prop.ptype, prop.name, prop.value))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to add product - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to add a new product", str(e))

    def get_product(self, obj_id: int) -> model.Product | None:
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("SELECT * FROM product WHERE id=?", (obj_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                prod = model.Product(obj_id, row['last_update'], row['type'], row['tags'])
                cur.execute("SELECT * FROM product_info WHERE product_id=? AND language=?", (obj_id, self._lang))
                row = cur.fetchone()
                if row is None:
                    self._logger.warning(f"Info for product {obj_id} for language {self._lang} is absent in the DB")
                else:
                    info = model.ObjectInfo(row['name'], row['description'])
                    prod.add_info(self._lang, info)
                cur.execute("SELECT * FROM product_property WHERE product_id=? AND language=?", (obj_id, self._lang))
                for row in cur.fetchall():
                    prop = model.ObjectProperty(row['type'], row['name'], row['value'])
                    prod.add_prop(self._lang, prop)
                cur.execute("SELECT id FROM variant WHERE product_id=?", (obj_id,))
                for row in cur.fetchall():
                    prod.add_variant(row[0])
                return prod
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get product or its properties - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get product", str(e))

    def update_product(self, prod: model.Product):
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("UPDATE product SET last_update=?, type=?, tags=? WHERE id=?",
                            (prod.last_update, prod.prod_type, prod.tags, prod.obj_id))
                cur.execute("DELETE FROM product_info WHERE product_id=?", (prod.obj_id,))
                for lang, info in prod.info.items():
                    cur.execute("INSERT INTO product_info (product_id, language, name, description) VALUES(?,?,?,?)",
                                (prod.obj_id, lang, info.name, info.description))
                cur.execute("DELETE FROM product_property WHERE product_id=?", (prod.obj_id,))
                for lang, prop in prod.props.items():
                    cur.execute("INSERT INTO product_property (product_id, language, type, name, value) VALUES(?,?)",
                                (prod.obj_id, lang, prop.ptype, prop.name, prop.value))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to update product {prod.obj_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to update product", str(e))

    def remove_product(self, obj_id: int):
        with self._lock:
            try:
                self._db.execute("DELETE FROM product WHERE id=?", (obj_id,))
                self._db.commit()
                # connected records in product_info and product_property should be removed automatically
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to remove product {obj_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to remove product", str(e))

    def get_products(self) -> list[model.Product]:
        with self._lock:
            products = list()
            try:
                cur = self._db.cursor()
                cur.execute("SELECT id FROM product")
                for row in cur.fetchall():
                    products.append(self.get_product(row[0]))
                return products
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get all products - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get all products", str(e))

    def get_product_ids(self) -> list[int]:
        with self._lock:
            ids = list()
            try:
                cur = self._db.cursor()
                cur.execute("SELECT id FROM product")
                for row in cur.fetchall():
                    ids.append(row[0])
                return ids
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get all product IDs - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get all product IDs", str(e))

    def add_variant(self, var: model.Variant):
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("INSERT INTO variant (id, product_id, price, price_compare, price_formatted, "
                            "price_compare_formatted, deleted, media_id) VALUES(?,?,?,?,?,?,?,? )",
                            (var.obj_id, var.prod_id, var.price, var.price_comp, var.price_fmt, var.price_comp_fmt,
                             1 if var.deleted else 0, var.media_id))
                for lang, info in var.info.items():
                    cur.execute("INSERT INTO variant_info (variant_id, language, name, description) VALUES(?,?,?,?)",
                                (var.obj_id, lang, info.name, info.description))
                for lang, prop in var.props.items():
                    cur.execute("INSERT INTO variant_property (product_id, language, type, name, value) "
                                "VALUES(?,?,?,?,?)", (var.obj_id, lang, prop.ptype, prop.name, prop.value))
                for opt in var.options:
                    cur.execute("INSERT INTO variant_option (variant_id, option, value) VALUES(?,?,?)",
                                (opt.variant_id, opt.option, opt.value))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to add variant - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to add a new variant", str(e))

    def get_variant(self, obj_id: int) -> model.Variant | None:
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("SELECT * FROM variant WHERE id=?", (obj_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                var = model.Variant(obj_id, row['product_id'], row['price'], row['price_compare'],
                                    row['price_formatted'], row['price_compare_formatted'], row['deleted'],
                                    row['media_id'])
                if var.media_id is not None:
                    var.set_media(self.get_media(var.media_id))
                cur.execute("SELECT * FROM variant_info WHERE variant_id=? AND language=?", (obj_id, self._lang))
                row = cur.fetchone()
                if row is None:
                    self._logger.warning(f"Info for variant {obj_id} for language {self._lang} is absent in the DB")
                else:
                    info = model.ObjectInfo(row['name'], row['description'])
                    var.add_info(self._lang, info)
                cur.execute("SELECT * FROM variant_property WHERE variant_id=? AND language=?", (obj_id, self._lang))
                for row in cur.fetchall():
                    prop = model.ObjectProperty(row['type'], row['name'], row['value'])
                    var.add_prop(self._lang, prop)
                cur.execute("SELECT * FROM variant_option WHERE variant_id=?", (obj_id,))
                for row in cur.fetchall():
                    var.add_option(model.VariantOption._make(row))
                return var
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get variant or its properties - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get variant", str(e))

    def update_variant(self, var: model.Variant):
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute(
                    "UPDATE variant SET price=?, price_compare=?, price_formatted=?, price_compare_formatted=?, "
                    "deleted=?, media_id=? WHERE id=?",
                    (var.price, var.price_comp, var.price_fmt, var.price_comp_fmt,
                     1 if var.deleted else 0, var.media_id, var.obj_id))
                cur.execute("DELETE FROM variant_info WHERE variant_id=?", (var.obj_id,))
                for lang, info in var.info.items():
                    cur.execute("INSERT INTO variant_info (variant_id, language, name, description) VALUES(?,?,?,?)",
                                (var.obj_id, lang, info.name, info.description))
                cur.execute("DELETE FROM variant_property WHERE variant_id=?", (var.obj_id,))
                for lang, prop in var.props.items():
                    cur.execute("INSERT INTO variant_property (variant_id, language, type, name, value) VALUES(?,?)",
                                (var.obj_id, lang, prop.ptype, prop.name, prop.value))
                cur.execute("DELETE FROM variant_option WHERE variant_id=?", (var.obj_id,))
                for opt in var.options:
                    cur.execute("INSERT INTO variant_option (variant_id, option, value) VALUES(?,?,?)",
                                opt.variant_id, opt.option, opt.value)
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to update variant {var.obj_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to update variant", str(e))

    def remove_variant(self, obj_id: int):
        with self._lock:
            try:
                self._db.execute("DELETE FROM variant WHERE id=?", (obj_id,))
                self._db.commit()
                # connected records in variant_info, variant_property and variant_option should be removed automatically
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to remove variant {obj_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to remove variant", str(e))

    def get_variants(self, prod_id: int = 0) -> list[model.Variant]:
        with self._lock:
            variants = list()
            try:
                cur = self._db.cursor()
                if prod_id == 0:
                    cur.execute("SELECT id FROM variant")
                else:
                    cur.execute("SELECT id FROM variant WHERE product_id=?", (prod_id,))
                for row in cur.fetchall():
                    variants.append(self.get_variant(row[0]))
                return variants
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get all variants (product_id={prod_id}) - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get all variants", str(e))

    def get_variant_ids(self) -> list[int]:
        with self._lock:
            ids = list()
            try:
                cur = self._db.cursor()
                cur.execute("SELECT id FROM variant")
                for row in cur.fetchall():
                    ids.append(row[0])
                return ids
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get all variant IDs - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get all variant IDs", str(e))

    def add_inventory_item(self, inv_item: model.InventoryItem):
        with self._lock:
            try:
                self._db.execute("INSERT INTO inventory (unit_id, tray_number, location, variant_id, width, quantity, "
                                 "depth) VALUES(?,?,?,?,?,?,?)",
                                 (inv_item.unit_id, inv_item.tray_number, inv_item.location, inv_item.variant_id,
                                  inv_item.width, inv_item.quantity, inv_item.depth))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to add inventory item - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to add a new inventory item", str(e))

    def get_inventory_item(self, unit_id: int, tray: int, location: int) -> model.InventoryItem | None:
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("SELECT * FROM inventory WHERE unit_id=?, tray_number=?, location=?",
                            (unit_id, tray, location))
                row = cur.fetchone()
                if row is None:
                    return None
                inv_item = model.InventoryItem._make(row)
                return inv_item
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get inventory item - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get inventory item", str(e))

    def update_inventory_item_quantity(self, inv_item: model.InventoryItem):
        with self._lock:
            try:
                self._db.execute("UPDATE inventory SET quantity=? WHERE unit_id=?, tray_number=?, location=?",
                                 (inv_item.quantity, inv_item.unit_id, inv_item.tray_number, inv_item.location))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error("Failed to update inventory item's quantity "
                                   f"{inv_item.unit_id}:{inv_item.tray_number}:{inv_item.location} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to update inventory item", str(e))

    def update_inventory_item(self, inv_item: model.InventoryItem):
        with self._lock:
            try:
                self._db.execute("UPDATE inventory SET variant_id=?, width=?, depth=?, quantity=? "
                                 "WHERE unit_id=?, tray_number=?, location=?",
                                 (inv_item.variant_id, inv_item.width, inv_item.depth, inv_item.quantity,
                                  inv_item.unit_id, inv_item.tray_number, inv_item.location))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error("Failed to update inventory item "
                                   f"{inv_item.unit_id}:{inv_item.tray_number}:{inv_item.location} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to update inventory item", str(e))

    def remove_inventory_items(self, variant_id: int):
        with self._lock:
            try:
                self._db.execute("DELETE FROM inventory WHERE variant_id=?", (variant_id,))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to remove inventory items for {variant_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to remove inventory items", str(e))

    def remove_inventory_item(self, inv_item: model.InventoryItem):
        with self._lock:
            try:
                self._db.execute("DELETE FROM inventory WHERE unit_id=? AND tray_number=? AND location=?",
                                 (inv_item.unit_id, inv_item.tray_number, inv_item.location))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to remove inventory item for "
                                   f"{inv_item.unit_id}:{inv_item.tray_number}:{inv_item.location} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to remove inventory item", str(e))

    def get_inventory_items_by_variant(self, variant_id: int) -> list[model.InventoryItem]:
        with self._lock:
            inv_items = list()
            try:
                cur = self._db.cursor()
                cur.execute("SELECT * FROM inventory WHERE variant_id=?", (variant_id,))
                for row in cur.fetchall():
                    inv_item = model.InventoryItem._make(row)
                    inv_items.append(inv_item)
                return inv_items
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get inventory items for variant {variant_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get inventory items", str(e))

    def get_inventory_items_by_unit(self, unit_id: int) -> list[model.InventoryItem]:
        with self._lock:
            inv_items = list()
            try:
                cur = self._db.cursor()
                cur.execute("SELECT * FROM inventory WHERE unit_id=?", (unit_id,))
                for row in cur.fetchall():
                    inv_item = model.InventoryItem._make(row)
                    inv_items.append(inv_item)
                return inv_items
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get inventory items for unit {unit_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get inventory items", str(e))

    def add_cart(self, cart: model.Cart) -> int:
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("INSERT INTO cart (display_id, transaction_id, type, order_info, status, checkout_method, "
                            "locked_at) VALUES(?,?,?,?,?,?)",
                            (cart.display_id, int(cart.cart_type), cart.order_info, int(cart.status),
                             int(cart.checkout_method), cart.locked_at))
                cart_id = cur.lastrowid
                self._db.commit()
                return cart_id
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to add cart - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to add cart", str(e))

    def get_cart(self, cart_id: int) -> model.Cart | None:
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("SELECT * FROM cart WHERE id=?", (cart_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                cart = model.Cart(row['id'], row['display_id'], row['transaction_id'], row['type'], row['order_info'],
                                  row['status'], row['checkout_method'], row['locked_at'])
                return cart
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get cart - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get cart", str(e))

    def get_cart_by_transaction(self, transaction_id: str) -> model.Cart | None:
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("SELECT * FROM cart WHERE transaction_id=?", (transaction_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                cart = model.Cart(row['id'], row['display_id'], row['transaction_id'], row['type'], row['order_info'],
                                  row['status'], row['checkout_method'], row['locked_at'])
                return cart
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get cart by transaction {transaction_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get cart by transaction", str(e))

    def update_cart(self, cart: model.Cart):
        with self._lock:
            try:
                self._db.execute("UPDATE cart SET display_id=?, transaction_id=?, type=?, order_info=?, status=?, "
                                 "checkout_method=?, locked_at=? WHERE id=?",
                                 (cart.display_id, cart.transaction_id, int(cart.cart_type), cart.order_info,
                                  int(cart.status), int(cart.checkout_method), cart.locked_at))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to update cart - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to update cart", str(e))

    def remove_cart(self, cart_id: int):
        with self._lock:
            try:
                self._db.execute("DELETE FROM cart WHERE id=?", (cart_id,))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to remove cart - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to remove cart", str(e))

    def get_carts(self, order_info: str = None) -> list[model.Cart]:
        with self._lock:
            carts = list()
            try:
                cur = self._db.cursor()
                if order_info is not None:
                    cur.execute("SELECT * FROM cart WHERE order_info=?", (order_info,))
                else:
                    cur.execute("SELECT * FROM cart")
                for row in cur.fetchall():
                    cart = model.Cart(row['id'], row['display_id'], row['transaction_id'], row['type'],
                                      row['order_info'], row['status'], row['checkout_method'], row['locked_at'])
                    carts.append(cart)
                return carts
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get carts - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get carts", str(e))

    def add_cart_item(self, ci: model.CartItem):
        with self._lock:
            try:
                self._db.execute("INSERT INTO cart_contents (card_id, variant_id, amount) VALUES (?,?,?)",
                                 (ci.cart_id, ci.variant_id, ci.amount))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to add cart item - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to add cart item", str(e))

    def get_cart_items(self, cart_id: int) -> list[model.CartItem]:
        with self._lock:
            cart_items = list()
            try:
                cur = self._db.cursor()
                cur.execute("SELECT * FROM cart_contents WHERE cart_id=?", (cart_id,))
                for row in cur.fetchall():
                    ci = model.CartItem._make(row)
                    cart_items.append(ci)
                return cart_items
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get cart contents for {cart_id} - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get cart contents", str(e))

    def update_cart_item(self, ci: model.CartItem):
        with self._lock:
            try:
                self._db.execute("UPDATE cart_contents SET amount=? WHERE cart_id=? AND variant_id=?",
                                 (ci.amount, ci.cart_id, ci.variant_id))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to update cart item - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to update cart item", str(e))

    def remove_cart_item(self, ci: model.CartItem):
        with self._lock:
            try:
                self._db.execute("DELETE FROM cart_contents WHERE cart_id=? AND variant_id=?",
                                 (ci.cart_id, ci.variant_id))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to remove cart item - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to remove cart item", str(e))

    def add_reservation(self, r: model.Reservation) -> int:
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("INSERT INTO reservation (cart_id, variant_id, unit_id, location, quantity",
                            (r.cart_id, r.variant_id, r.unit_id, r.location, r.quantity))
                r_id = cur.lastrowid
                self._db.commit()
                return r_id
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to add reservation - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to add reservation", str(e))

    def update_reservation(self, r: model.Reservation, new_location: int = 0):
        with self._lock:
            try:
                self._db.execute("UPDATE reservation SET location=?, quantity=? WHERE id=?",
                                 (r.location if new_location == 0 else new_location, r.quantity, r.id))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to update reservation - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to update reservation", str(e))

    def add_or_update_reservation(self, r: model.Reservation):
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("SELECT id, quantity FROM reservation WHERE cart_id=? AND unit_id=? AND location=? "
                            "AND variant_id=?",
                            (r.cart_id, r.unit_id, r.location, r.variant_id))
                row = cur.fetchone()
                if row is None:
                    self.add_reservation(r)
                else:
                    r.id = row[0]
                    r.quantity += row[1]
                    self.update_reservation(r)
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to add or update reservation - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to add or update reservation", str(e))

    def remove_reservation(self, r_id: int):
        with self._lock:
            try:
                self._db.execute("DELETE FROM reservation WHERE id=?", (r_id,))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to remove reservation - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to remove reservation", str(e))

    def get_reservations(self, variant_id: int, cart_id: int = 0) -> list[model.Reservation]:
        with self._lock:
            reservations = list()
            try:
                cur = self._db.cursor()
                if cart_id == 0:
                    cur.execute("SELECT * FROM reservation WHERE variant_id=?", (variant_id,))
                else:
                    cur.execute("SELECT * FROM reservation WHERE card_id=? AND variantId_id=?", (cart_id, variant_id))
                for row in cur.fetchall():
                    r = model.Reservation._make(row)
                    reservations.append(r)
                return reservations
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get reservations - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get reservations", str(e))

    def add_order_history_record(self, rec: model.OrderHistoryRecord) -> int:
        with self._lock:
            try:
                cur = self._db.cursor()
                cur.execute("INSERT INTO order_history (transaction_id, order_info, completion_cause, created_at",
                            (rec.transaction_id, rec.order_info, int(rec.completion_status), rec.created_at))
                rec_id = cur.lastrowid
                self._db.commit()
                return rec_id
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to add order history record - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to add order history record", str(e))

    def remove_order_history_record(self, rec_id: int):
        with self._lock:
            try:
                self._db.execute("DELETE FROM order_history WHERE id=?", (rec_id,))
                self._db.commit()
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to remove order history record - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to remove order history record", str(e))

    def get_order_history_records(self, order_info: str = None) -> list[model.OrderHistoryRecord]:
        with self._lock:
            records = list()
            try:
                cur = self._db.cursor()
                if order_info is not None:
                    cur.execute("SELECT * FROM order_history WHERE order_info=?", (order_info,))
                else:
                    cur.execute("SELECT * FROM order_history")
                for row in cur.fetchall():
                    rec = model.OrderHistoryRecord(row['id'], row['transaction_id'], row['order_info'],
                                                   row['completion_cause'], row['created_at'])
                    records.append(rec)
                return records
            except sqlite3.DatabaseError as e:
                self._logger.error(f"Failed to get order history records - {str(e)}")
                raise utils.DbError(_myname_(self), "Failed to get order history records", str(e))
