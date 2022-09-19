from core.appmodule import AppModule
from core.logger import Logger
from core.event_bus import EventBus, Event
from cloud.cloud_client import CloudClient
from db.database import Database
from core.events import EventType, PlanogramStatusReason
from db import model
from core import utils
import json
from pathlib import Path
from enum import Enum, auto, unique
from threading import Thread, Condition
from collections import deque
from copy import deepcopy


@unique
class PlanogramEventType(Enum):
    DUMMY = auto()
    PRODUCT_UPDATED = auto()
    PRODUCT_DELETED = auto()
    COLLECTION_UPDATED = auto()
    BRAND_UPDATED = auto()
    PLANOGRAM_UPDATED = auto()
    APPLY_PLANOGRAM = auto()
    REJECT_PLANOGRAM = auto()
    GET_PLANOGRAM = auto()


# Events structure:
# PRODUCT_UPDATED:
#   'product_id': int
#
# PRODUCT_DELETED:
#   'product_id': int
#
# COLLECTION_UPDATED:
#   'collection_id': int
#
# BRAND_UPDATED:
#   'lastUpdate': int
#
# PLANOGRAM_UPDATED:
#   no fields
#
# APPLY_PLANOGRAM:
#   no fields
#
# REJECT_PLANOGRAM:
#   no fields
#
# GET_PLANOGRAM:
#   no fields
#

class PlanogramEvent:
    def __init__(self, ev_type: PlanogramEventType, ev_body: dict):
        self.type = ev_type
        self.body = ev_body


class PlanogramLogic(AppModule):
    """Implements logic that handles corresponding notifications from the Cloud about updates in entire planogram
       or separate items like collections and products, downloads from the Cloud updated data and saves it
       to the database or filesystem in case of media objects.
     """
    MYNAME = 'logic.plangrm'
    REQ_CFG_OPTIONS = ['local_image_url_prefix', 'brand_info_filename', 'ui_model_filename']

    def __init__(self, config_data: dict, logger: Logger, ev_bus: EventBus, cloud_client: CloudClient, db: Database,
                 data_dir: Path, img_dir: Path):
        super().__init__(PlanogramLogic.MYNAME, config_data, logger)
        self._ev_bus = ev_bus
        self._cloud_client = cloud_client
        self._db = db
        self._data_dir = data_dir
        self._img_dir = img_dir
        self._event_q = deque()
        self._cv = Condition()
        self._event_thread: Thread = Thread(target=self._event_processing_worker)
        self._stopped = False
        self._brand_info = dict()
        self._current_planogram: list[dict | None] = list()
        self._new_planogram: list[dict | None] = list()
        self._new_collections: list[model.Collection] = list()
        self._new_products: list[model.Product] = list()
        self._new_variants: list[model.Variant] = list()
        self._ui_model = dict()

    def _get_my_required_cfg_options(self) -> list:
        return PlanogramLogic.REQ_CFG_OPTIONS

    def start(self):
        iot_client = self._cloud_client.get_iot_client()
        iot_client.register_handler('product', self._on_product_update)
        iot_client.register_handler('collection', self._on_collection_update)
        iot_client.register_handler('brand', self._on_brand_update)
        iot_client.register_handler('planogram', self._on_planogram_update)
        self._brand_info['lastUpdate'] = 0
        self._brand_info['logoId'] = 0
        for unit_id in range(1, model.MAX_UNITS + 1):
            self._new_planogram.append(None)
            inv_items = self._db.get_inventory_items_by_unit(unit_id)
            if len(inv_items) == 0:
                self._current_planogram.append(None)
            else:
                trays = dict()
                for item in inv_items:
                    if item.tray_number not in trays:
                        trays[item.tray_number] = dict()
                    if item.location not in trays[item.tray_number]:
                        trays[item.tray_number][item.location] = {'width': item.width,
                                                                  'depth': item.depth,
                                                                  'variant_id': item.variant_id}
                self._current_planogram.append(trays)
        self._ev_bus.subscribe(EventType.NEW_PLANOGRAM_APPLY, self._event_handler)
        self._ev_bus.subscribe(EventType.NEW_PLANOGRAM_REJECT, self._event_handler)
        self._ev_bus.subscribe(EventType.GET_PLANOGRAM, self._event_handler)
        self._event_thread.start()
        self._logger.info("Planogram Logic module started")

    def stop(self):
        self._stopped = True
        with self._cv:
            self._event_q.appendleft(PlanogramEvent(PlanogramEventType.DUMMY, {}))
            self._cv.notify()
        self._event_thread.join()
        self._logger.info("Planogram Logic module stopped")

    def _on_product_update(self, msg: str):
        self._logger.debug(f"Received: ({msg})")
        try:
            data = json.loads(msg)
            upd_type = data['update_type']
            if upd_type != 'update' and upd_type != 'delete':
                return
            product_id = data['product_id']
            if upd_type == 'update':
                with self._cv:
                    self._event_q.appendleft(PlanogramEvent(PlanogramEventType.PRODUCT_UPDATED,
                                                            {'product_id': product_id}))
                    self._cv.notify()
            else:
                with self._cv:
                    self._event_q.appendleft(PlanogramEvent(PlanogramEventType.PRODUCT_DELETED,
                                                            {'product_id': product_id}))
                    self._cv.notify()
        except json.JSONDecodeError as e:
            self._logger.error(f"Failed to process product update notification - {str(e)}")
        except KeyError:
            self._logger.warning(f"Received product update notification is malformed")

    def _on_collection_update(self, msg: str):
        self._logger.debug(f"Received: ({msg})")
        try:
            data = json.loads(msg)
            upd_type = data['update_type']
            if upd_type != 'update':
                return
            collection_id = data['collection_id']
            with self._cv:
                self._event_q.appendleft(PlanogramEvent(PlanogramEventType.COLLECTION_UPDATED,
                                                        {'collection_id': collection_id}))
                self._cv.notify()
        except json.JSONDecodeError as e:
            self._logger.error(f"Failed to process collection update notification - {str(e)}")
        except KeyError:
            self._logger.warning(f"Received collection update notification is malformed")

    def _on_brand_update(self, msg: str):
        self._logger.debug(f"Received: ({msg})")
        try:
            data = json.loads(msg)
            last_update = data['lastUpdate']
            with self._cv:
                self._event_q.appendleft(PlanogramEvent(PlanogramEventType.BRAND_UPDATED,
                                                        {'lastUpdate': last_update}))
                self._cv.notify()
        except json.JSONDecodeError as e:
            self._logger.error(f"Failed to process brand update notification - {str(e)}")
        except KeyError:
            self._logger.warning(f"Received product brand notification is malformed")

    def _on_planogram_update(self, msg: str):
        self._logger.debug(f"Received: ({msg})")
        try:
            data = json.loads(msg)
            with self._cv:
                self._event_q.appendleft(PlanogramEvent(PlanogramEventType.PLANOGRAM_UPDATED, {}))
                self._cv.notify()
        except json.JSONDecodeError as e:
            self._logger.error(f"Failed to process planogram update notification - {str(e)}")

    def _event_processing_worker(self):
        """Processes internal events in a separate thread"""
        while not self._stopped:
            with self._cv:
                self._cv.wait_for(lambda: len(self._event_q) > 0)
                ev = self._event_q.pop()
            if ev.type == PlanogramEventType.PRODUCT_DELETED:
                self._product_deleted_event_handler(ev.body)
            elif ev.type == PlanogramEventType.PRODUCT_UPDATED:
                self._product_updated_event_handler(ev.body)
            elif ev.type == PlanogramEventType.COLLECTION_UPDATED:
                self._collection_updated_event_handler(ev.body)
            elif ev.type == PlanogramEventType.BRAND_UPDATED:
                self._brand_updated_event_handler(ev.body)
            elif ev.type == PlanogramEventType.PLANOGRAM_UPDATED:
                self._planogram_updated_event_handler(ev.body)
            elif ev.type == PlanogramEventType.APPLY_PLANOGRAM:
                self._apply_new_data()
                self._apply_new_planogram()
            elif ev.type == PlanogramEventType.GET_PLANOGRAM:
                self._planogram_updated_event_handler({})

    def _event_handler(self, ev: Event):
        """Processes external events"""
        if ev.type == EventType.NEW_PLANOGRAM_APPLY:
            with self._cv:
                self._event_q.appendleft(PlanogramEvent(PlanogramEventType.APPLY_PLANOGRAM, {}))
                self._cv.notify()
        elif ev.type == EventType.NEW_PLANOGRAM_REJECT:
            # Not a heavy operation, can be done right away
            for unit_id in range(1, model.MAX_UNITS + 1):
                self._new_planogram[unit_id - 1] = None
            self._new_products.clear()
            self._new_collections.clear()
            self._new_variants.clear()
        elif ev.type == EventType.GET_PLANOGRAM:
            with self._cv:
                self._event_q.appendleft(PlanogramEvent(PlanogramEventType.GET_PLANOGRAM, {}))
                self._cv.notify()

    def _product_updated_event_handler(self, params: dict):
        try:
            prod = self._db.get_product(params['product_id'])
            if prod is None:
                return
            req_params = {'productId': prod.obj_id, 'deviceId': ''}
            upd_prod_data = self._cloud_client.invoke_api_get('product', req_params)
            self._update_product(prod, upd_prod_data)
        except utils.CloudApiNotFound:
            self._logger.error("GET API for product is not found in the Cloud client")
        except utils.CloudApiFormatError as e:
            self._logger.error(f"GET API for product returned malformed response - {e.msg}")
        except utils.CloudApiServerError as e:
            self._logger.error(f"Failed to get product data from the Cloud, server returned: code {e.status_code}, "
                               f"message ({e.response})")
        except utils.CloudApiConnectionError as e:
            self._logger.error(f"Failed to connect to the Cloud to get product data - {e.msg}")
        except utils.CloudApiTimeoutError:
            self._logger.error("Failed to get product data from the Cloud due to timeout")
        except KeyError as e:
            self._logger.error(f"Received product data for ID {params['product_id']} is malformed - {str(e)}")
        except utils.DbError as e:
            # TODO: telemetry
            pass

    def _product_deleted_event_handler(self, params: dict):
        try:
            prod = self._db.get_product(params['product_id'])
            if prod is None:
                return
            for var_id in prod.variants:
                var = self._db.get_variant(var_id)
                if var is not None:
                    var.deleted = True
                    self._db.update_variant(var)
                    self._logger.info(f"Variant {var.obj_id} was set to deleted")
        except utils.DbError as e:
            # TODO: telemetry
            pass

    def _collection_updated_event_handler(self, params: dict):
        try:
            coll = self._db.get_collection(params['collection_id'])
            if coll is None:
                return
            req_params = {'collectionId': coll.obj_id, 'deviceId': ''}
            upd_coll_data = self._cloud_client.invoke_api_get('collection', req_params)
            self._update_collection(coll, upd_coll_data)
        except utils.CloudApiNotFound:
            self._logger.error("GET API for collection is not found in the Cloud client")
        except utils.CloudApiFormatError as e:
            self._logger.error(f"GET API for collection returned malformed response - {e.msg}")
        except utils.CloudApiServerError as e:
            self._logger.error(f"Failed to get collection data from the Cloud, server returned: code {e.status_code}, "
                               f"message ({e.response})")
        except utils.CloudApiConnectionError as e:
            self._logger.error(f"Failed to connect to the Cloud to get collection data - {e.msg}")
        except utils.CloudApiTimeoutError:
            self._logger.error("Failed to get collection data from the Cloud due to timeout")
        except KeyError as e:
            self._logger.error(f"Received collection data for ID {params['collection_id']} is malformed - {str(e)}")
        except utils.DbError as e:
            # TODO: telemetry
            pass

    def _brand_updated_event_handler(self, params: dict):
        try:
            if params['lastUpdate'] != 0 and params['lastUpdate'] <= self._brand_info['lastUpdate']:
                self._logger.debug("Requested to update brand-info but it seems we already have the latest")
                return
            upd_brand_info = self._cloud_client.invoke_api_get('brand', {})
            if upd_brand_info['lastUpdate'] > self._brand_info['lastUpdate']:
                if upd_brand_info['logoId'] != self._brand_info['logoId']:
                    try:
                        image_name = self._cloud_client.download_image(upd_brand_info['logoUrl'], self._img_dir)
                        self._brand_info = deepcopy(upd_brand_info)
                        self._brand_info['logoUrl'] = self._config['local_image_url_prefix'] + image_name
                    except utils.CloudApiImageDownloadError as e:
                        self._logger.error(f"Failed to download brand logo from the Cloud - {e.msg}")
                    except utils.CloudApiServerError as e:
                        self._logger.error(f"Failed to download brand logo from the Cloud, server returned: "
                                           f"code {e.status_code}, message ({e.response})")
                    except utils.CloudApiConnectionError as e:
                        self._logger.error(f"Failed to connect to the Cloud to download brand logo - {e.msg}")
                    except utils.CloudApiTimeoutError:
                        self._logger.error("Failed to download brand logo from the Cloud due to timeout")
                else:
                    # Logo has not changed, so preserve the current local URL
                    curr_logo_url = self._brand_info['logoUrl']
                    self._brand_info = deepcopy(upd_brand_info)
                    self._brand_info['logoUrl'] = curr_logo_url
                with open(self._data_dir.joinpath(self._config['brand_info_filename']), 'w') as f:
                    json.dump(self._brand_info, f, indent=4)
                self._logger.debug("Brand info is saved to file")
                self._ev_bus.post(Event(EventType.BRAND_INFO_UPDATED, {}))
            else:
                self._logger.info("Retrieved brand-info but it seems we already have the latest")
        except utils.CloudApiNotFound:
            self._logger.error("GET API for brand is not found in the Cloud client")
        except utils.CloudApiFormatError as e:
            self._logger.error(f"GET API for brand returned malformed response - {e.msg}")
        except utils.CloudApiServerError as e:
            self._logger.error(f"Failed to get brand-info from the Cloud, server returned: code {e.status_code}, "
                               f"message ({e.response})")
        except utils.CloudApiConnectionError as e:
            self._logger.error(f"Failed to connect to the Cloud to get brand-info - {e.msg}")
        except utils.CloudApiTimeoutError:
            self._logger.error("Failed to get brand-info from the Cloud due to timeout")
        except KeyError as e:
            self._logger.error(f"Received brand-info is malformed - {str(e)}")

    def _planogram_updated_event_handler(self, params: dict):
        """Invoked in two cases: when a notification arrives from the Cloud that the planogram was changed
           and when technical personnel uses maintenance UI and wants to get the latest planogram from the Cloud
        """
        is_ok = False
        try:
            req_params = {'deviceId': ''}
            planogram_data = self._cloud_client.invoke_api_get('planogram', req_params)
            for stock in planogram_data['planogram']['stocks']:
                unit_id = stock['number']
                if unit_id < 1 or unit_id > model.MAX_UNITS:
                    self._logger.error(f"Received planogram contains data for unit with incorrect number {unit_id}")
                    # TODO: telemetry
                    return
                trays = dict()
                for tray in stock['trays']:
                    if tray['number'] not in trays:
                        trays[tray['number']] = dict()
                    for slot in tray['slots']:
                        trays[tray['number']][slot['number']] = {'width': slot['width'],
                                                                 'depth': slot['depth'],
                                                                 'variant_id': slot['variantId']}
                self._new_planogram[unit_id - 1] = trays
            is_equal = True
            for unit_id in range(1, model.MAX_UNITS + 1):
                if self._new_planogram[unit_id - 1] is None:
                    self._logger.error(f"New planogram does not have data for unit {unit_id}")
                    return
                is_equal = self._compare_planogram_trays(self._current_planogram[unit_id - 1],
                                                         self._new_planogram[unit_id - 1])
                if not is_equal:
                    break
            self._new_collections.clear()
            self._new_products.clear()
            self._new_variants.clear()
            for coll_data in planogram_data['collections']:
                coll = model.Collection(coll_data['id'], float(coll_data['last_update']), None)
                coll.set_media(model.Media(coll_data['image']['url'], coll_data['image']['last_update']))
                for loc in coll_data['localization']:
                    coll.add_info(loc['language'], model.ObjectInfo(loc['name'], loc['description']))
                for prod_id in coll_data['products']:
                    coll.add_product(prod_id)
                self._new_collections.append(coll)
            for prod_data in planogram_data['products']:
                prod = model.Product(prod_data['id'], prod_data['last_update'], prod_data['product_type'],
                                     ",".join(prod_data['tags']))
                for loc in prod_data['localization']:
                    lang = loc['language']
                    prod.add_info(lang, model.ObjectInfo(loc['name'], loc['description']))
                    for prop in loc['properties']:
                        prod.add_prop(lang, model.ObjectProperty(prop['type'], prop['name'], prop['value']))
                for var_data in prod_data['variants']:
                    prod.add_variant(var_data['id'])
                    var = model.Variant(var_data['id'], prod.obj_id, var_data['price'], var_data['price_cmp'],
                                        var_data['price_fmt'], var_data['price_cmp_fmt'],
                                        0 if var_data['deleted'] else 1, None)
                    var.set_media(model.Media(var_data['image']['url'], var_data['last_update']))
                    for loc in var_data['localization']:
                        lang = loc['language']
                        var.add_info(lang, model.ObjectInfo(loc['name'], ''))
                        for prop in loc['properties']:
                            var.add_prop(lang, model.ObjectProperty(prop['type'], prop['name'], prop['value']))
                    for opt in var_data['options']:
                        var.add_option(model.VariantOption(var.obj_id, opt['type'], opt['value']))
                    self._new_variants.append(var)
                self._new_products.append(prod)
            self._ui_model = deepcopy(planogram_data['uiModel'])
            self._process_ui_model()
            if is_equal:
                self._apply_new_data()
                self._ev_bus.post(Event(EventType.PLANOGRAM_IS_UP_TO_DATE, {}))
            else:
                status, reason = self._validate_new_planogram_against_reservations()
                self._ev_bus.post(Event(EventType.NEW_PLANOGRAM_AVAILABLE, {'status': status, 'reason': reason}))
            is_ok = True
        except utils.CloudApiNotFound:
            self._logger.error("GET API for planogram is not found in the Cloud client")
        except utils.CloudApiFormatError as e:
            self._logger.error(f"GET API for planogram returned malformed response - {e.msg}")
        except utils.CloudApiServerError as e:
            self._logger.error(f"Failed to get planogram data from the Cloud, server returned: code {e.status_code}, "
                               f"message ({e.response})")
        except utils.CloudApiConnectionError as e:
            self._logger.error(f"Failed to connect to the Cloud to get planogram data - {e.msg}")
        except utils.CloudApiTimeoutError:
            self._logger.error("Failed to get planogram data from the Cloud due to timeout")
        except KeyError as e:
            self._logger.error(f"Received planogram data is malformed - {str(e)}")
        finally:
            if not is_ok:
                self._ev_bus.post(Event(EventType.PLANOGRAM_UPDATE_FAILED, {}))

    def _update_product(self, prod: model.Product, upd_prod_data: dict):
        last_update = float(upd_prod_data['last_update'])
        if last_update != prod.last_update:
            prod.prod_type = upd_prod_data['product_type']
            prod.tags = ','.join(upd_prod_data['tags'])
            prod.last_update = last_update
            prod.clear_info()
            prod.clear_props()
            for loc in upd_prod_data['localization']:
                lang = loc['language']
                prod.add_info(lang, model.ObjectInfo(loc['name'], loc['description']))
                for prop in loc['properties']:
                    prod.add_prop(lang, model.ObjectProperty(prop['type'], prop['name'], prop['value']))
        self._db.update_product(prod)
        self._logger.info(f"Product {prod.obj_id} was updated")

        for var_data in upd_prod_data['variants']:
            var_id = var_data['id']
            if var_id in prod.variants:
                var = self._db.get_variant(var_id)
                var.price = var_data['price']
                var.price_comp = var_data['price_cmp']
                var.price_fmt = var_data['price_fmt']
                var.price_comp_fmt = var_data['price_cmp_fmt']
                var.deleted = var_data['deleted']
                var.clear_info()
                var.clear_props()
                for loc in var_data['localization']:
                    lang = loc['language']
                    var.add_info(lang, model.ObjectInfo(loc['name'], ''))
                    for prop in loc['properties']:
                        var.add_prop(lang, model.ObjectProperty(prop['type'], prop['name'], prop['value']))
                var.clear_options()
                for opt in var_data['options']:
                    var.add_option(model.VariantOption(var_id, opt['type'], opt['value']))
                var_image_data = var_data['image']
                image_name = utils.get_name_from_url(var_image_data['url'])
                if var_image_data['last_update'] != var.media.last_update or image_name != var.media.filename:
                    try:
                        image_name = self._cloud_client.download_image(var_image_data['url'], self._img_dir)
                        media = model.Media(image_name, var_image_data['last_update'])
                        var.media_id = self._db.add_media(media)
                        var.set_media(media)
                    except utils.CloudApiImageDownloadError as e:
                        self._logger.error(f"Failed to download image from the Cloud - {e.msg}")
                    except utils.CloudApiServerError as e:
                        self._logger.error(f"Failed to download image from the Cloud, server returned: "
                                           f"code {e.status_code}, message ({e.response})")
                    except utils.CloudApiConnectionError as e:
                        self._logger.error(f"Failed to connect to the Cloud to download image - {e.msg}")
                    except utils.CloudApiTimeoutError:
                        self._logger.error("Failed to download image from the Cloud due to timeout")
                self._db.update_variant(var)
                self._logger.info(f"Variant {var.obj_id} was updated")

    def _update_collection(self, coll: model.Collection, upd_coll_data: dict):
        last_update = float(upd_coll_data['last_update'])
        if coll.last_update != last_update:
            coll.last_update = last_update
            coll.clear_info()
            for loc in upd_coll_data['localization']:
                lang = loc['language']
                coll.add_info(lang, model.ObjectInfo(loc['name'], loc['description']))
            coll.clear_products()
            for prod_id in upd_coll_data['products']:
                coll.add_product(prod_id)
            coll_image_data = upd_coll_data['image']
            image_name = utils.get_name_from_url(coll_image_data['url'])
            if coll_image_data['last_update'] != coll.media.last_update or image_name != coll.media.filename:
                try:
                    image_name = self._cloud_client.download_image(coll_image_data['url'], self._img_dir)
                    media = model.Media(image_name, coll_image_data['last_update'])
                    coll.media_id = self._db.add_media(media)
                    coll.set_media(media)
                except utils.CloudApiImageDownloadError as e:
                    self._logger.error(f"Failed to download image from the Cloud - {e.msg}")
                except utils.CloudApiServerError as e:
                    self._logger.error(f"Failed to download image from the Cloud, server returned: "
                                       f"code {e.status_code}, message ({e.response})")
                except utils.CloudApiConnectionError as e:
                    self._logger.error(f"Failed to connect to the Cloud to download image - {e.msg}")
                except utils.CloudApiTimeoutError:
                    self._logger.error("Failed to download image from the Cloud due to timeout")
            self._db.update_collection(coll)
            self._logger.info(f"Collection {coll.obj_id} was updated")

    @staticmethod
    def _compare_planogram_trays(self, current_trays: dict, new_trays: dict) -> bool:
        if len(current_trays) != len(new_trays):
            return False
        if set(current_trays.keys()) != set(new_trays.keys()):
            return False
        for k in current_trays.keys():
            curr_tray = current_trays[k]
            new_tray = new_trays[k]
            if len(curr_tray) != len(new_tray):
                return False
            if set(curr_tray.keys()) != set(new_tray.keys()):
                return False
            for loc in curr_tray.keys():
                curr_slot = curr_tray[loc]
                new_slot = new_tray[loc]
                if (curr_slot['width'] != new_slot['width'] or curr_slot['depth'] != new_slot['depth'] or
                        curr_slot['variant_id'] != new_slot['variant_id']):
                    return False
        return True

    def _apply_new_data(self):
        """Check received in the latest planogram objects are newer than the existent ones; update those that newer;
           download new images if needed; remove objects, which are absent in the new planogram; save UiModel
        """
        try:
            for new_prod in self._new_products:
                prod = self._db.get_product(new_prod.obj_id)
                if prod is None:
                    self._db.add_product(new_prod)
                else:
                    if new_prod.last_update != prod.last_update:
                        self._db.update_product(new_prod)
            for new_coll in self._new_collections:
                coll = self._db.get_collection(new_coll.obj_id)
                if coll is None:
                    try:
                        image_name = self._cloud_client.download_image(new_coll.media.filename, self._img_dir)
                        media = model.Media(image_name, new_coll.media.last_update)
                        new_coll.media_id = self._db.add_media(media)
                        new_coll.set_media(media)
                    except utils.CloudApiImageDownloadError as e:
                        self._logger.error(f"Failed to download image from the Cloud - {e.msg}")
                    except utils.CloudApiServerError as e:
                        self._logger.error(f"Failed to download image from the Cloud, server returned: "
                                           f"code {e.status_code}, message ({e.response})")
                    except utils.CloudApiConnectionError as e:
                        self._logger.error(f"Failed to connect to the Cloud to download image - {e.msg}")
                    except utils.CloudApiTimeoutError:
                        self._logger.error("Failed to download image from the Cloud due to timeout")
                    self._db.add_collection(new_coll)
                else:
                    if new_coll.last_update != coll.last_update:
                        if new_coll.media.last_update != coll.media.last_update:
                            try:
                                image_name = self._cloud_client.download_image(new_coll.media.filename, self._img_dir)
                                media = model.Media(image_name, new_coll.media.last_update)
                                new_coll.media_id = self._db.add_media(media)
                                new_coll.set_media(media)
                            except utils.CloudApiImageDownloadError as e:
                                self._logger.error(f"Failed to download image from the Cloud - {e.msg}")
                            except utils.CloudApiServerError as e:
                                self._logger.error(f"Failed to download image from the Cloud, server returned: "
                                                   f"code {e.status_code}, message ({e.response})")
                            except utils.CloudApiConnectionError as e:
                                self._logger.error(f"Failed to connect to the Cloud to download image - {e.msg}")
                            except utils.CloudApiTimeoutError:
                                self._logger.error("Failed to download image from the Cloud due to timeout")
                        else:
                            new_coll.media_id = coll.media_id
                            new_coll.set_media(coll.media)
                        self._db.update_collection(new_coll)
            for new_var in self._new_variants:
                var = self._db.get_variant(new_var.obj_id)
                if var is None:
                    try:
                        image_name = self._cloud_client.download_image(new_var.media.filename, self._img_dir)
                        media = model.Media(image_name, new_var.media.last_update)
                        new_var.media_id = self._db.add_media(media)
                        new_var.set_media(media)
                    except utils.CloudApiImageDownloadError as e:
                        self._logger.error(f"Failed to download image from the Cloud - {e.msg}")
                    except utils.CloudApiServerError as e:
                        self._logger.error(f"Failed to download image from the Cloud, server returned: "
                                           f"code {e.status_code}, message ({e.response})")
                    except utils.CloudApiConnectionError as e:
                        self._logger.error(f"Failed to connect to the Cloud to download image - {e.msg}")
                    except utils.CloudApiTimeoutError:
                        self._logger.error("Failed to download image from the Cloud due to timeout")
                    self._db.add_variant(new_var)
                else:
                    if new_var.media.last_update != var.media.last_update:
                        try:
                            image_name = self._cloud_client.download_image(new_var.media.filename, self._img_dir)
                            media = model.Media(image_name, new_var.media.last_update)
                            new_var.media_id = self._db.add_media(media)
                            new_var.set_media(media)
                        except utils.CloudApiImageDownloadError as e:
                            self._logger.error(f"Failed to download image from the Cloud - {e.msg}")
                        except utils.CloudApiServerError as e:
                            self._logger.error(f"Failed to download image from the Cloud, server returned: "
                                               f"code {e.status_code}, message ({e.response})")
                        except utils.CloudApiConnectionError as e:
                            self._logger.error(f"Failed to connect to the Cloud to download image - {e.msg}")
                        except utils.CloudApiTimeoutError:
                            self._logger.error("Failed to download image from the Cloud due to timeout")
                    else:
                        new_var.media_id = var.media_id
                        new_var.set_media(var.media)
                    self._db.update_variant(new_var)

            all_var_ids = self._db.get_variant_ids()
            new_var_ids = [v.obj_id for v in self._new_variants]
            diff_var_ids = set(all_var_ids).difference(new_var_ids)
            for var_id in diff_var_ids:
                self._db.remove_variant(var_id)
            all_prod_ids = self._db.get_product_ids()
            new_prod_ids = [p.obj_id for p in self._new_products]
            diff_prod_ids = set(all_prod_ids).difference(new_prod_ids)
            for prod_id in diff_prod_ids:
                self._db.remove_product(prod_id)
            all_coll_ids = self._db.get_collection_ids()
            new_coll_ids = [c.obj_id for c in self._new_collections]
            diff_coll_ids = set(all_coll_ids).difference(new_coll_ids)
            for coll_id in diff_coll_ids:
                self._db.remove_collection(coll_id)

            if 'updated' in self._ui_model:
                del self._ui_model['updated']
                ui_model_file = self._data_dir.joinpath(self._config['ui_model_filename'])
                with open(ui_model_file, 'w') as f:
                    json.dump(self._ui_model, f, indent=4)
                self._ev_bus.post(Event(EventType.UI_MODEL_UPDATED, {}))
        except utils.DbError as e:
            # TODO: telemetry
            pass

    def _process_ui_model(self):
        ui_model_file = self._data_dir.joinpath(self._config['ui_model_filename'])
        if ui_model_file.exists():
            with open(ui_model_file) as f:
                curr_ui_model = json.load(f)
                curr_exists = True
                changed = self._ui_model['last_updated'] != curr_ui_model['last_updated']
        else:
            curr_exists = False
            changed = True
        if not changed:
            return
        self._logger.debug("Ui Model has updated, processing")
        # Don't process brand in ui_model, it is obsolete
        for prof_id, profile in self._ui_model['profiles'].items():
            for section in profile['sections']:
                if section['type'] == 'left-banner':
                    new_image_id = section['description']['imageId']
                    curr_image_id = 0
                    if curr_exists:
                        if prof_id in curr_ui_model['profiles']:
                            for curr_section in curr_ui_model['profiles'][prof_id]['sections']:
                                if curr_section['type'] == 'left-banner':
                                    curr_image_id = curr_section['description']['imageId']
                                    break
                    if new_image_id != curr_image_id:
                        self._logger.debug(f"Profile {prof_id}, section left-banner has new image id, downloading")
                        try:
                            image_name = self._cloud_client.download_image(section['description']['imageUrl'],
                                                                           self._img_dir)
                            section['description']['imageUrl'] = self._config['local_image_url_prefix'] + image_name
                        except utils.CloudApiImageDownloadError as e:
                            self._logger.error(f"Failed to download image from the Cloud - {e.msg}")
                        except utils.CloudApiServerError as e:
                            self._logger.error(f"Failed to download image from the Cloud, server returned: "
                                               f"code {e.status_code}, message ({e.response})")
                        except utils.CloudApiConnectionError as e:
                            self._logger.error(f"Failed to connect to the Cloud to download image - {e.msg}")
                        except utils.CloudApiTimeoutError:
                            self._logger.error("Failed to download image from the Cloud due to timeout")
                elif section['type'] == 'right-banner':
                    new_image_id = section['description']['imageId']
                    curr_image_id = 0
                    if curr_exists:
                        if prof_id in curr_ui_model['profiles']:
                            for curr_section in curr_ui_model['profiles'][prof_id]['sections']:
                                if curr_section['type'] == 'right-banner':
                                    curr_image_id = curr_section['description']['imageId']
                                    break
                    if new_image_id != curr_image_id:
                        self._logger.debug(f"Profile {prof_id}, section right-banner has new image id, downloading")
                        try:
                            image_name = self._cloud_client.download_image(section['description']['imageUrl'],
                                                                           self._img_dir)
                            section['description']['imageUrl'] = self._config['local_image_url_prefix'] + image_name
                        except utils.CloudApiImageDownloadError as e:
                            self._logger.error(f"Failed to download image from the Cloud - {e.msg}")
                        except utils.CloudApiServerError as e:
                            self._logger.error(f"Failed to download image from the Cloud, server returned: "
                                               f"code {e.status_code}, message ({e.response})")
                        except utils.CloudApiConnectionError as e:
                            self._logger.error(f"Failed to connect to the Cloud to download image - {e.msg}")
                        except utils.CloudApiTimeoutError:
                            self._logger.error("Failed to download image from the Cloud due to timeout")
        # Add a flag, that the model was updated and needs to be saved, should be removed before saving
        self._ui_model['updated'] = True

    def _apply_new_planogram(self):
        try:
            for unit_id in range(1, model.MAX_UNITS + 1):
                new_trays = self._new_planogram[unit_id - 1]
                curr_trays = self._current_planogram[unit_id - 1]
                for tray_num, tray in new_trays.items():
                    if tray_num not in curr_trays:
                        for loc, slot in tray.items():
                            self._db.add_inventory_item(model.InventoryItem(unit_id, tray_num, loc, slot['variant_id'],
                                                                            slot['width'], 0, slot['depth']))
                    else:
                        curr_tray = curr_trays[tray_num]
                        for loc, slot in tray.items():
                            if loc not in curr_tray:
                                self._db.add_inventory_item(model.InventoryItem(unit_id, tray_num, loc,
                                                                                slot['variant_id'], slot['width'],
                                                                                0, slot['depth']))
                            else:
                                curr_slot = curr_tray[loc]
                                if (slot['variant_id'] != curr_slot['variant_id'] or slot['width'] != curr_slot['width']
                                        or slot['depth'] != curr_slot['depth']):
                                    self._db.update_inventory_item(model.InventoryItem(unit_id, tray_num, loc,
                                                                                       slot['variant_id'],
                                                                                       slot['width'], 0, slot['depth']))
                for tray_num, tray in curr_trays.items():
                    if tray_num not in new_trays:
                        for loc, slot in tray.items():
                            self._db.remove_inventory_item(model.InventoryItem(unit_id, tray_num, loc,
                                                                               slot['variant_id'], slot['width'],
                                                                               0, slot['depth']))
                    else:
                        new_tray = new_trays[tray_num]
                        for loc, slot in tray.items():
                            if loc not in new_tray:
                                self._db.remove_inventory_item(model.InventoryItem(unit_id, tray_num, loc,
                                                                                   slot['variant_id'], slot['width'],
                                                                                   0, slot['depth']))
            self._current_planogram = deepcopy(self._new_planogram)
            for unit_id in range(1, model.MAX_UNITS + 1):
                self._new_planogram[unit_id-1] = None
            self._ev_bus.post(Event(EventType.PLANOGRAM_UPDATE_DONE, {}))
        except KeyError as e:
            self._logger.error(f"Planogram data structure is malformed - {str(e)}")
        except utils.DbError as e:
            # TODO: telemetry
            pass

    def _validate_new_planogram_against_reservations(self) -> (bool, PlanogramStatusReason):
        """Checks if there is a conflict between new planogram and active remote reservations"""
        # Check if there are reserved variants absent in the new planogram and block planogram applying if it is so
        reserved_variants = list()
        new_variant_ids = [var.obj_id for var in self._new_variants]
        carts = self._db.get_carts()
        for cart in carts:
            if (cart.cart_type == model.CartType.REMOTE and (cart.status == model.CartStatus.PRERESERVATION or
                                                             cart.status == model.CartStatus.RESERVED)):
                cart_contents = self._db.get_cart_items(cart.obj_id)
                for item in cart_contents:
                    reserved_variants.append(item.variant_id)
                    if item.variant_id not in new_variant_ids:
                        self._logger.info(f"Reserved variant {item.variant_id} is not present in the new planogram")
                        return False, PlanogramStatusReason.RESERVED_PRODUCT_ABSENT
        # Now verify that number of slots for every reserved variant in the new planogram not less than now
        for var_id in reserved_variants:
            for unit_id in range(1, model.MAX_UNITS+1):
                current_slots_count = 0
                new_slots_count = 0
                curr_trays = self._current_planogram[unit_id-1]
                for _, tray in curr_trays.items():
                    for _, slot in tray.item():
                        if slot['variant_id'] == var_id:
                            current_slots_count += 1
                new_trays = self._new_planogram[unit_id-1]
                for _, tray in new_trays.items():
                    for _, slot in tray.items():
                        if slot['variant_id'] == var_id:
                            new_slots_count += 1
                if current_slots_count > new_slots_count:
                    self._logger.info(f"Reserved variant {var_id} in unit {unit_id} is in {current_slots_count} "
                                      f"slot(s) now but in {new_slots_count} slot(s) in the new planogram")
                    return False, PlanogramStatusReason.RESERVED_PRODUCT_OCCUPIES_LESS_SLOTS
        return True, PlanogramStatusReason.NO_REASON
