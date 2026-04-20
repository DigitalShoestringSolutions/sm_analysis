import logging
from pandas import DataFrame
import requests
import time


logger = logging.getLogger(__name__)


class TokenExpired(Exception):
    pass


class KBManager:
    inst = {}

    @classmethod
    def get_inst(cls, app_id, secret, base_url) -> "KBManager":
        if cls.inst.get(app_id) is None:
            cls.inst[app_id] = cls(app_id, secret, base_url)
        return cls.inst[app_id]

    def __init__(self, app_id, secret, base_url):
        self.__base_url = base_url
        self._id_map = {}

        self._token = None
        self.app_id = app_id
        self.secret = secret

        self._token_expiry = 0

    @property
    def token(self):
        if not self._token or time.time() > self._token_expiry - 60:
            self._refresh_token()
        return self._token

    def expire_token(self):
        self._token_expiry = 0

    def _refresh_token(self):
        """Obtain a new JWT token using app_id/secret.  We store expiry as
        roughly one hour from now since docs don't specify TTL.
        """
        if not self.app_id or not self.secret:
            return
        url = f"{self.__base_url}/token"
        try:
            resp = requests.post(
                url, json={"appId": self.app_id, "secret": self.secret}
            )
            resp.raise_for_status()
            data = resp.json()
            self._token = data.get("token")
            # naive expiry estimate
            self._token_expiry = time.time() + 3500
            logger.info("Obtained new Kinabase token")
        except Exception as e:
            logger.error(f"Unable to refresh Kinabase token: {e}")

    def id_map(self, collection: str, pk_field: str):
        if not collection in self._id_map:
            self.refresh_id_map(collection, pk_field)
        elif pk_field not in self._id_map[collection]:
            self.refresh_id_map(collection, pk_field)
        return self._id_map.get(collection, {}).get(pk_field, {})

    def refresh_id_map(self, collection_id, pk_field="machineName"):
        if collection_id is None:
            logger.error("No collection_id configured, cannot refresh id map")
            return
        url = f"{self.__base_url}/collections/{collection_id}"
        headers = {"Content-Type": "application/json"}
        headers["Authorization"] = f"Bearer {self.token}"
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 401:
                # unauthorized: check status endpoint and try refreshing token then retry once
                logger.warning("ID map fetch returned 401, checking auth status")
                self._refresh_token()
                headers["Authorization"] = f"Bearer {self.token}"
                resp = requests.get(url, headers=headers)
                if resp.status_code != 200:
                    logger.error(f"ID map fetch failed {resp.status_code}: {resp.text}")
                    return
            if resp.status_code != 200:
                logger.error(f"ID map fetch failed {resp.status_code}: {resp.text}")
                return
            else:
                logger.debug(f"ID map fetch succeeded: {resp.json()}")
        except Exception as e:
            logger.error(f"Error during ID map fetch request: {e}")

        body = resp.json()
        for entry in body.get("records", []):
            pk_value = entry.get("data", {}).get(pk_field)
            logger.info(f"Name: {pk_value} ID: {entry.get('id')}")
            self._id_map[collection_id] = self._id_map.get(collection_id, {})
            self._id_map[collection_id][pk_field] = self._id_map[collection_id].get(
                pk_field, {}
            )
            self._id_map[collection_id][pk_field][pk_value] = entry.get("id")


class IDMap:
    inst = {}

    @classmethod
    def get_inst(cls, key):
        if cls.inst.get(key) is None:
            cls.inst[key] = cls(key)
        return cls.inst[key]

    def __init__(self, key):
        self.map = {}
        self.key = key

    def insert(self, k, v):
        self.map[k] = v

    def add(self, data, record):
        self.map[data[self.key]] = record["id"]

    def get(self, data):
        return self.map.get(data[self.key])


def write_history(
    config,
    fields={},
    timestamp_field="_time",
    collection_id=None,
    kb_pk_field="machineName",
    data_pk_field="machine",
):
    kinabase_conf = config["kinabase"]
    base_url = kinabase_conf.get("base_url", "https://app.kinabase.com/api/v1")
    collection_id = (
        collection_id
        if collection_id is not None
        else kinabase_conf.get("collection_id")
    )
    # mapping from full topic to collection id, overrides default
    app_id = kinabase_conf.get("app_id")
    secret = kinabase_conf.get("secret")

    async def wrapped(data_frame):
        logger.info(f"data_frame: {data_frame}")
        kb = KBManager.get_inst(app_id, secret, base_url)

        attempt_count = 0
        while attempt_count < 2:
            try:
                _send_ingest(
                    kb,
                    base_url,
                    kb.token,
                    data_frame.to_dict(orient="records"),
                    collection_id,
                    fields,
                    kb_pk_field,
                    data_pk_field,
                    timestamp_field,
                )
                break
            except TokenExpired:
                kb.expire_token()
                attempt_count += 1

        return data_frame

    return wrapped


def write_record(config, fields={}, pk_field="pk", collection_id=None):
    kinabase_conf = config["kinabase"]
    base_url = kinabase_conf.get("base_url", "https://app.kinabase.com/api/v1")
    collection_id = (
        collection_id
        if collection_id is not None
        else kinabase_conf.get("collection_id")
    )
    # mapping from full topic to collection id, overrides default
    app_id = kinabase_conf.get("app_id")
    secret = kinabase_conf.get("secret")

    async def wrapped(record):

        kb = KBManager.get_inst(app_id, secret, base_url)
        attempt_count = 0
        while attempt_count < 2:
            try:
                _send_single(
                    base_url, kb.token, record, collection_id, fields, pk_field
                )
                break
            except TokenExpired:
                kb.expire_token()
                attempt_count += 1

        return record

    return wrapped


def update_record(
    config,
    fields={},
    kb_pk_field="pk",
    data_pk_field="machine",
    collection_id=None,
):
    kinabase_conf = config["kinabase"]
    base_url = kinabase_conf.get("base_url", "https://app.kinabase.com/api/v1")
    collection_id = (
        collection_id
        if collection_id is not None
        else kinabase_conf.get("collection_id")
    )
    # mapping from full topic to collection id, overrides default
    app_id = kinabase_conf.get("app_id")
    secret = kinabase_conf.get("secret")

    async def wrapped(record):
        logger.info(f"record: {record}")

        kb = KBManager.get_inst(app_id, secret, base_url)

        attempt_count = 0
        while attempt_count < 2:
            try:
                _update_single(
                    kb,
                    base_url,
                    kb.token,
                    record,
                    collection_id,
                    fields,
                    kb_pk_field,
                    data_pk_field,
                )
                break
            except TokenExpired:
                kb.expire_token()
                attempt_count += 1

        return record

    return wrapped


def _send_ingest(
    kb: KBManager,
    base_url,
    token,
    records,
    cid,
    fields,
    kb_pk_field,
    data_pk_field,
    timestamp_field="_time",
):
    """POST the provided list of record dicts to the ingest endpoint.

    ``collection_id`` may be provided per-call; if not, the default
    stored during initialization is used.
    """
    if not records:
        return

    if not cid:
        logger.error("No collection_id configured, cannot upload")
        return

    url = f"{base_url}/collections/{cid}/ingest"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    body = {
        "mode": "FUTURE_FACING",
        "records": _to_records(
            kb, records, fields, cid, kb_pk_field, data_pk_field, timestamp_field
        ),
    }
    logger.info(f"url: {url}")
    logger.info(f"body: {body}")
    try:
        resp = requests.post(url, headers=headers, json=body)
        if resp.status_code == 401:
            # unauthorized: check status endpoint and try refreshing token then retry once
            logger.warning("Bulk upload returned 401, checking auth status")
            raise TokenExpired()
        if resp.status_code != 200:
            logger.error(f"Bulk upload failed {resp.status_code}: {resp.text}")
        else:
            logger.info(f"Bulk upload succeeded: {resp.json()}")
    except Exception as e:
        logger.error(f"Error during bulk upload request: {e}")


def get_id_for_pk(collection_id, base_url, token, pk_field, pk_value):
    if collection_id is None:
        logger.error("No collection_id configured, cannot refresh id map")
        return
    index = 0
    found = False
    while not found:
        url = f"{base_url}/collections/{collection_id}?pageIndex={index}"
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            resp = requests.get(url, headers=headers)
            if resp.status_code == 401:
                # unauthorized: check status endpoint and try refreshing token then retry once
                logger.warning("Bulk upload returned 401, checking auth status")
                raise TokenExpired()
            if resp.status_code != 200:
                logger.error(f"Bulk upload failed {resp.status_code}: {resp.text}")
            else:
                logger.debug(f"Bulk upload succeeded: {resp.json()}")
        except Exception as e:
            logger.error(f"Error during bulk upload request: {e}")

        body = resp.json()
        records = body.get("records", [])
        if len(records) == 0:
            logger.error(f"Could not find id for pk {pk_value} in field {pk_field}")
            return None

        for entry in records:
            value = entry.get("data", {}).get(pk_field)
            if value == pk_value:
                return entry.get("id")

        index += 1


def _to_records(
    kb: KBManager,
    entries,
    fields,
    collection,
    kb_pk_field,
    data_pk_field,
    timestamp_field,
):
    # group entries by machine name, then create a record set per machine
    grouped_entries = {}
    for entry in entries:
        data_pk = entry.get(data_pk_field)
        if data_pk not in grouped_entries:
            grouped_entries[data_pk] = []
        grouped_entries[data_pk].append(entry)

    record_set = []

    for data_pk, entries in grouped_entries.items():
        logger.debug(f"{data_pk_field}: {data_pk}")
        id = kb.id_map(collection, kb_pk_field).get(data_pk)
        if not id:
            kb.refresh_id_map(collection, kb_pk_field)
            id = kb.id_map(collection, kb_pk_field).get(data_pk)
            if not id:
                logger.error(f"No pk found for {data_pk_field} {data_pk}")
                continue

        record_set.append(
            {
                "id": id,
                "changes": [
                    {
                        "timestamp": entry.get(timestamp_field)
                        .to_pydatetime()
                        .isoformat(),
                        "data": {  # group by timestamp across machine
                            field_name: entry.get(field_key)
                            for field_name, field_key in fields.items()
                            if entry.get(field_key) is not None
                        },
                    }
                    for entry in entries
                ],
            }
        )

    return record_set


def _send_single(base_url, token, record, cid, fields, pk_field):
    if not record:
        return

    if not cid:
        logger.error("No collection_id configured, cannot upload")
        return

    url = f"{base_url}/collections/{cid}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    body = {
        "id": None,
        "numericId": None,
        "data": {
            field_name: record.get(field_key)
            for field_name, field_key in fields.items()
        },
        "external": None,
        "lists": None,
        "fileStorage": None,
    }
    logger.info(f"url: {url}")
    logger.info(f"body: {body}")
    try:
        resp = requests.post(url, headers=headers, json=body)
        if resp.status_code == 401:
            # unauthorized: check status endpoint and try refreshing token then retry once
            logger.warning("upload returned 401, checking auth status")
            raise TokenExpired()
        if resp.status_code != 200:
            logger.error(f"upload failed {resp.status_code}: {resp.text}")
        else:
            result = resp.json()
            IDMap.get_inst(pk_field).add(record, result)
            logger.info(f"upload succeeded: {result}")
    except Exception as e:
        logger.error(f"Error during upload request: {e}")


def _update_single(
    kb: KBManager, base_url, token, record, cid, fields, kb_pk_field, data_pk_field
):
    if not record:
        return

    if not cid:
        logger.error("No collection_id configured, cannot upload")
        return

    id = kb.id_map(cid, kb_pk_field).get(record[data_pk_field])
    if id is None:
        kb.refresh_id_map(cid,kb_pk_field)
        id = kb.id_map(cid, kb_pk_field).get(record[data_pk_field])
        if id is None:
            logger.error(
                f"Can't find matching ID for {data_pk_field} of {record[data_pk_field]} in {kb_pk_field} field of {cid} collection"
            )

    url = f"{base_url}/collections/{cid}/{id}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    body = {
        "id": id,
        "numericId": id,
        "data": {
            field_name: record.get(field_key)
            for field_name, field_key in fields.items()
        },
        "external": None,
        "lists": None,
        "fileStorage": None,
    }
    logger.info(f"url: {url}")
    logger.info(f"body: {body}")
    try:
        resp = requests.patch(url, headers=headers, json=body)
        if resp.status_code == 401:
            # unauthorized: check status endpoint and try refreshing token then retry once
            logger.warning("Bulk upload returned 401, checking auth status")
            raise TokenExpired()
        if resp.status_code != 200:
            logger.error(f"Bulk upload failed {resp.status_code}: {resp.text}")
        else:
            logger.info(f"Bulk upload succeeded: {resp.json()}")
    except Exception as e:
        logger.error(f"Error during bulk upload request: {e}")
