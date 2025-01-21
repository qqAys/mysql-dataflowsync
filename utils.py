import binascii
import datetime
import decimal
import hashlib
import json
import os
import pickle
import random
import time
from pathlib import Path

import pymysql
import yaml
from loguru import logger
from pymysql.err import OperationalError

from field_mappings import field_mappings, field_mappings_raw


LOG_SQL_MAX_RETRY = 15
TARGET_SQL_INSERT_MAX_RETRY = 60


PROJECT_DATA_BASE_PATH = Path("/mysql-dataflowsync_data")
PROJECT_DATA_BASE_PATH.mkdir(exist_ok=True)

with open(Path(PROJECT_DATA_BASE_PATH, "config.yml"), "r") as config_file:
    config_data = yaml.safe_load(config_file)

PROJECT_NAME = config_data["project_name"]
QUEUE_PATH = Path(PROJECT_DATA_BASE_PATH, "persist_queue")
LOG_PATH = Path(PROJECT_DATA_BASE_PATH, "logs")
LOG_DB_PASSWORD = os.getenv("MARIADB_ROOT_PASSWORD")
DML_SERIALIZATION = config_data["dml_serialization"]


def log_init():

    LOG_PATH.mkdir(exist_ok=True, parents=True)

    cdc_logger = logger.bind(name="CDC")
    dpu_logger = logger.bind(name="DPU")
    error_logger = logger.bind(name="ERROR")

    # logger.remove()

    log_format = "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {name}:{line} | {function} >>> {message}"

    logger.add(  # cdc log
        sink=Path(LOG_PATH, "cdc.log"),
        format=log_format,
        rotation="00:00",
        compression="zip",
        filter=lambda record: record["extra"].get("name") == "CDC",
    )
    logger.add(  # dpu log
        sink=Path(LOG_PATH, "dpu.log"),
        format=log_format,
        rotation="00:00",
        compression="zip",
        filter=lambda record: record["extra"].get("name") == "DPU",
    )
    logger.add(  # error log
        sink=Path(LOG_PATH, "error.log"),
        format=log_format,
        rotation="100 MB",
        compression="zip",
        filter=lambda record: record["extra"].get("name") == "ERROR",
    )

    return cdc_logger, dpu_logger, error_logger


def pickle_loads(hex_string):
    """
    pickle de-serialisation
    :param hex_string:
    :return:
    """
    binary_data = binascii.unhexlify(hex_string.replace("0x", ""))

    # de-serialisation
    original_object = pickle.loads(binary_data)

    return original_object


def dict_to_hash(_dict):
    """
    Converting a dictionary to a hash
    :param _dict:
    :return:
    """

    # Dictionary is unordered, convert dictionary to sorted JSON string.
    sorted_dict_str = json.dumps(_dict, sort_keys=True)

    # Generate a hash
    hash_object = hashlib.md5(sorted_dict_str.encode())
    return hash_object.hexdigest()


def generate_random_server_id() -> int:
    """
    Generate a random SERVER ID
    :return: int
    """
    ts_str = str(time.time()).replace(".", "")

    ts_choices_two_list = random.choices(str(ts_str), k=2)

    random_one = str(random.choice(range(100, 999)))

    random_number = "".join(ts_choices_two_list) + random_one

    return int(random_number)


def convert_values(_dict: dict) -> dict:
    """
    Iterate through the dictionary and convert the type of the value
    :param _dict:
    :return:
    """
    for key, value in _dict.items():

        # Time type 1
        if isinstance(value, datetime.datetime):
            _dict[key] = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        # Time type 2
        elif isinstance(value, datetime.date):
            _dict[key] = value.strftime("%Y-%m-%d")

        # Exact Fractional Types
        elif isinstance(value, decimal.Decimal):
            _dict[key] = float(value)

        # If it's a dictionary type then another conversion
        elif isinstance(value, dict):
            convert_values(value)
    return _dict


def find_values_differences(cdc_data: dict):
    """
    Finding field differences before and after updates
    :param cdc_data:
    :return:
    """
    before_values = cdc_data["before_values"]
    after_values = cdc_data["after_values"]
    update_values = {}

    common_keys = set(before_values.keys()) & set(after_values.keys())

    for key in common_keys:
        if before_values[key] != after_values[key]:
            update_values[key] = after_values[key]

    return update_values


def mapping_data(table: str, action: str, event_raw_data: dict) -> dict:
    """
    Table name mapping
    :param table: table name
    :param action: action
    :param event_raw_data: raw event with **UNKNOWN_COL[X]**
    :return.
    """
    # value type conversion
    event_raw_data = convert_values(event_raw_data)

    # If it is an update action, it will output the value before and after the update.
    if action == "update":
        # before updating
        before_values_mapped_data = {
            field_mappings_raw[table][int(col.replace("UNKNOWN_COL", ""))]: val
            for col, val in event_raw_data["data"]["before_values"].items()
        }
        # after updating
        after_values_mapped_data = {
            field_mappings_raw[table][int(col.replace("UNKNOWN_COL", ""))]: val
            for col, val in event_raw_data["data"]["after_values"].items()
        }
        event_raw_data["data"] = {
            "before_values": before_values_mapped_data,
            "after_values": after_values_mapped_data,
        }
        return event_raw_data
    else:
        # Insert and Delete Actions
        mapped_data = {
            field_mappings_raw[table][int(col.replace("UNKNOWN_COL", ""))]: val
            for col, val in event_raw_data["data"].items()
        }
        event_raw_data["data"] = mapped_data
        return event_raw_data


def generate_insert_statement(func_name: str, data: dict):
    """
    Generate an INSERT statement for the database

    :param func_name: method name
    :param data: Dictionary of data to be inserted, the key is the field name and the value is the field value.
    :return: generated INSERT statement string
    """
    # Extract field name
    table_name = func_name.replace("_process_", "")

    # Get source database field mapping
    source_column_map = field_mappings_raw[table_name]
    # Get source database field mapping
    target_column_map = field_mappings[table_name]

    # Building a List of Fields
    columns = [target_column_map[i] for i in sorted(target_column_map.keys()) if target_column_map[i] != "id"]
    columns_str = ", ".join(columns).replace("last_value", "`last_value`")

    # Constructing a list of values
    values = []
    for i in target_column_map.keys():
        source_column = source_column_map[i]
        if target_column_map[i] == "id":
            continue
        _value = data[source_column]
        if isinstance(_value, str):
            # If the value is a string, put it in quotes.
            values.append(f"'{_value}'")
        elif _value is None:
            # If the value is None, use NULL
            values.append("NULL")
        else:
            # Values of other types are used directly
            values.append(str(_value))

    values_str = ", ".join(values)

    # Building a Complete INSERT Statement
    insert_statement = (
        f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"
    )

    return insert_statement


def generate_update_statement(func_name: str, cdc_data: dict, where_clause: str):
    """
    Generate an UPDATE statement for a database

    :param func_name: method name
    :param cdc_data: Pre- and post-update data generated by the CDC.
    :param where_clause: constraint string, e.g. ‘id = 3’
    :return: generated UPDATE statement string
    """
    table_name = func_name.replace("_process_", "")
    # Constructing the SET Clause
    update_fields = find_values_differences(cdc_data)
    if len(update_fields) == 0:
        return None

    # Processing Field Name
    new_data = {}
    for field in update_fields:
        field_raw = field_mappings_raw.get(table_name)
        field_pos = list(filter(lambda x: field_raw[x] == field, field_raw))[0]
        new_data[field_mappings[table_name][field_pos]] = update_fields[field]
    update_fields = new_data

    set_clauses = []
    for field, value in update_fields.items():
        if isinstance(value, str):
            set_clauses.append(f"{field} = '{value}'")
        else:
            set_clauses.append(f"{field} = {value}")

    set_clause = ", ".join(set_clauses)

    # Constructing a Complete UPDATE Statement
    update_statement = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"

    return update_statement


class LogDBConnection:
    """
    Log database connection class
    """

    def __init__(
            self,
            host,
            port,
            user,
            password,
    ):
        """
        Initialising connection parameters
        :param host:
        :param port:
        :param user:
        :param password:
        """
        _, self.dpu_logger, self.error_logger = log_init()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = f"dfs_{PROJECT_NAME}"
        self.connection = None
        self.initialized = True
        self.connect()

    def connect(self):
        """
        establish a connection
        :return:
        """
        try:
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )
            print(f"{datetime.datetime.now()} | Log database is connected, checking if initialisation is required...")
        except OperationalError as e:
            self.error_logger.warning(e.__str__())
            time.sleep(5)
            self.connect()

        with self.connection.cursor() as cursor:
            cursor.execute(f"show databases like '{self.database}';")
            if cursor.fetchone() is None:
                self.error_logger.warning(f"{self.database} Database does not exist")
                cursor.execute(f"create database `{self.database}` collate utf8mb4_general_ci;")
                self.connection.commit()
                self.initialized = False
                self.error_logger.warning(f"{self.database} Database created")

            self.connection.close()
            self.connection = None

        self.connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        if self.initialized is False:
            self.initialize()

    def initialize(self):
        """
        Initialising the database
        :return:
        """
        with self.connection.cursor() as cursor:
            _sql = """create table if not exists cdc_log
            (
                cdc_id             int auto_increment
                    primary key,
                cdc_dt             datetime(3)                         not null,
                log_file           varchar(16)                         not null,
                log_pos            int                                 not null,
                log_dt             datetime                            not null,
                `table`            varchar(64)                         not null,
                action             enum ('insert', 'update', 'delete') not null,
                dpu_process_status tinyint(1) default 0                null,
                data               longblob                            not null
            )
                partition by hash (`cdc_id`) partitions 10;"""
            cursor.execute(_sql)
            _sql = """create table if not exists dpu_log
            (
                dpu_id             int auto_increment
                    primary key,
                cdc_id             int                                 not null,
                dt                 datetime(3)                         not null,
                `table`            varchar(64)                         not null,
                action             enum ('insert', 'update', 'delete') not null,
                dml_execute_status tinyint(1) default 0                null,
                dml                longblob                            not null
            )
                partition by hash (`dpu_id`) partitions 10;"""
            cursor.execute(_sql)
            _sql = """create table if not exists db_rel
            (
                rel_id       int auto_increment
                    primary key,
                field_define varchar(64) not null,
                old_id       int         not null,
                new_id       int         not null
            )
                partition by hash (`rel_id`) partitions 5;"""
            cursor.execute(_sql)

            def create_index(table_name, index_name, column_name):
                """
                Creating Indexes
                :param table_name:
                :param index_name:
                :param column_name:
                :return:
                """
                sql = f"""create index if not exists {index_name} on {table_name} ({column_name});"""
                cursor.execute(sql)

            create_index("cdc_log", "idx_cdc_id", "cdc_id")
            create_index("cdc_log", "idx_cdc_dt", "cdc_dt")
            create_index("cdc_log", "idx_cdc_log_pos", "log_pos")
            create_index("cdc_log", "idx_cdc_log_dt", "log_dt")
            create_index("dpu_log", "idx_dpu_id", "dpu_id")
            create_index("dpu_log", "idx_dpu_cdc_id", "cdc_id")
            create_index("dpu_log", "idx_dpu_dt", "dt")
            create_index("db_rel", "idx_rel", "field_define, old_id")

        self.connection.commit()
        print(f"{datetime.datetime.now()} | Log database initialised successfully")

    def cdc_max_log_pos_query(self) -> tuple:
        """
        Get the current location of the largest binlog
        :return:
        """
        _sql = """select log_file, log_pos
        from cdc_log
        order by cdc_id desc
        limit 1;"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(_sql)
                self.connection.commit()
                try:
                    return cursor.fetchone()
                except TypeError:
                    return None, None
        except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
            self.error_logger.error(f"CDC max_log_pos enquiry error: {e}")
            return None, None

    def cdc_processed_execute_insert(self, event_data: dict, retry=0):
        """
        Insert data into the cdc_log table
        :param event_data.
        :param retry: retry count
        :return:
        """
        if retry >= LOG_SQL_MAX_RETRY:
            self.error_logger.critical(f"Reconnected  {LOG_SQL_MAX_RETRY} times, will return cdc_id = -1")
            return -1

        log_file = event_data["log_file"]
        log_pos = event_data["log_pos"]
        log_dt = event_data["log_dt"]
        cdc_dt = event_data["cdc_dt"]
        table = event_data["table"]
        action = event_data["action"]
        data = event_data["data"]
        _sql = """insert into cdc_log (cdc_dt, log_file, log_pos, log_dt, `table`, action, data)
        values (%s, %s, %s, %s, %s, %s, %s);"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    _sql,
                    (
                        cdc_dt,
                        log_file,
                        log_pos,
                        log_dt,
                        table,
                        action,
                        pickle.dumps(data),
                    ),
                )
                self.connection.commit()
                if retry != 0:
                    self.error_logger.warning(f"Reconnect successfully, statement executed successfully")
                return cursor.lastrowid
        except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
            self.error_logger.error(
                f"CDC log database insertion error: {e} {(cdc_dt, log_pos, log_dt, table, action, data)}"
            )
            self.error_logger.warning(f"Trying to reconnect, current number of attempts: {retry + 1}")
            try:
                self.connection.close()
            except:
                pass
            time.sleep(1)
            self.connect()
            return self.cdc_processed_execute_insert(event_data, retry=retry + 1)

    def dpu_processed_log_insert(self, raw, dml, retry=0):
        """
        Insert data into the dpu_log table
        :param raw.
        :param dml.
        :param retry: retry count
        :return:
        """
        if dml is None:
            return None
        if retry >= LOG_SQL_MAX_RETRY:
            self.error_logger.critical(f"Reconnected  {LOG_SQL_MAX_RETRY} times, will return dpu_id = -1")
            return -1
        cdc_id = raw["cdc_id"]
        table = raw["table"]
        action = raw["action"]
        dt = datetime.datetime.now()
        _sql = """insert into dpu_log (cdc_id, dt, `table`, action, dml)
        values  (%s, %s, %s, %s, %s);"""
        _sql = _sql.replace("None", "null")

        # DPU Processing Completed SQL
        _dpu_done_sql = """update cdc_log
        set dpu_process_status = 1
        where cdc_id = %s;"""
        try:
            with self.connection.cursor() as cursor:
                if DML_SERIALIZATION is False:
                    cursor.execute(_sql, (cdc_id, dt, table, action, dml))
                else:
                    cursor.execute(_sql, (cdc_id, dt, table, action, pickle.dumps(dml)))
                dpu_id = cursor.lastrowid
                cursor.execute(_dpu_done_sql, cdc_id)
                self.connection.commit()
                if retry != 0:
                    self.error_logger.warning(f"Reconnect successfully, statement executed successfully")
                log_info = {
                    "dpu_id": dpu_id,
                    "cdc_id": cdc_id,
                    "dt": dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "table": table,
                    "dml": dml,
                }
                self.dpu_logger.info(log_info)
                return dpu_id
        except Exception as e:
            self.error_logger.error(
                f"[cdc_id: {cdc_id}] DPU log database insertion error: {e} {(cdc_id, dt, table, action, dml)}"
            )
            self.error_logger.warning(f"Trying to reconnect, current number of attempts: {retry + 1}")
            try:
                self.connection.close()
            except:
                pass
            time.sleep(1)
            self.connect()
            return self.dpu_processed_log_insert(raw, dml, retry=retry + 1)

    def dpu_after_dml_execute_update(self, dpu_id):
        """
        Update dml execution status
        :param dpu_id:
        :return:
        """
        _sql = """update dpu_log
        set dml_execute_status = 1
        where dpu_id = %s;"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(_sql, dpu_id)
                self.connection.commit()
                self.dpu_logger.info({"dpu_id": dpu_id, "execute": True})
                return True
        except Exception as e:
            self.error_logger.error(f"[dpu_id: {dpu_id}] DPU log database update error: {e}")
            return False

    def dpu_relationship_query(self, field_define, old_id):
        """
        Query DPU Relationship
        :param field_define:
        :param old_id:
        :return:
        """
        _sql = """select new_id
        from db_rel
        where field_define = %s
          and old_id = %s;"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(_sql, (field_define, old_id))
                self.connection.commit()
                try:
                    return cursor.fetchone()[0]
                except TypeError:
                    return old_id
        except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
            self.error_logger.error(
                f"DPU log database update error: {e} field: {field_define} old_id: {old_id}"
            )
            return old_id

    def dpu_relationship_create(self, field_define, old_id, new_id, retry=0):
        """
        Creating DPU Relationships
        :param field_define:
        :param old_id:
        :param new_id:
        :param retry: retry count
        :return:
        """
        if old_id == new_id:
            return None
        if retry >= LOG_SQL_MAX_RETRY:
            self.error_logger.critical(f"Reconnected  {LOG_SQL_MAX_RETRY} times, will not return rel_id")
            return None
        _sql = (
            """insert into db_rel (field_define, old_id, new_id) values (%s, %s, %s);"""
        )
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(_sql, (field_define, old_id, new_id))
                self.connection.commit()
                rel_id = cursor.lastrowid
                self.dpu_logger.info(
                    {
                        "rel_id": rel_id,
                        "field": field_define,
                        "old_id": old_id,
                        "new_id": new_id,
                    }
                )
                if retry != 0:
                    self.error_logger.warning(f"Reconnect successfully, statement executed successfully")
                return rel_id
        except (pymysql.err.OperationalError, pymysql.err.InterfaceError) as e:
            self.error_logger.error(
                f"DPU relational database insertion error: {e} field: {field_define} old_id: {old_id} new_id: {new_id}"
            )
            self.error_logger.warning(f"Trying to reconnect, current number of attempts: {retry + 1}")
            try:
                self.connection.close()
            except:
                pass
            time.sleep(1)
            self.connect()
            return self.dpu_relationship_create(
                field_define, old_id, new_id, retry=retry + 1
            )


class TargetDBConnection:
    """
    Target database connection
    """

    def __init__(self, host, port, user, password, schemas):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = password
        self.schemas = schemas
        self.connection = pymysql.connect(
            host=host, user=user, password=password, port=port, database=schemas
        )
        # 日志
        _, _, self.error_logger = log_init()

    def insert_and_update(self, sql, retry=0):
        """
        Execute the insert statement
        :param sql:
        :param retry: Retry count
        :return:
        """
        if sql is None:
            return None
        if retry >= TARGET_SQL_INSERT_MAX_RETRY:
            self.error_logger.critical(
                f"Re-connected  {TARGET_SQL_INSERT_MAX_RETRY} times, failed to execute DML statement on target database, will return None {sql}"
            )
            return None
        sql = sql.replace("None", "null")
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                self.connection.commit()
                if retry != 0:
                    self.error_logger.warning(f"Reconnect successfully, statement executed successfully")
                if sql.startswith("UPDATE") or sql.startswith("DELETE"):
                    return cursor.rowcount
                else:
                    return cursor.lastrowid
        except (
                pymysql.err.OperationalError,
                pymysql.err.InterfaceError,
                pymysql.err.IntegrityError,
        ) as e:
            self.error_logger.error(f"Target database insert/update/delete errors: {e} {sql}")
            self.error_logger.warning(f"Trying to reconnect, current number of attempts: {retry + 1}")
            try:
                self.connection.close()
            except:
                pass
            time.sleep(5)
            self.__init__(self.host, self.port, self.user, self.passwd, self.schemas)
            return self.insert_and_update(sql, retry=retry + 1)


if __name__ == "__main__":
    pass
