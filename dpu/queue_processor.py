import datetime
import inspect

from persist_queue import PersistQueue
from utils import (
    TargetDBConnection,
    LogDBConnection,
    config_data,
    generate_update_statement,
    generate_insert_statement,
    log_init,
    PROJECT_NAME,
    LOG_DB_PASSWORD,
)

INSERT = "insert"
UPDATE = "update"
DELETE = "delete"

# If the DPU is running inside a docker container, there is no need to change the following log database connections.
LOG_MARIADB_SETTINGS = {
    "host": "db",
    "port": 3306,
    "user": "root",
    "passwd": LOG_DB_PASSWORD,
}
TARGET_MYSQL_SETTINGS = {
    "host": config_data["target_database"]["host"],
    "port": config_data["target_database"]["port"],
    "user": config_data["target_database"]["user"],
    "passwd": config_data["target_database"]["passwd"],
    "schemas": config_data["target_database"]["schemas"]
}


class DPU:
    """
    The DPU (Data Processing Unit) class is responsible for handling data processing tasks from a queue.
    It connects to both a log database and a target database, processes incoming CDC (Change Data Capture)
    data based on predefined table processors, and logs the results.

    Attributes:
        error_logger (Logger): Logger instance for capturing errors.
        log_db (LogDBConnection): Connection object for the log database.
        target_db (TargetDBConnection): Connection object for the target database.
        table_processors (dict): Dictionary mapping table names to their respective processing methods.

    Methods:
        __init__(): Initializes the DPU instance, establishes database connections, and sets up table processors.
        _get_queue_item(): Retrieves an item from the persistence queue.
        _handle_process_data(raw): Processes raw CDC data by routing it to the appropriate table processor.
        start(): Continuously retrieves items from the queue and processes them.

        ---example---
        _process_example_table(raw): Processes data for 'example_table', handling INSERT, UPDATE, and DELETE actions.
        _process_skip_table(raw): Placeholder method for tables that should be skipped during processing.
        ---example---
    """

    def __init__(self):
        """
        Initializes the DPU instance. Sets up logging, establishes connections to the log and target databases,
        and defines table-specific processing methods.
        """

        print(
            f"\n\n{datetime.datetime.now()} | ==========DMP SERVER DPU(Unit) START=========="
        )
        print(f"{datetime.datetime.now()} | Project: {PROJECT_NAME}")

        _, _, self.error_logger = log_init()

        self.log_db = LogDBConnection(
            host=LOG_MARIADB_SETTINGS["host"],
            port=LOG_MARIADB_SETTINGS["port"],
            user=LOG_MARIADB_SETTINGS["user"],
            password=LOG_MARIADB_SETTINGS["passwd"],
        )

        print(f"{datetime.datetime.now()} | DPU log database connection successful")

        self.target_db = TargetDBConnection(
            host=TARGET_MYSQL_SETTINGS["host"],
            port=TARGET_MYSQL_SETTINGS["port"],
            user=TARGET_MYSQL_SETTINGS["user"],
            password=TARGET_MYSQL_SETTINGS["passwd"],
            schemas=TARGET_MYSQL_SETTINGS["schemas"],
        )

        print(f"{datetime.datetime.now()} | Target database connection successful")

        self.table_processors = {
            "example_table": self._process_example_table,
            "skip_table": self._process_skip_table,
        }

    @staticmethod
    def _get_queue_item():
        """
        Retrieves an item from the persistence queue.

        Returns:
            dict: A dictionary containing CDC data.
        """

        return PersistQueue.get()

    def _handle_process_data(self, raw):
        """
        Processes raw CDC data by identifying the appropriate table processor and executing it.

        Args:
            raw (dict): Raw CDC data including table name and action type.

        Raises:
            ValueError: If no processor is found for the specified table.
        """

        print(f"{datetime.datetime.now()} | processing cdc_id:{raw['cdc_id']}", flush=True)

        table_name = raw["table"]
        processor = self.table_processors.get(table_name)

        if processor is None:
            raise ValueError(f"No handler for this table: {table_name}")
        else:
            try:
                processor(raw)
                print(f"{datetime.datetime.now()} | processing complete | cdc_id:{raw['cdc_id']}", flush=True)
            except Exception as e:
                self.error_logger.critical(e, raw)
                print(f"{datetime.datetime.now()} | processing failure | cdc_id:{raw['cdc_id']}", flush=True)

    def start(self):
        """
        Starts the continuous processing loop. Retrieves items from the queue and processes each one until interrupted.
        """

        while True:
            queue_item = self._get_queue_item()
            self._handle_process_data(queue_item)

    def _process_example_table(self, raw):
        """
        Processes data for 'example_table' based on the action type (INSERT, UPDATE, DELETE).

        Args:
            raw (dict): Raw CDC data specific to 'example_table'.
        """

        cdc_data = raw["data"]
        action = raw["action"]

        # INSERT action
        if action == INSERT:

            # Deconstructing Raw Data
            primary_id = cdc_data["id"]
            foreign_id = cdc_data["foreign_id"]

            # Query if there is a DPU relationship, if so replace the value.
            cdc_data["foreign_id"] = self.log_db.dpu_relationship_query(
                "foreign_id", foreign_id
            )

            # DML build
            dml = generate_insert_statement(
                inspect.currentframe().f_code.co_name, cdc_data
            )

            # DPU logging and obtaining DPU log IDs
            dpu_id = self.log_db.dpu_processed_log_insert(raw, dml)

            # Submit the target database and get a new ID
            row_id = self.target_db.insert_and_update(dml)

            # Update DPU-DML Execution Status
            if row_id != 0 and row_id is not None:
                self.log_db.dpu_after_dml_execute_update(dpu_id)

                # Creating DPU Relationships
                self.log_db.dpu_relationship_create("primary_id", primary_id, row_id)

        # UPDATE action
        elif action == UPDATE:

            primary_id = cdc_data["after_values"]["id"]
            foreign_id = cdc_data["after_values"]["foreign_id"]

            new_primary_id = self.log_db.dpu_relationship_query(
                "primary_id", primary_id
            )
            cdc_data["after_values"]["foreign_id"] = self.log_db.dpu_relationship_query(
                "foreign_id", foreign_id
            )

            dml = generate_update_statement(
                inspect.currentframe().f_code.co_name,
                cdc_data,
                f"id = {new_primary_id}",
            )

            dpu_id = self.log_db.dpu_processed_log_insert(raw, dml)

            # Commit the target database and get the number of rows affected
            rowcount = self.target_db.insert_and_update(dml)

            # Update DPU-DML Execution Status
            if rowcount != 0 and rowcount is not None:
                self.log_db.dpu_after_dml_execute_update(dpu_id)

        # DELETE action
        elif action == DELETE:

            primary_id = cdc_data["id"]

            new_primary_id = self.log_db.dpu_relationship_query(
                "primary_id", primary_id
            )

            # Delete actions are rarely encountered when I actually use them, so here you need to manually process the delete sql statement.
            delete_sql = f"""DELETE FROM example_tabl WHERE id = {new_primary_id};"""

            dpu_id = self.log_db.dpu_processed_log_insert(raw, delete_sql)

            row_count = self.target_db.insert_and_update(delete_sql)

            if row_count != 0:
                self.log_db.dpu_after_dml_execute_update(dpu_id)

        else:
            pass

    def _process_skip_table(self, raw):
        """
        Placeholder method for tables that should be skipped during processing.

        Args:
            raw (dict): Raw CDC data for the table to be skipped.
        """

        pass


if __name__ == "__main__":
    dpu = DPU()
