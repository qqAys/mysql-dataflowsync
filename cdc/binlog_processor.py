import datetime
from time import strftime, localtime

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from persist_queue import PersistQueue
from utils import (
    LogDBConnection,
    config_data,
    log_init,
    PROJECT_NAME,
    mapping_data,
    generate_random_server_id,
    LOG_DB_PASSWORD,
)


class CDC:
    """
    The CDC (Change Data Capture) class is responsible for capturing changes from a source MySQL database's binlog.
    It initializes connections to the source and log databases, processes binlog events, logs these events, and queues them for further processing.

    Attributes:
        cdc_logger (Logger): Logger instance for general CDC logging.
        error_logger (Logger): Logger instance for capturing errors.
        SOURCE_MYSQL_SETTINGS (dict): Configuration settings for connecting to the source MySQL database.
        SOURCE_MYSQL_SERVER_ID (int): Unique server ID used for the binlog connection.
        SOURCE_MYSQL_ONLY_SCHEMAS (list): List of schemas to monitor in the source database.
        LOG_MARIADB_SETTINGS (dict): Configuration settings for connecting to the log MariaDB database.
        log_db (LogDBConnection): Connection object for the log database.
        bin_log_file (str): Current binlog file being processed.

    Methods:
        __init__(): Initializes the CDC instance, sets up logging, establishes database connections, and configures binlog monitoring.
        start(): Initiates the binlog capture process by determining the starting position and processing events.
        binlog_connection(log_file=None, log_pos=None): Establishes a connection to the source database's binlog stream.
        binlog_processor(stream): Processes events from the binlog stream, logs them, and queues them for further processing.
    """

    def __init__(self):
        """
        Initializes the CDC instance. Sets up logging, establishes connections to the source and log databases,
        and configures binlog monitoring using the provided configuration settings.
        """

        print(f"\n\n{datetime.datetime.now()} | ==========DMP SERVER CDC-Unit START==========")
        print(f"{datetime.datetime.now()} | Project: {PROJECT_NAME}")

        # Log Initialisation
        self.cdc_logger, _, self.error_logger = log_init()

        # Source database connection
        self.SOURCE_MYSQL_SETTINGS = {
            "host": config_data["source_database"]["host"],
            "port": config_data["source_database"]["port"],
            "user": config_data["source_database"]["user"],
            "passwd": config_data["source_database"]["passwd"],
        }

        # If you need to specify a SERVER ID, you can read it from the configuration
        # self.SOURCE_MYSQL_SERVER_ID = config_data["source_database"]["server_id"]

        self.SOURCE_MYSQL_SERVER_ID = generate_random_server_id()
        self.SOURCE_MYSQL_ONLY_SCHEMAS = config_data["source_database"]["schemas"]

        # Logging database connection
        # If the DPU is running inside a docker container, there is no need to change the following log database connections.
        self.LOG_MARIADB_SETTINGS = {
            "host": "db",
            "port": 3306,
            "user": "root",
            "passwd": LOG_DB_PASSWORD,
        }
        # Connect to the log database
        self.log_db = LogDBConnection(
            host=self.LOG_MARIADB_SETTINGS["host"],
            port=self.LOG_MARIADB_SETTINGS["port"],
            user=self.LOG_MARIADB_SETTINGS["user"],
            password=self.LOG_MARIADB_SETTINGS["passwd"],
        )

        print(f"{datetime.datetime.now()} | CDC log database connection successful")

        # Initialise binlog filename
        self.bin_log_file = None

    def start(self):
        """
        Initiates the binlog capture process. Determines the starting binlog file and position either from the configuration file or the log database.
        Begins processing binlog events from the determined position.
        """

        # Query the binlog file and its location
        config_binlog_file = config_data.get("binlog_file")
        config_binlog_pos = config_data.get("binlog_pos")

        if config_binlog_file is None or config_binlog_pos is None:

            # If the binlog file and location are not specified, it is retrieved from the log database
            query_result = self.log_db.cdc_max_log_pos_query()

            if query_result is None:
                _log_file, _log_pos = None, None
            else:
                _log_file, _log_pos = query_result

            if _log_file is not None and _log_pos is not None:
                print(f"{datetime.datetime.now()} | CDC log database query to latest location [{_log_file}:{_log_pos}], will resume processing")
            else:
                print(f"{datetime.datetime.now()} | CDC log database was not queried for the latest position, it will be retrieved from the source database.")
        else:
            _log_file, _log_pos = config_binlog_file, config_binlog_pos
            print(f"{datetime.datetime.now()} | config.yml configuration file query to latest location [{_log_file}:{_log_pos}], will resume processing")

        try:
            # Try a binlog connection
            binlog_stream = self.binlog_connection(_log_file, _log_pos)
            self.binlog_processor(binlog_stream)
        except Exception as e:
            self.error_logger.critical(e)
            # Reconnect
            self.start()

    def binlog_connection(self, log_file: str = None, log_pos: int = None):
        """
        Establishes a connection to the source database's binlog stream with the specified binlog file and position.

        Args:
            log_file (str, optional): The binlog file name to start reading from. Defaults to None.
            log_pos (int, optional): The position within the binlog file to start reading from. Defaults to None.

        Returns:
            BinLogStreamReader: A stream reader connected to the source database's binlog.
        """

        # Instantiating a BinLog Stream Read
        stream = BinLogStreamReader(
            connection_settings=self.SOURCE_MYSQL_SETTINGS,
            server_id=self.SOURCE_MYSQL_SERVER_ID,  # Setting the server_id
            blocking=True,
            only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent, RotateEvent],
            only_schemas=[self.SOURCE_MYSQL_ONLY_SCHEMAS],
            resume_stream=True,
            enable_logging=False,
            slave_heartbeat=10,
            log_file=log_file,
            log_pos=log_pos,
        )

        print(f"{datetime.datetime.now()} | Source database connection successful, server_id: [{self.SOURCE_MYSQL_SERVER_ID}]")

        return stream

    def binlog_processor(self, stream):
        """
        Processes events from the binlog stream, logs them, and queues them for further processing.

        Args:
            stream (BinLogStreamReader): The binlog stream reader providing events to process.
        """

        print(f"{datetime.datetime.now()} | Source database Binlog stream read in progress...")

        for binlog_event in stream:
            # Binlog rotation event detection
            if isinstance(binlog_event, RotateEvent):
                # Update binlog filename
                self.bin_log_file = binlog_event.next_binlog
                continue

            # Get binlog location
            bin_log_pos = binlog_event.packet.log_pos
            print(f"{datetime.datetime.now()} | Receiving {self.bin_log_file}:{bin_log_pos}", flush=True)
            for row in binlog_event.rows:
                # Constructing an initial dictionary of events
                event = {
                    "cdc_id": None,
                    "cdc_dt": datetime.datetime.now(),
                    "log_file": self.bin_log_file,
                    "log_pos": bin_log_pos,
                    "log_dt": strftime(
                        "%Y-%m-%d %H:%M:%S", localtime(binlog_event.timestamp)
                    ),
                    "schema": binlog_event.schema,
                    "table": binlog_event.table,
                }

                # Action judgement

                # DELETE action
                if isinstance(binlog_event, DeleteRowsEvent):
                    event["action"] = "delete"
                    event["data"] = row["values"]

                # UPDATE action
                elif isinstance(binlog_event, UpdateRowsEvent):
                    event["action"] = "update"
                    data = {
                        "before_values": row["before_values"],
                        "after_values": row["after_values"],
                    }
                    event["data"] = data

                # INSERT action
                elif isinstance(binlog_event, WriteRowsEvent):
                    event["action"] = "insert"
                    event["data"] = row["values"]

                # Field Name Mapping
                event_mapping = mapping_data(binlog_event.table, event["action"], event)

                # Logging to cdc log database
                cdc_id = self.log_db.cdc_processed_execute_insert(event_mapping)
                event_mapping["cdc_id"] = cdc_id

                self.cdc_logger.info(event_mapping)

                # Add to queue
                PersistQueue.put(event_mapping)

            print(f"{datetime.datetime.now()} | Receiving Completion {self.bin_log_file}:{bin_log_pos}", flush=True)


if __name__ == "__main__":
    pass
