# Based on Tableau example
# altered to work with TPC-H data.
from pathlib import Path
import time
import argparse

from tableauhyperapi import __version__ as hyperversion
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException


parser = argparse.ArgumentParser(description="TPC-H query using Hyper API")
parser.add_argument(
    "--path",
    type=str,
    dest="data_path",
    default="data/lineitem.tbl",
    help="path to lineitem.tbl",
)
parser.add_argument("--preprocessed", action="store_true",
                    help="whether the input file is quantity,extended_price,discount,shipdate with shipdate being an integer")

parser.add_argument(
    "--single-threaded",
    dest='single_threaded',
    action="store_true",
    help="whether to use a single thread for processing only",
)

args = parser.parse_args()

# 'l_orderkey', 'l_partkey', 'l_suppkey',\n",
#    "           'l_linenumber', 'l_quantity', 'l_extendedprice',\n",
#    "           'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',\n",
#    "          'l_shipdate', 'l_commitdate', 'l_receiptdate',\n",
#    "           'l_shipinstruct', 'l_shipmode', 'l_comment'
#
# from https://github.com/dimitri/tpch-citus/blob/master/schema/tpch-schema.sql
# CREATE TABLE lineitem
# (
#     l_orderkey    BIGINT not null,
#     l_partkey     BIGINT not null,
#     l_suppkey     BIGINT not null,
#     l_linenumber  BIGINT not null,
#     l_quantity    DOUBLE PRECISION not null,
#     l_extendedprice  DOUBLE PRECISION not null,
#     l_discount    DOUBLE PRECISION not null,
#     l_tax         DOUBLE PRECISION not null,
#     l_returnflag  CHAR(1) not null,
#     l_linestatus  CHAR(1) not null,
#     l_shipdate    DATE not null,
#     l_commitdate  DATE not null,
#     l_receiptdate DATE not null,
#     l_shipinstruct CHAR(25) not null,
#     l_shipmode     CHAR(10) not null,
#     l_comment      VARCHAR(44) not null
# );
lineitem_table = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="Lineitem",
    columns=[
        TableDefinition.Column("l_orderkey", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("l_partkey", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("l_suppkey", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("l_linenumber", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("l_quantity", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("l_extendedprice", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("l_discount", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("l_tax", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("l_returnflag", SqlType.char(1), NOT_NULLABLE),
        TableDefinition.Column("l_linestatus", SqlType.char(1), NOT_NULLABLE),
        TableDefinition.Column("l_shipdate", SqlType.date(), NOT_NULLABLE),
        TableDefinition.Column("l_commitdate", SqlType.date(), NOT_NULLABLE),
        TableDefinition.Column("l_receiptdate", SqlType.date(), NOT_NULLABLE),
        TableDefinition.Column("l_shipinstruct", SqlType.char(25), NOT_NULLABLE),
        TableDefinition.Column("l_shipmode", SqlType.char(10), NOT_NULLABLE),
        TableDefinition.Column("l_comment", SqlType.varchar(44), NOT_NULLABLE)
    ]
)

lineitem_table_preprocessed = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="Lineitem",
    columns=[
        TableDefinition.Column("l_quantity", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("l_extendedprice", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("l_discount", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("l_shipdate", SqlType.big_int(), NOT_NULLABLE),
    ]
)


def run_create_hyper_file_from_csv():
    """
    An example demonstrating loading data from a csv into a new Hyper file
    """
    if args.preprocessed:
        print('running on 4 columns')
    else:
        print('running on 16 columns')

    load_time = -1
    query_time = -1
    tstart = time.time()
    path_to_database = Path("lineitem.hyper")

    # Optional process parameters.
    # They are documented in the Tableau Hyper documentation, chapter "Process Settings"
    # (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/processsettings.html).
    process_parameters = {
        # Limits the number of Hyper event log files to two.
        #"log_file_max_count": "2",
        # Limits the size of Hyper event log files to 100 megabytes.
        #"log_file_size_limit": "100M"
        "soft_concurrent_query_thread_limit" : "16",
        "hard_concurrent_query_thread_limit" : "16",
        "memory_limit" : "100g"
    }

    # single threaded?
    if args.single_threaded:
        process_parameters["soft_concurrent_query_thread_limit"] = "1"
        process_parameters["hard_concurrent_query_thread_limit"] = "1"

    result = None

    # Starts the Hyper Process with telemetry enabled to send data to Tableau.
    # To opt out, simply set telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU.
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
                      parameters=process_parameters) as hyper:

        # Optional connection parameters.
        # They are documented in the Tableau Hyper documentation, chapter "Connection Settings"
        # (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/connectionsettings.html).
        connection_parameters = {"lc_time": "en_US"}

        # Creates new Hyper file "customer.hyper".
        # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE,
                        parameters=connection_parameters) as connection:

            table_name = ''
            if args.preprocessed:
                connection.catalog.create_table(table_definition=lineitem_table_preprocessed)
                table_name = lineitem_table_preprocessed.table_name
            else:
                connection.catalog.create_table(table_definition=lineitem_table)
                table_name = lineitem_table.table_name

            # Using path to current file, create a path that locates CSV file packaged with these examples.
            path_to_csv = args.data_path

            # Load all rows into "Lineitem" table from the CSV file.
            # `execute_command` executes a SQL statement and returns the impacted row count.
            count_in_lineitem_table = connection.execute_command(
                command=f"COPY {table_name} from {escape_string_literal(path_to_csv)} with "
                f"(format csv, NULL 'NULL', delimiter '|')")

            print(f"The number of rows in table {lineitem_table.table_name} is {count_in_lineitem_table}.")
            load_time = time.time() - tstart
            print('Loading CSV to Hyper took {}s'.format(load_time))
            tstart = time.time()
            # issue query
            # here, TPC-H Q6
            # SELECT
            #     sum(l_extendedprice * l_discount) as revenue
            # FROM
            #     lineitem
            # WHERE
            #     l_shipdate >= date '1994-01-01'
            #     AND l_shipdate < date '1994-01-01' + interval '1' year
            #     AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
            #     AND l_quantity < 24;

            q = ''
            if args.preprocessed:
                q = f"""SELECT
    sum(l_extendedprice * l_discount) as revenue
FROM
    {table_name}
WHERE
    l_shipdate >= 19940101
    AND l_shipdate < 19950101
    AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
    AND l_quantity < 24"""
            else:
                q = f"""SELECT
    sum(l_extendedprice * l_discount) as revenue
FROM
    {table_name}
WHERE
    l_shipdate >= date '1994-01-01'
    AND l_shipdate < date '1994-01-01' + interval '1' year
    AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
    AND l_quantity < 24"""

            result = connection.execute_list_query(query=q)
            query_time = time.time() - tstart
            print('Query took {}s'.format(query_time))
            print('Result::')
            print(result)
            
        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")
    print('framework,version,load,query,result\n{},{},{},{},{}'.format('hyper',hyperversion,load_time, query_time, str(result)))

if __name__ == '__main__':
    try:
        run_create_hyper_file_from_csv()
    except HyperException as ex:
        print(ex)
        exit(1)
