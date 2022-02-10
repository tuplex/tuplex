# Based on Tableau example
# altered to work with TPC-H data.
# this implements TPC-H Q19
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
    "--lineitem_path",
    type=str,
    dest="lineitem_path",
    default="data/lineitem.tbl",
    help="path to lineitem.tbl",
)
parser.add_argument(
    "--part_path",
    type=str,
    dest="part_path",
    default="data/part.tbl",
    help="path to part.tbl",
)
parser.add_argument(
    "--preprocessed",
    action="store_true",
    help="whether the input file is preprocessed (eg. pushdown applied and file is integers)",
)
parser.add_argument(
    "--weld-mode",
    dest="weld_mode",
    action="store_true",
    help="whether the input file is integers"
)
parser.add_argument(
    "--single-threaded",
    dest='single_threaded',
    action="store_true",
    help="whether to use a single thread for processing only",
)

args = parser.parse_args()
if args.weld_mode:
    assert args.preprocessed, 'weld mode requires preprocessed data'

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

# restrict to
#  "l_partkey",
#         "l_quantity",
#         "l_extendedprice",
#         "l_discount",
#         "l_shipinstruct",
#         "l_shipmode",
lineitem_table_preprocessed = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="Lineitem",
    columns=[
        TableDefinition.Column("l_partkey", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("l_quantity", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("l_extendedprice", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("l_discount", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("l_shipinstruct", SqlType.char(25), NOT_NULLABLE),
        TableDefinition.Column("l_shipmode", SqlType.char(10), NOT_NULLABLE),
    ]
)

lineitem_table_weld = TableDefinition(
    # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
    table_name="Lineitem",
    columns=[
        TableDefinition.Column("l_partkey", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("l_quantity", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("l_extendedprice", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("l_discount", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("l_shipinstruct", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("l_shipmode", SqlType.big_int(), NOT_NULLABLE),
    ]
)

# CREATE TABLE part
# (
#     p_partkey     BIGINT not null,
#     p_name        VARCHAR(55) not null,
#     p_mfgr        CHAR(25) not null,
#     p_brand       CHAR(10) not null,
#     p_type        VARCHAR(25) not null,
#     p_size        INTEGER not null,
#     p_container   CHAR(10) not null,
#     p_retailprice DOUBLE PRECISION not null,
#     p_comment     VARCHAR(23) not null
# );
part_table = TableDefinition(
    table_name="Part",
    columns=[
        TableDefinition.Column("p_partkey", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("p_name", SqlType.varchar(55), NOT_NULLABLE),
        TableDefinition.Column("p_mfgr", SqlType.char(25), NOT_NULLABLE),
        TableDefinition.Column("p_brand", SqlType.char(10), NOT_NULLABLE),
        TableDefinition.Column("p_type", SqlType.varchar(25), NOT_NULLABLE),
        TableDefinition.Column("p_size", SqlType.int(), NOT_NULLABLE),
        TableDefinition.Column("p_container", SqlType.char(10), NOT_NULLABLE),
        TableDefinition.Column("p_retailprice", SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column("p_comment", SqlType.varchar(23), NOT_NULLABLE)
    ]
)

# restrict to ["p_partkey", "p_brand", "p_size", "p_container"], all bigint
part_table_preprocessed = TableDefinition(
    table_name="Part",
    columns=[
        TableDefinition.Column("p_partkey", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("p_brand", SqlType.char(10), NOT_NULLABLE),
        TableDefinition.Column("p_size", SqlType.int(), NOT_NULLABLE),
        TableDefinition.Column("p_container", SqlType.char(10), NOT_NULLABLE)
    ]
)

part_table_weld = TableDefinition(
    table_name="Part",
    columns=[
        TableDefinition.Column("p_partkey", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("p_brand", SqlType.big_int(), NOT_NULLABLE),
        TableDefinition.Column("p_size", SqlType.int(), NOT_NULLABLE),
        TableDefinition.Column("p_container", SqlType.big_int(), NOT_NULLABLE)
    ]
)

def run_create_hyper_file_from_csv():
    """
    An example demonstrating loading data from a csv into a new Hyper file
    """
    if args.preprocessed:
        print('running on {} + {} columns'.format(5, 4))
    else:
        print('running on {} + {} columns'.format(16, 9))

    load_time = -1
    query_time = -1
    tstart = time.time()
    path_to_database = Path("tpchq19.hyper")

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

            lineitem_table_name = ''
            part_table_name = ''
            if args.preprocessed:
                connection.catalog.create_table(table_definition=lineitem_table_preprocessed)
                lineitem_table_name = lineitem_table_preprocessed.table_name
                connection.catalog.create_table(table_definition=part_table_preprocessed)
                part_table_name = part_table_preprocessed.table_name
            else:
                connection.catalog.create_table(table_definition=lineitem_table)
                lineitem_table_name = lineitem_table.table_name
                connection.catalog.create_table(table_definition=part_table)
                part_table_name = part_table.table_name

            # Using path to current file, create a path that locates CSV file packaged with these examples.
            lineitem_csv_path = args.lineitem_path
            part_csv_path = args.part_path

            # Load all rows into "Lineitem" table from the CSV file.
            # `execute_command` executes a SQL statement and returns the impacted row count.
            count_in_lineitem_table = connection.execute_command(
                command=f"COPY {lineitem_table_name} from {escape_string_literal(lineitem_csv_path)} with "
                f"(format csv, NULL 'NULL', delimiter '|')")
            count_in_part_table = connection.execute_command(
                command=f"COPY {part_table_name} from {escape_string_literal(part_csv_path)} with "
                        f"(format csv, NULL 'NULL', delimiter '|')")

            print(f"The number of rows in table {lineitem_table.table_name} is {count_in_lineitem_table}.")
            print(f"The number of rows in table {part_table.table_name} is {count_in_part_table}.")
            load_time = time.time() - tstart
            print('Loading CSV to Hyper took {}s'.format(load_time))
            tstart = time.time()
            # issue query
            # here, TPC-H Q19
            # select
            #   sum(l_extendedprice * (1 - l_discount)) as revenue
            # from
            #   lineitem,
            #   part
            # where
            #   p_partkey = l_partkey
            #   and (
            #     (
            #       p_brand = 'Brand#12'
            #       and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            #       and l_quantity >= 1 and l_quantity <= 11
            #       and p_size between 1 and 5
            #       and l_shipmode in ('AIR', 'AIR REG')
            #       and l_shipinstruct = 'DELIVER IN PERSON'
            #     )
            #     or
            #     (
            #       p_brand = 'Brand#23'
            #       and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            #       and l_quantity >= 10 and l_quantity <= 20
            #       and p_size between 1 and 10
            #       and l_shipmode in ('AIR', 'AIR REG')
            #       and l_shipinstruct = 'DELIVER IN PERSON'
            #     )
            #     or
            #     (
            #       p_brand = 'Brand#34'
            #       and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            #       and l_quantity >= 20 and l_quantity <= 30
            #       and p_size between 1 and 15
            #       and l_shipmode in ('AIR', 'AIR REG')
            #       and l_shipinstruct = 'DELIVER IN PERSON'
            #     )
            #   )

            q = ''
            if args.weld_mode:
                q = f"""select
                  sum(l_extendedprice * (1 - l_discount)) as revenue
                from
                  {lineitem_table_name},
                  {part_table_name}
                where
                  p_partkey = l_partkey
                  and (
                    (
                      p_brand = 12
                      and p_container in (18, 31, 25, 4)
                      and l_quantity >= 1 and l_quantity <= 1 + 10
                      and p_size between 1 and 5
                      and l_shipmode in (3, 7)
                      and l_shipinstruct = 0
                    )
                    or
                    (
                      p_brand = 23
                      and p_container in (5, 38, 19, 13)
                      and l_quantity >= 10 and l_quantity <= 10 + 10
                      and p_size between 1 and 10
                      and l_shipmode in (3, 7)
                      and l_shipinstruct = 0
                    )
                    or
                    (
                      p_brand = 34
                      and p_container in (1, 14, 29, 21)
                      and l_quantity >= 20 and l_quantity <= 20 + 10
                      and p_size between 1 and 15
                      and l_shipmode in (3, 7)
                      and l_shipinstruct = 0
                    )
                  )"""
            else:
                q = f"""select
  sum(l_extendedprice * (1 - l_discount)) as revenue
from
  {lineitem_table_name},
  {part_table_name}
where
  p_partkey = l_partkey
  and (
    (
      p_brand = 'Brand#12'
      and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      and l_quantity >= 1 and l_quantity <= 11
      and p_size between 1 and 5
      and l_shipmode in ('AIR', 'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
      p_brand = 'Brand#23'
      and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      and l_quantity >= 10 and l_quantity <= 20
      and p_size between 1 and 10
      and l_shipmode in ('AIR', 'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
      p_brand = 'Brand#34'
      and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      and l_quantity >= 20 and l_quantity <= 30
      and p_size between 1 and 15
      and l_shipmode in ('AIR', 'AIR REG')
      and l_shipinstruct = 'DELIVER IN PERSON'
    )
  )"""

            result = connection.execute_list_query(query=q)
            query_time = time.time() - tstart
            print('Query took {}s'.format(query_time))
            print('Result::')
            print(result)

        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")
    print('framework,version,load,query,result\n{},{},{},{},{}'.format('hyper',hyperversion,load_time, query_time, str(result[0][0])))

if __name__ == '__main__':
    try:
        run_create_hyper_file_from_csv()
    except HyperException as ex:
        print(ex)
        exit(1)
