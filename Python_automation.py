#!/usr/bin/python
## Name: Python_automation.py
##
## Description:
## This script queries Oracle databases and reports raw results in CSV format.
## Author: Nagaraj
##

## Modules
import cx_Oracle
from datetime import datetime as dt
import time
from argparse import ArgumentParser
from argparse import RawTextHelpFormatter

## Script Information
NAME = 'Python automation for Oracle Database'
AUTHOR = 'Nagaraj'

##### GLOBALS #####

## REPORT INFORMATION
# Generic Query Report
LOG_TIME = dt.strftime(dt.now(), "-%m-%d-%Y--%H-%M-%S")
REPORT_PATH = "/home/nag/Scripts/Oracle/"
REPORT_GENERIC_QUERY_RAW_FILE = REPORT_PATH + "generic_query_" + LOG_TIME + ".csv"  # generic query results (raw)

# Script Activity Log
LOG_FILE = REPORT_PATH + "oracle_checks" + LOG_TIME + ".log"  # script log activity
LOG_FH = open(LOG_FILE, 'wb')

## DATABASE INFORMATION
DB1 = ["hostname/db1", "customer1"]
DB2 = ["hostname/db2", "customer2"]
DATABASES = (DB1, DB2)

## LOGIN INFORMATION
USERNAME = "userid"
PASSWORD = "p@$$w0rd"

## DATABASE QUERIES
# Generic Query
CHECKDAY = "10/21/2020"

GENERIC_QUERY = "select os_username os_user, username, userhost, extended_timestamp, action_name, to_char(timestamp, 'MM/DD/YYYY hh24:mi:ss') timestamp, \
                action_name, to_char(logoff_time,'MM/DD/YYYY hh24:mi:ss') logoff_time, decode(returncode,0,'SUCCESS','FAILED') login_status \
                from dba_audit_session where (to_date(timestamp) LIKE to_date('" + CHECKDAY + "','MM/DD/RRRR'))"

## QUERY RESULTS
# Raw Query Results
GENERIC_QUERY_RAW = {}  # GENERIC_QUERY: raw data (for debug)

##### FUNCTIONS #####


## Login to database
def dbLogin(username, password, database):
    """Login to database"""

    print "Logging into database"
    LOG_FH.write("\nLogging into database")

    conn_string = username + "/" + password + "@" + database
    conn = cx_Oracle.connect(conn_string)
    curs = conn.cursor()

    return conn, curs


## Logout from database
def dbLogout(conn):
    """Logout from database"""

    print "Logging out from database"
    LOG_FH.write("\nLogging out from database")
    conn.close()

    return


## Run database query and save results
def dbQuery(curs, query, database_name, customer_name):
    """Run database query and save results"""

    # Run query
    curs.execute(query)

    # Column header names
    # Add database name as first column
    headers = ["CUSTOMER", "DATABASE"]

    for columns in curs.description:
        print columns
        headers.append(str(columns[0]))

    # List of just query results
    query_results = []

    LOG_FH.write("Raw Query results\n")

    for row in curs:
        LOG_FH.write("\n")
        LOG_FH.write(str(row))
        #        print row
        query_results.append([customer_name, database_name] +
                             [str(i) for i in list(row)])

    return [headers], query_results


## Print and format report data
def reportPrinter(data_dict, REPORT_FILE):
    """Print and format report data"""

    # check if there is both headers and results data and if there's
    # just header data (all dictionaries will have this by default)
    # then there must be no results, so skip reporting
    sum_data_len = 0

    for key, value in data_dict.iteritems():
        if key == 'headers':
            continue
        sum_data_len = sum_data_len + len(value)

    if sum_data_len == 0:
        print "\tNo results to report"
        LOG_FH.write("\n\tNo results to report")
        return

    REPORT_FH = open(REPORT_FILE, 'wb')

    # print headers at the top
    for header_name in data_dict['headers']:
        header_name = tuple(header_name)
        REPORT_FH.write('"' + '","'.join(header_name) + '"\n')

    # write out results as double-quoted and comma-delimited
    for db_name, results in data_dict.iteritems():
        if db_name == 'headers':
            continue
        else:
            for result in results:
                result = tuple(result)
                REPORT_FH.write('"' + '","'.join(result) + '"\n')

    REPORT_FH.close()

    return


## Run database audit checks
def dbAudits(database):
    """Run database audit checks"""

    database_name = database[0].split('/')[1]
    customer_name = database[1]

    # database query
    conn, curs = dbLogin(USERNAME, PASSWORD, database[0])

    print "Querying", database_name
    LOG_FH.write("\nQuerying " + database_name)
    GENERIC_QUERY_RAW['headers'], GENERIC_QUERY_RAW[database_name] = dbQuery(
        curs, GENERIC_QUERY, database_name, customer_name)

    # logoff database
    dbLogout(conn)

    return


## Execute Oracle Database Checks
def main():
    """Execute Oracle Checks"""

    # Start script timer
    start = dt.now()
    LOG_FH.write("\n\nTime Start: " + str(start))

    # run database audits
    for database in DATABASES:
        print "\nCurrent Database: ", database
        LOG_FH.write("\n\nCurrent Database: " + str(database))
        dbAudits(database)

    # write out database query results and analyses
    print "\nWriting out raw query results"
    LOG_FH.write("\n\nWriting out raw query results")

    print "Generic Query"
    LOG_FH.write("\nGeneric Query")
    reportPrinter(GENERIC_QUERY_RAW, REPORT_GENERIC_QUERY_RAW_FILE)

    # End script timer
    end = dt.now()
    elapsed = end - start
    print "\nTimer: ", elapsed
    LOG_FH.write("\n\nTime End: " + str(end))
    LOG_FH.write("\nTime Elapsed: " + str(elapsed))

    # Close log file
    LOG_FH.close()

    return


## Run script
if __name__ == '__main__':
    try:
        main()
    except (IOError, KeyError, AttributeError, ValueError, NameError,
            TypeError, LookupError), message:
        error = '%s -- Got Exception: %s\n' % (dt.now(), message)
        print error
        LOG_FH.write("\n\n" + error)
