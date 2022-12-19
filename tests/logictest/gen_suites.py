#!/usr/bin/env python3
# -*- coding: UTF-8 -*-
# This is a generator of test cases
# Turn cases in directory ../suites/0_stateless/* into sqllogictest formation
import os
import re
import copy
import time

import mysql.connector
import fire

from log import log
from config import mysql_config, http_config
from logictest import is_empty_line
from http_connector import HttpConnector, format_result

suite_path = "../suites/0_stateless/"
logictest_path = "./suites/gen/"
skip_exist = True

database_regex = r"use|USE (?P<database>.*);"
error_regex = r"(?P<statement>.*)-- {ErrorCode (?P<expectError>.*)}"
query_statment_first_words = ["select", "show", "explain", "describe"]

exception_sqls = []
manual_cases = []

STATEMENT_OK = """statement ok
{statement}

"""

STATEMENT_ERROR = """statement error {error_id}
{statement}

"""

STATEMENT_QUERY = """query {query_options_with_labels}
{statement}

{results}
"""

# results_string looks like, result is seperate by space.
# 1 1 1
RESULTS_TEMPLATE = """{label_separate}
{results_string}"""


def first_word(text):
    return text.split()[0]


def get_error_statment(line):
    return re.match(error_regex, line, re.MULTILINE | re.IGNORECASE)


def get_database(line):
    return re.match(database_regex, line, re.MULTILINE | re.IGNORECASE)


# get_all_cases read all file from suites dir
# but only parse .sql file, .result file will be ignore, run sql and fetch results
# need a local running databend-meta and databend-query or change config.py to your cluster
def get_all_cases():
    # copy from databend-test
    def collect_subdirs_with_pattern(cur_dir_path, pattern):
        return list(
            # Make sure all sub-dir name starts with [0-9]+_*.
            filter(
                lambda fullpath: os.path.isdir(fullpath)
                and re.search(pattern, fullpath.split("/")[-1]),
                map(
                    lambda _dir: os.path.join(cur_dir_path, _dir),
                    os.listdir(cur_dir_path),
                ),
            )
        )

    def collect_files_with_pattern(cur_dir_path, patterns):
        return list(
            filter(
                lambda fullpath: os.path.isfile(fullpath)
                and os.path.splitext(fullpath)[1] in patterns.split("|"),
                map(
                    lambda _dir: os.path.join(cur_dir_path, _dir),
                    os.listdir(cur_dir_path),
                ),
            )
        )

    def get_all_tests_under_dir_recursive(suite_dir):
        all_tests = copy.deepcopy(collect_files_with_pattern(suite_dir, ".sql|.sh|.py"))
        # Collect files in depth 0 directory.
        sub_dir_paths = copy.deepcopy(
            collect_subdirs_with_pattern(suite_dir, "^[0-9]+")
        )
        # Recursively get files from sub-directories.
        while len(sub_dir_paths) > 0:
            cur_sub_dir_path = sub_dir_paths.pop(0)

            all_tests += copy.deepcopy(
                collect_files_with_pattern(cur_sub_dir_path, ".sql|.sh|.py")
            )

            sub_dir_paths += copy.deepcopy(
                collect_subdirs_with_pattern(cur_sub_dir_path, "^[0-9]+")
            )
        return all_tests

    return get_all_tests_under_dir_recursive(suite_path)


def parse_cases(sql_file):
    # New session every case file
    http_client = HttpConnector(**http_config)
    cnx = mysql.connector.connect(**mysql_config)
    mysql_client = cnx.cursor()

    def mysql_fetch_results(sql):
        ret = ""
        try:
            mysql_client.execute(sql)
            r = mysql_client.fetchall()

            for row in r:
                rowlist = []
                for item in row:
                    if item is None:
                        item = "NULL"
                    rowlist.append(str(item))
                row_string = " ".join(rowlist)
                if len(row_string) == 0:  # empty line replace with tab
                    row_string = "\t"
                ret = ret + row_string + "\n"
        except Exception as err:
            log.warning(
                f"SQL: {sql}  fetch no results, msg:{str(err)} ,check it manual."
            )
        return ret

    target_dir = os.path.dirname(str.replace(sql_file, suite_path, logictest_path))
    case_name = os.path.splitext(os.path.basename(sql_file))[0]
    target_file = os.path.join(target_dir, case_name)

    if skip_exist and os.path.exists(target_file):
        log.warning(f"skip case file {target_file}, already exist.")
        return

    log.info(f"Write test case to path: {target_dir}, case name is {case_name}")

    content_output = ""
    f = open(sql_file, encoding="UTF-8")
    sql_content = ""
    for line in f.readlines():
        if is_empty_line(line):
            continue

        if line.startswith("--"):  # pass comment
            continue

        # multi line sql
        sql_content = sql_content + line.rstrip()
        if ";" not in line:
            continue

        statement = sql_content.strip()
        sql_content = ""

        # error statement
        errorStatment = get_error_statment(statement)
        if errorStatment is not None:
            content_output = content_output + STATEMENT_ERROR.format(
                error_id=errorStatment.group("expectError"),
                statement=errorStatment.group("statement"),
            )
            continue

        if str.lower(first_word(statement)) in query_statment_first_words:
            # query statement

            try:
                http_results = format_result(http_client.fetch_all(statement))
                query_options = http_client.get_query_option()
            except Exception:
                exception_sqls.append(statement)
                log.error(f"Exception SQL: {statement}")
                continue

            if query_options == "" or is_empty_line(http_results):
                log.warning(
                    f"statement: {statement} type query could not get query_option change to ok statement"
                )
                content_output = content_output + STATEMENT_OK.format(
                    statement=statement
                )
                continue

            mysql_results = mysql_fetch_results(statement)
            query_options_with_labels = f"{query_options}"

            log.debug("sql: " + statement)
            log.debug("mysql return: " + mysql_results)
            log.debug("http  return: " + http_results)

            if http_results is not None and mysql_results != http_results:
                case_results = RESULTS_TEMPLATE.format(
                    results_string=mysql_results, label_separate="----"
                )

                case_results = (
                    case_results
                    + "\n"
                    + RESULTS_TEMPLATE.format(
                        results_string=http_results, label_separate="---- http"
                    )
                )

                query_options_with_labels = f"{query_options} label(http)"
            else:
                case_results = RESULTS_TEMPLATE.format(
                    results_string=mysql_results, label_separate="----"
                )

            content_output = content_output + STATEMENT_QUERY.format(
                query_options_with_labels=query_options_with_labels,
                statement=statement,
                results=case_results,
            )
        else:
            # ok statement
            try:
                # insert,drop,create does not need to execute by different handlers
                if str.lower(statement).split()[0] not in ["insert", "drop", "create"]:
                    http_client.query_with_session(statement)
                mysql_client.execute(statement)
            except Exception as err:
                log.warning(f"statement {statement} execute error,msg {str(err)}")
                pass

            content_output = content_output + STATEMENT_OK.format(statement=statement)

    f.close()
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    caseFile = open(target_file, "w", encoding="UTF-8")
    caseFile.write(content_output)
    caseFile.close()


def output():
    print("=================================")
    print("Exception sql using Http handler:")
    print("\n".join(exception_sqls))
    print("=================================")
    print("\n")
    print("=================================")
    print("Manual suites:")
    print("\n".join(manual_cases))
    print("=================================")


def main(pattern=".*"):
    log.debug(f"Case filter regex pattern is {pattern}")
    all_cases = get_all_cases()

    for file in all_cases:
        if not re.match(pattern, file):
            log.debug(f"Test file {file}  does not match pattern {pattern}")
            continue
        log.info(f"Test file {file} match pattern {pattern}")

        # .result will be ignore
        if ".result" in file or ".result_filter" in file:
            continue

        # .py .sh will be ignore, need log
        if ".py" in file or ".sh" in file:
            manual_cases.append(file)
            log.warning(f"test file {file} will be ignore")
            continue

        parse_cases(file)
        time.sleep(0.01)

    output()


if __name__ == "__main__":
    log.info(
        f"Start generate sqllogictest suites from path: {suite_path} to path: {logictest_path}"
    )
    fire.Fire(main)
