from abc import ABC

import logictest
import http_connector
from mysql.connector.errors import Error


class TestHttp(logictest.SuiteRunner, ABC):
    def __init__(self, kind, args):
        super().__init__(kind, args)
        self._http = None

    def get_connection(self):
        if self._http is None:
            self._http = http_connector.HttpConnector(**self.driver)
        return self._http

    def reset_connection(self):
        if self._http is not None:
            self._http.reset_session()

    def batch_execute(self, statement_list):
        for statement in statement_list:
            self.execute_statement(statement)
        self.reset_connection()

    def execute_ok(self, statement):
        try:
            self.get_connection().query_with_session(statement)
        except Error as e:
            return e

    def execute_error(self, statement):
        try:
            self.get_connection().query_with_session(statement)
        except Error as e:
            return e

    def execute_query(self, statement):
        results = self.get_connection().fetch_all(statement.text)
        # query_type = statement.s_type.query_type
        vals = []
        for (ri, row) in enumerate(results):
            for (i, v) in enumerate(row):
                if isinstance(v, type(None)):
                    vals.append("NULL")
                    continue

                # todo(youngsofun) : check the schema instead
                # if query_type[i] == 'I':
                #     if not isinstance(v, int):
                #         log.error(
                #             "Expected int, got type {} in query {} row {} col {} value {}"
                #             .format(type(v), statement.text, ri, i, v))
                # elif query_type[i] == 'F' or query_type[i] == 'R':
                #     if not isinstance(v, float):
                #         log.error(
                #             "Expected float, got type {} in query {} row {} col {} value {}"
                #             .format(type(v), statement.text, ri, i, v))
                # elif query_type[i] == 'T':
                #     # include data, timestamp, dict, list ...
                #     if not (isinstance(v, str) or isinstance(v, dict) or
                #             isinstance(v, list)):
                #         log.error(
                #             "Expected string, got type {} in query {} row {} col {} value {}"
                #             .format(type(v), statement.text, ri, i, v))
                # elif query_type[i] == 'B':
                #     if not isinstance(v, bool):
                #         log.error(
                #             "Expected bool, got type {} in query {} row {} col {} value {}"
                #             .format(type(v), statement.text, ri, i, v))
                # else:
                #     log.error(
                #         "Unknown type {} in query {} row {} col {} value {}".
                #         format(query_type[i], statement.text, ri, i, v))
                # if isinstance(v, bool):
                #     v = str(v).lower(
                #     )  # bool to string in python will be True/False
                vals.append(str(v))
        return vals
