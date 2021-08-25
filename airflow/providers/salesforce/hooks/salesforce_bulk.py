#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
This module contains a Salesforce Bulk API Hook which allows you to connect
to your Salesforce instance using Bulk API

.. note:: this hook also relies on the simple_salesforce package:
      https://github.com/simple-salesforce/simple-salesforce
"""
import logging
import time
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from requests import Session

from simple_salesforce.bulk import SFBulkType

from airflow.providers.salesforce.hooks.salesforce import SalesforceHook

log = logging.getLogger(__name__)


class SalesforceBulkHook(SalesforceHook):
    """
    Creates new connection to Salesforce Bulkd API and allows
        you to pull data out of SFDC and save it to a file.
    """

    default_conn_name = "salesforce_default"

    def __init__(
        self,
        salesforce_conn_id: str = default_conn_name,
        session_id: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> None:
        super().__init__(salesforce_conn_id, session_id, session)

    def make_query(
        self, query: str, include_deleted: bool = False, query_params: Optional[dict] = None
    ) -> dict:
        """
        Make a query to Salesforce.

        :param query: The query to make to Salesforce.
        :type query: str
        :param include_deleted: True if the query should include deleted records.
        :type include_deleted: bool
        :param query_params: Additional optional arguments
        :type query_params: dict
        :return: The query result.
        :rtype: dict
        """
        conn = self.get_conn()

        sfbulk = SFBulkType(object_name="Contact", bulk_url=self.bulk.bulk_url,
                            headers=self.bulk.headers, session=self.bulk.session)

        results = sfbulk.query("select id from Contact")

        self.log.info("Querying for all objects")
        query_params = query_params or {}
        query_results = conn.query_all(query, include_deleted=include_deleted, **query_params)

        self.log.info(
            "Received results: Total size: %s; Done: %s", query_results['totalSize'], query_results['done']
        )

        return query_results

    def get_object_from_salesforce(self, obj: str, fields: Iterable[str]) -> dict:
        """
        Get all instances of the `object` from Salesforce.
        For each model, only get the fields specified in fields.

        All we really do underneath the hood is run:
            SELECT <fields> FROM <obj>;

        :param obj: The object name to get from Salesforce.
        :type obj: str
        :param fields: The fields to get from the object.
        :type fields: iterable
        :return: all instances of the object from Salesforce.
        :rtype: dict
        """
        query = f"SELECT {','.join(fields)} FROM {obj}"

        self.log.info(
            "Making query to Salesforce: %s",
            query if len(query) < 30 else " ... ".join([query[:15], query[-15:]]),
        )

        return self.make_query(query)

    @classmethod
    def _to_timestamp(cls, column: pd.Series) -> pd.Series:
        """
        Convert a column of a dataframe to UNIX timestamps if applicable

        :param column: A Series object representing a column of a dataframe.
        :type column: pandas.Series
        :return: a new series that maintains the same index as the original
        :rtype: pandas.Series
        """
        # try and convert the column to datetimes
        # the column MUST have a four digit year somewhere in the string
        # there should be a better way to do this,
        # but just letting pandas try and convert every column without a format
        # caused it to convert floats as well
        # For example, a column of integers
        # between 0 and 10 are turned into timestamps
        # if the column cannot be converted,
        # just return the original column untouched
        try:
            column = pd.to_datetime(column)
        except ValueError:
            log.error("Could not convert field to timestamps: %s", column.name)
            return column

        # now convert the newly created datetimes into timestamps
        # we have to be careful here
        # because NaT cannot be converted to a timestamp
        # so we have to return NaN
        converted = []
        for value in column:
            try:
                converted.append(value.timestamp())
            except (ValueError, AttributeError):
                converted.append(pd.np.NaN)

        return pd.Series(converted, index=column.index)

    def write_object_to_file(
        self,
        query_results: List[dict],
        filename: str,
        fmt: str = "csv",
        coerce_to_timestamp: bool = False,
        record_time_added: bool = False,
    ) -> pd.DataFrame:
        """
        Write query results to file.

        Acceptable formats are:
            - csv:
                comma-separated-values file. This is the default format.
            - json:
                JSON array. Each element in the array is a different row.
            - ndjson:
                JSON array but each element is new-line delimited instead of comma delimited like in `json`

        This requires a significant amount of cleanup.
        Pandas doesn't handle output to CSV and json in a uniform way.
        This is especially painful for datetime types.
        Pandas wants to write them as strings in CSV, but as millisecond Unix timestamps.

        By default, this function will try and leave all values as they are represented in Salesforce.
        You use the `coerce_to_timestamp` flag to force all datetimes to become Unix timestamps (UTC).
        This is can be greatly beneficial as it will make all of your datetime fields look the same,
        and makes it easier to work with in other database environments

        :param query_results: the results from a SQL query
        :type query_results: list of dict
        :param filename: the name of the file where the data should be dumped to
        :type filename: str
        :param fmt: the format you want the output in. Default:  'csv'
        :type fmt: str
        :param coerce_to_timestamp: True if you want all datetime fields to be converted into Unix timestamps.
            False if you want them to be left in the same format as they were in Salesforce.
            Leaving the value as False will result in datetimes being strings. Default: False
        :type coerce_to_timestamp: bool
        :param record_time_added: True if you want to add a Unix timestamp field
            to the resulting data that marks when the data was fetched from Salesforce. Default: False
        :type record_time_added: bool
        :return: the dataframe that gets written to the file.
        :rtype: pandas.Dataframe
        """
        fmt = fmt.lower()
        if fmt not in ['csv', 'json', 'ndjson']:
            raise ValueError(f"Format value is not recognized: {fmt}")

        df = self.object_to_df(
            query_results=query_results,
            coerce_to_timestamp=coerce_to_timestamp,
            record_time_added=record_time_added,
        )

        # write the CSV or JSON file depending on the option
        # NOTE:
        #   datetimes here are an issue.
        #   There is no good way to manage the difference
        #   for to_json, the options are an epoch or a ISO string
        #   but for to_csv, it will be a string output by datetime
        #   For JSON we decided to output the epoch timestamp in seconds
        #   (as is fairly standard for JavaScript)
        #   And for csv, we do a string
        if fmt == "csv":
            # there are also a ton of newline objects that mess up our ability to write to csv
            # we remove these newlines so that the output is a valid CSV format
            self.log.info("Cleaning data and writing to CSV")
            possible_strings = df.columns[df.dtypes == "object"]
            df[possible_strings] = (
                df[possible_strings]
                .astype(str)
                .apply(lambda x: x.str.replace("\r\n", "").str.replace("\n", ""))
            )
            # write the dataframe
            df.to_csv(filename, index=False)
        elif fmt == "json":
            df.to_json(filename, "records", date_unit="s")
        elif fmt == "ndjson":
            df.to_json(filename, "records", lines=True, date_unit="s")

        return df
