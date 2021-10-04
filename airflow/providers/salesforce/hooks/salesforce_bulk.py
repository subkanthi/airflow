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
    Creates new connection to Salesforce Bulk API
    """

    default_conn_name = "salesforce_default"

    def __init__(
        self,
        object_name,
        salesforce_conn_id: str = default_conn_name,
        session_id: Optional[str] = None,
        session: Optional[Session] = None,
    ) -> None:
        super().__init__(salesforce_conn_id, session_id, session)

    def get_conn(self) {
        super().get_conn()
    }
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

        # query_results = self.bulk.query_all(query, include_deleted=include_deleted, **query_params)

        sfbulk = SFBulkType(
            object_name="Contact",
            bulk_url=self.bulk.bulk_url,
            headers=self.bulk.headers,
            session=self.bulk.session,
        )

        results = sfbulk.query("select id from Contact")

        print(results)
        #
        # self.log.info("Querying for all objects")
        # query_params = query_params or {}
        # query_results = conn.query_all(query, include_deleted=include_deleted, **query_params)

        # self.log.info(
        #     "Received results: Total size: %s; Done: %s", query_results['totalSize'], query_results['done']
        # )
        #
        # return query_results

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
