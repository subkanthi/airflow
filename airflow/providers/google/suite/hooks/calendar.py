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
"""Hook for Google Drive service"""
from io import TextIOWrapper
from typing import Any, Optional, Sequence, Union

from googleapiclient.discovery import Resource, build
from googleapiclient.http import HttpRequest, MediaFileUpload

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GoogleCalendarHook(GoogleBaseHook):

    def __init__(self, api_version: str = "v3"):
        self.api_version = api_version

    def get_conn(self) -> Any:
        """
        Retrieves the connection to Google Calendar.

        :return: Google Calendar services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build("calendar", self.api_version, http=http_authorized, cache_discovery=False)
        return self._conn

    def get_calendar_entries(self, date, number_entries):
        """
        Retrieves the calendar entries from the date provided.

        :return; List of calendar entries
        """

    def create_event(self, data):
        """
        Creates a calendar event from the json.
        """
        event = self.get_conn().events().insert(calendarId='primary', body=data).execute()
        self.log.info("Created event %s", event["htmlLink"])
