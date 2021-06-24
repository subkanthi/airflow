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

import os

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.sheets_to_gcs import GoogleSheetsToGCSOperator
from airflow.providers.google.suite.operators.calendar import GoogleCalendarInsertEventOperator
from airflow.providers.google.suite.operators.sheets import GoogleSheetsCreateSpreadsheetOperator
from airflow.providers.google.suite.transfers.gcs_to_sheets import GCSToGoogleSheetsOperator
from airflow.utils.dates import days_ago

GCS_BUCKET = os.environ.get("SHEETS_GCS_BUCKET", "test28397ye")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1234567890qwerty")
NEW_SPREADSHEET_ID = os.environ.get("NEW_SPREADSHEET_ID", "1234567890qwerty")

with models.DAG(
    "example_google_calendar",
    schedule_interval=None,  # Override to match your needs,
    start_date=days_ago(1),
    tags=["example"],
) as dag:

    # [START insert_event]
    insert_event = GoogleCalendarInsertEventOperator(
        task_id="create_spreadsheet"
        #,spreadsheet=SPREADSHEET
    )
    # [END insert_event]

insert_event

