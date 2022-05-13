# Copyright (c) 2022 Filippo Argiolas <filippo.argiolas@ca.infn.it>.
#
# a simple script to download timeseries data from ThingsBoard
# barely tested, poor error checking, ugly code, use at your own risk
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

import sys
import requests
import datetime as dt
import pandas as pd

from rich.progress import Progress
from .util import console, info, warning, print_exception


class TBDownload(object):
    """Minimal client to download timeseries data from a ThingsBoard instance.

    A few Python bindings for TB REST APIs, just the few needed to
    enumerate assets and devices and download timeseries data. Not
    meant for anything serious and never really tested outside our
    specific ThingsBoard instance.

    ThingsBoard has different APIs for different kind of users
    (Tenant Admins and Customers) even when they do basically the same
    things (e.g. enumerate devices the user can access). Then there's
    a special user for the Public dashboards, it's a Customer without
    credentials, but not all customer entry-points work... Here we try
    to abstract all this away, at least for the few entry-points we
    use.

    :param url: address of the thingsboard instance
    :param public_id: CustomerId of the Public user, from the public dashboard url
    :param username: Customer or Tenant username
    :param password: Customer or Tenant password

    Note: if both public_id and credentials are provided public_id is
    preferred and credentials are silently ignored.

    """

    def __init__(self, url, public_id=None, username=None, password=None):
        """Initialize class arguments."""
        self.url = url
        self.public_id = public_id
        self.username = username
        self.password = password

    # poor error handling but useful for debugging: raise fatal
    # exceptions for every error, including http ones
    def _get(self, *args, **kwargs):
        try:
            r = requests.get(*args, **kwargs)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException:
            print_exception()
            sys.exit(1)

    def _post(self, *args, **kwargs):
        try:
            r = requests.post(*args, **kwargs)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException:
            print_exception()
            sys.exit(1)

    def login(self):
        """Login with credentials provided at init."""
        if self.public_id:
            r = self._post(f'{self.url}/api/auth/login/public',
                           json={'publicId': self.public_id})
        else:
            r = self._post(f'{self.url}/api/auth/login',
                           json={'username': self.username,
                                 'password': self.password})

        self.token = r.json()['token']
        self.auth_headers = {'X-Authorization': f'Bearer {self.token}'}

        if self.public_id:
            self.user_authority = "CUSTOMER"
            self.user_id = self.public_id
        else:
            r = self._get(f'{self.url}/api/auth/user',
                          headers=self.auth_headers)
            self.user_authority = r.json()['authority']
            self.user_id = r.json()['customerId']['id']

    def get_assets(self, page_size=20, page=0):
        """Enumerate assets available for current user.

        :param page_size: assets per page
        :param page: current page
        """
        if self.user_authority == "TENANT_ADMIN":
            r = self._get(f'{self.url}/api/tenant/assets',
                          headers=self.auth_headers,
                          params={'pageSize': page_size, 'page': page})

        else:
            r = self._get(f'{self.url}/api/customer/{self.user_id}/assets',
                          headers=self.auth_headers,
                          params={'pageSize': page_size, 'page': page})

        return r.json()

    def get_asset_devices(self, asset):
        """Enumerate devices contained in selected asset.

        :param asset: asset as returned by `get_assets()`
        """
        entity_id = asset['id']['id']
        entity_type = asset['id']['entityType']

        r = self._get(f'{self.url}/api/relations',
                      headers=self.auth_headers,
                      params={'fromId': entity_id,
                              'fromType': entity_type,
                              'relationType': 'Contains'})
        relations = r.json()

        devs = []
        for rel in relations:
            to_id = rel['to']['id']
            r = self._get(f'{self.url}/api/device/{to_id}',
                          headers=self.auth_headers)

            devs.append(r.json())

        return devs

    def query_attributes(self, device, attributes=''):
        """Query attributes from a device."""
        entity_id = device['id']['id']
        entity_type = device['id']['entityType']
        r = self._get(f'{self.url}/api/plugins/telemetry/{entity_type}/{entity_id}/values/attributes',
                      params={'keys': ','.join(attributes)},
                      headers=self.auth_headers)
        return r.json()

    def get_devices(self, page_size=30, page=0, text_search=""):
        """Enumerate devices.

        Enumerate the devices current user can access.

        :params page_size: devices per page
        :params page: current page
        :text_search: filter devices by name
        """
        if self.user_authority == "TENANT_ADMIN":
            r = self._get(f'{self.url}/api/tenant/devices',
                          headers=self.auth_headers,
                          params={'pageSize': page_size, 'page': page,
                                  'textSearch': text_search})
        else:
            r = self._get(f'{self.url}/api/customer/{self.user_id}/devices',
                          headers=self.auth_headers,
                          params={'pageSize': page_size, 'page': page,
                                  'textSearch': text_search})
        return r.json()

    def get_timeseries(self, device, keys, start_ts, end_ts, limit=86400 * 10):
        """Retrieve time series for a device in the desired interval.

        :param device: desired device
        :param keys: data columns to download (empty string to get them all)
        :param start_ts: interval start timestamp (in seconds)
        :param end_ts: interval end timestamp (in seconds)
        :param limit: maximum number of rows to retrieve
        :return: a pandas dataframe
        """
        entity_id = device['id']['id']
        entity_type = device['id']['entityType']
        r = self._get(f'{self.url}/api/plugins/telemetry/{entity_type}/{entity_id}/values/timeseries',
                      headers=self.auth_headers,
                      params={'keys': ','.join(keys),
                              'limit': limit,
                              'startTs': int(start_ts * 1000),
                              'endTs': int(end_ts * 1000)})

        if not r.json():
            warning("no data for selected interval")
            return None

        dfs = []
        for k in r.json().keys():
            df = pd.DataFrame.from_dict(r.json()[k])
            df.set_index("ts", drop=True, inplace=True)
            df.columns = [k]
            dfs.append(df.sort_index())

        return pd.concat(dfs, join="outer", axis=1)

    def save_timeseries(self, device, start_date, end_date, filename="output.csv"):
        """Save timeseries data from selected interval in CSV file.

        Download timeseries data from selected interval one day at the
        time using `get_timeseries`, concat resulting dataframes and
        save everything to a CSV file.

        :param device: desired device
        :param start_date: interval start, python datetime object
        :param end_date: interval end, python datetime object
        :param filename: output filename

        """
        entity_id = device['id']['id']
        entity_type = device['id']['entityType']
        r = self._get(f'{self.url}/api/plugins/telemetry/{entity_type}/{entity_id}/keys/timeseries',
                      headers=self.auth_headers)

        keys = r.json()
        if not keys:
            warning("no telemetry found for selected device")
            return

        delta = dt.timedelta(hours=3)
        total_timespan = end_date - start_date
        orig_start_date = start_date

        info(f"saving to `{filename}`")

        # create the csv in write mode in the first iteration
        # append in the following ones
        # also only enable header in the first loop
        append = False

        with Progress(transient=True, console=console) as progress:
            task = progress.add_task("[red]Downloading...", total=total_timespan / delta)
            while start_date <= end_date:
                interval_start = start_date
                interval_end = interval_start + delta
                span = start_date - orig_start_date
                start_date = interval_end

                fmt = "%Y-%m-%d %H:%M:%S"
                progress.console.print(f'fetching data from {interval_start.strftime(fmt)} to {interval_end.strftime(fmt)} (days {span.days+1} of {total_timespan.days+1})')

                df = self.get_timeseries(device, keys, interval_start.timestamp(), interval_end.timestamp())
                if df is None:
                    continue

                df.to_csv(filename, mode="a" if append else "w", columns=keys, header=not append)

                append = True

                progress.update(task, advance=1)

        info('[bold green]download complete.[/bold green]')
