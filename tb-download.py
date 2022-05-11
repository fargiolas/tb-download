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
import logging
import pandas as pd
import argparse


logging.basicConfig()
logger = logging.getLogger("tb-download")
logger.setLevel(logging.DEBUG)

class TBDownload(object):
    def __init__(self, url, public_id=None, username=None, password=None):
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
            logger.exception('an exception occured during GET')
            sys.exit(1)

    def _post(self, *args, **kwargs):
        try:
            r = requests.post(*args, **kwargs)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException:
            logger.exception('an exception occured during POST')
            sys.exit(1)

    def login(self):
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

    def query_attributes(self, devices, query="", attributes=""):
        attrs = []
        try:
            dev = [d for d in devices if query in d['name']][0]

            entity_id = dev['id']['id']
            entity_type = dev['id']['entityType']
            r = self._get(f'{self.url}/api/plugins/telemetry/{entity_type}/{entity_id}/values/attributes',
                          params={'keys': attributes},
                          headers=self.auth_headers)
            return r.json()

        except IndexError:
            pass

    def get_devices(self, page_size=30, page=0, text_search=""):
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


    def save_timeseries(self, device, start_date, end_date, prefix=""):
        """Retrieve timeseries from ThingsBoard one day at the time and saves them to a CSV file."""
        entity_id = device['id']['id']
        entity_type = device['id']['entityType']
        r = self._get(f'{self.url}/api/plugins/telemetry/{entity_type}/{entity_id}/keys/timeseries',
                      headers=self.auth_headers)

        keys = r.json()

        delta = dt.timedelta(days=1)
        total_timespan = end_date - start_date
        orig_start_date = start_date

        fname = f"{prefix}-{entity_id}.csv"

        logger.info(f"saving to {fname}")

        # create the csv in write mode in the first iteration
        # append in the following ones
        # also only enable header in the first loop
        append = False

        while start_date <= end_date:
            interval_start = start_date
            interval_end = interval_start + delta
            span = start_date - orig_start_date
            start_date = interval_end

            logger.info(f'{device["name"]}: fetching day {span.days+1} of {total_timespan.days+1} (from {interval_start.isoformat()} to {interval_end.isoformat()})')

            df = self.get_timeseries(device, keys, interval_start.timestamp(), interval_end.timestamp())
            if df is None:
                continue

            df.to_csv(fname, mode="a" if append else "w", columns=keys, header=not append)

            append = True

    def get_timeseries(self, device, keys, start_ts, end_ts):
        """Get timeseries data from ThingsBoard using REST API and stores it into a Pandas dataframe."""
        entity_id = device['id']['id']
        entity_type = device['id']['entityType']
        r = self._get(f'{self.url}/api/plugins/telemetry/{entity_type}/{entity_id}/values/timeseries',
                      headers=self.auth_headers,
                      params={'keys': ','.join(keys),
                              'limit': 86400 * 3,
                              'startTs': int(start_ts * 1000),
                              'endTs': int(end_ts * 1000)})

        if not r.json():
            logger.warning("no data for selected interval")
            return None

        dfs = []
        for k in r.json().keys():
            df = pd.DataFrame.from_dict(r.json()[k])
            df.set_index("ts", drop=True, inplace=True)
            df.columns = [k]
            dfs.append(df.sort_index())

        return pd.concat(dfs, join="outer", axis=1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ThingsBoard timeseries downloader",
                                     formatter_class=lambda prog: argparse.ArgumentDefaultsHelpFormatter(prog, max_help_position=8, width=100))
    parser.add_argument("--url", type=str, default="", help="ThingsBoard URL")
    parser.add_argument("--public-id", type=str, help="ThingsBoard public dashboard ID, login as Public Customer")
    parser.add_argument("--username", type=str, help="ThingsBoard Tenant username")
    parser.add_argument("--password", type=str, help="ThingsBoard Tenant password")
    parser.add_argument("--start-date", type=str, default="2022-03-01T00:00:00+00:00", help="start date, in ISO format")
    parser.add_argument("--end-date", type=str, default="2022-03-07T23:59:59+00:00", help="end date, in ISO format")
    parser.add_argument("--query", type=str, default="", help="Device search query (e.g. gas), empty for all devices")
    parser.add_argument("--list-devices", action="store_true", help="List assets and devices")

    args = parser.parse_args()

    if not (args.public_id or args.username or args.password):
        logger.error("Please provide either a username and a password for Tenant login or a Public Dashboard ID for public login")
        logger.error("If both credentials types are provided, public login will be preferred")
        exit()

        start_ts = int(dt.datetime.fromisoformat(args.start_date).timestamp() * 1000)
    end_ts = int(dt.datetime.fromisoformat(args.end_date).timestamp() * 1000)

    start_date = dt.datetime.fromisoformat(args.start_date)
    end_date = dt.datetime.fromisoformat(args.end_date)

    logger.info(f'connecting to [link]{args.url}[/link]')
    client = TBDownload(args.url,
                        public_id=args.public_id,
                        username=args.username,
                        password=args.password)
    client.login()
    logger.info('login successful')

    if args.list_devices:
        assets = client.get_assets()
        for asset in assets['data']:
            if 'Main_' in asset['name']:
                continue

            logger.info("---")
            logger.info(f'asset: {asset["name"]}')

            devs = client.get_asset_devices(asset)
            logger.info("devices: {}".format(", ".join([d['name'] for d in devs])))

            attrs = client.query_attributes(devs, query="-gps",
                                            attributes="station_name,station_location,active,lastActivityTime")
            for attr in attrs:
                if "Time" in attr["key"]:
                    attr["value"] = dt.datetime.fromtimestamp(attr["value"] / 1000.)
                logger.info(f'{attr["key"]}: {attr["value"]}')

        exit()

    logger.info(f'querying devices matching search query: {args.query}')
    devs = client.get_devices(text_search=args.query)
    device_list = devs["data"]

    if len(device_list) > 0:
        logger.info("found {} devices matching the search query: {}".format(
            len(device_list), ", ".join([d["name"] for d in device_list])))
    else:
        logger.warning("no devices matching search query: {}".format(args.query))

    for dev in device_list:
        logger.info("---")
        logger.info(f'downloading data for device: {dev["name"]}, id: {dev["id"]["id"]}')
        client.save_timeseries(dev, start_date, end_date, dev["name"])
