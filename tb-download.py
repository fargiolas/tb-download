# Copyright (c) 2022 Filippo Argiolas <filippo.argiolas@ca.infn.it>.
#
# a simple script to download timeseries data from ThingsBoard
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

import argparse
import functools
import logging

from tb_rest_client.rest_client_ce import RestClientCE
from tb_rest_client.rest import ApiException

import datetime as dt
import pandas as pd

logging.basicConfig()
logger = logging.getLogger("tb-download")
logger.setLevel(logging.DEBUG)


def save_timeseries(client, device, start_date, end_date, prefix=""):
    """Retrieve timeseries from ThingsBoard one day at the time and saves them to a CSV file."""
    keys = rest_client.get_timeseries_keys_v1(dev.id.entity_type, dev.id)
    delta = dt.timedelta(days=1)
    total_timespan = end_date - start_date
    orig_start_date = start_date
    append = False
    fname = f"{prefix}-{device.id.id}.csv"

    logger.info(f"saving to {fname}")

    while start_date <= end_date:
        interval_start = start_date
        interval_end = interval_start + delta
        span = start_date - orig_start_date
        start_date = interval_end

        logger.info(f"{device.name}: fetching day {span.days+1} of {total_timespan.days+1} (from {interval_start.isoformat()} to {interval_end.isoformat()})")

        df = get_timeseries(client, device, keys, interval_start.timestamp(), interval_end.timestamp())
        if df is None:
            continue
        if not append:
            mode = "w"
            header = True
        else:
            mode = "a"
            header = False

        df.to_csv(fname, mode=mode, columns=keys, header=header)


def get_timeseries(client, device, keys, start_ts, end_ts):
    """Get timeseries data from ThingsBoard using REST API and stores it into a Pandas dataframe."""
    ts = client.get_timeseries(device.id.entity_type, device.id, ','.join(keys),
                               limit=86400 * 2,
                               start_ts=int(start_ts * 1000),
                               end_ts=int(end_ts * 1000))
    if (len(ts.keys()) == 0):
        logger.warning("no data for selected interval")
        return None

    dfs = []
    for k in keys:
        df = pd.DataFrame.from_dict(ts[k])
        df.set_index("ts", drop=True, inplace=True)
        df.columns = [k]
        dfs.append(df.sort_index())

    return pd.concat(dfs, join="outer", axis=1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="CA2020 ThingsBoard timeseries downloader",
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

    with RestClientCE(base_url=args.url) as rest_client:
        # TB has different API depending on what type of user you are (tenant, customer, other?)
        # define a couple of helper methods to abstract those differences away
        # assume we only have tenant users and a public customer, we'll get back to this if we device to enable other customers too
        if args.public_id:
            _login = functools.partial(rest_client.public_login, args.public_id)
            _get_assets = functools.partial(rest_client.get_customer_assets, args.public_id)
            _get_devices = functools.partial(rest_client.get_customer_devices, args.public_id)
        else:
            _login = functools.partial(rest_client.login, username=args.username, password=args.password)
            _get_assets = rest_client.get_tenant_assets
            _get_devices = rest_client.get_tenant_devices

        try:
            logger.info(f"logging in to: {args.url}")
            _login()

            if args.list_devices:
                logger.info("getting assets")
                assets = _get_assets('30', '0')

                for asset in assets.data:
                    if 'Main_' in asset.name:
                        continue

                    logger.info("---")
                    logger.info(f'asset: {asset.name}')

                    relations = rest_client.find_by_from(asset.id, asset.id.entity_type, "Contains")
                    devs = []
                    for rel in relations:
                        dev = rest_client.get_device_by_id(rel.to.id)
                        devs.append(dev.name)

                        # gps devices provide useful metadata in the attributes
                        if "gps" in dev.name:
                            attrs = rest_client.get_attributes(dev.id.entity_type, dev.id.id, "station_label,station_location,active,lastActivityTime")
                            for attr in attrs:
                                k, v = attr["key"], attr["value"]
                                if k == "lastActivityTime":
                                    v = dt.datetime.fromtimestamp(v / 1000.)
                                logger.info(f'{k}: {v}')

                    logger.info("devices: {}".format(", ".join(devs)))

                exit()

            logger.info(f'querying devices matching search query: {args.query}')
            devs = _get_devices('30', '0', text_search=args.query)
            device_list = devs.data

            if len(device_list) > 0:
                logger.info("found {} devices matching the search query: {}".format(
                    len(device_list),
                    ", ".join([d.name for d in device_list])))
            else:
                logger.warning("no devices matching search query: {}".format(args.query))

            for dev in device_list:
                logger.info("---")
                logger.info(f'downloading data for device: {dev.name}, id: {dev.id.id}')
                save_timeseries(rest_client, dev, start_date, end_date, dev.name)

        except ApiException as e:
            logger.exception(e)

        except KeyboardInterrupt:
            logger.info("Caught CTRL+C, quitting.")
            exit(1)
