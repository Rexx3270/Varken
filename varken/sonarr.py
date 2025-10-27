from logging import getLogger
from requests import Session, Request
from datetime import datetime, timezone, date, timedelta

from varken.structures import Queue, SonarrTVShow
from varken.helpers import hashit, connection_handler


class SonarrAPI(object):
    def __init__(self, server, dbmanager):
        self.dbmanager = dbmanager
        self.server = server
        # Create session to reduce server web thread load, and globally define pageSize for all requests
        self.session = Session()
        self.session.headers = {'X-Api-Key': self.server.api_key}
        self.session.params = {'pageSize': 1000}
        self.logger = getLogger()

    def __repr__(self):
        return f"<sonarr-{self.server.id}>"

    def get_calendar(self, query="Missing"):
        endpoint = '/api/v3/calendar/'
        today = str(date.today())
        last_days = str(date.today() - timedelta(days=self.server.missing_days))
        future = str(date.today() + timedelta(days=self.server.future_days))
        now = datetime.now(timezone.utc).astimezone().isoformat()
        params = {'start': last_days if query == "Missing" else today,
                  'end': today if query == "Missing" else future}
        influx_payload = []
        air_days = []
        missing = []

        req = self.session.prepare_request(Request('GET', self.server.url + endpoint, params=params))
        get = connection_handler(self.session, req, self.server.verify_ssl)

        if not get:
            return

        tv_shows = []
        for show in get:
            try:
                tv_shows.append(SonarrTVShow(**show))
            except TypeError as e:
                self.logger.error('TypeError has occurred : while creating SonarrTVShow: %s - data: %s', e, show)

        for show in tv_shows:
            sxe = f'S{show.seasonNumber:0>2}E{show.episodeNumber:0>2}'
            downloaded = 1 if getattr(show, 'hasFile', False) else 0
            
            series_title = show.series.get('title', 'Unknown Series')
            episode_title = getattr(show, 'title', 'Unknown Episode')
            air_date_utc = getattr(show, 'airDateUtc', None)
            show_id = getattr(show, 'id', 0)
            
            if query == "Missing":
                if getattr(show, 'monitored', False) and not downloaded:
                    missing.append((series_title, downloaded, sxe, episode_title, air_date_utc, show_id))
            else:
                air_days.append((series_title, downloaded, sxe, episode_title, air_date_utc, show_id))
                
        for series_title, dl_status, sxe, episode_title, air_date_utc, sonarr_id in (air_days or missing):
            hash_id = hashit(f'{self.server.id}{series_title}{sxe}')
            influx_payload.append(
                {
                    "measurement": "Sonarr",
                    "tags": {
                        "type": query,
                        "sonarrId": sonarr_id,
                        "server": self.server.id,
                        "name": series_title,
                        "epname": episode_title,
                        "sxe": sxe,
                        "airsUTC": air_date_utc,
                        "downloaded": dl_status
                    },
                    "time": now,
                    "fields": {
                        "hash": hash_id
                    }
                }
            )

        self.dbmanager.write_points(influx_payload)
        if influx_payload:
            self.dbmanager.write_points(influx_payload)
        else:
            self.logger.debug("No Sonarr %s data to send to InfluxDB for server %s.", query, self.server.id)

    def get_queue(self):
        influx_payload = []
        endpoint = '/api/v3/queue'
        now = datetime.now(timezone.utc).astimezone().isoformat()
        queue = []

        req = self.session.prepare_request(Request('GET', self.server.url + endpoint))
        get = connection_handler(self.session, req, self.server.verify_ssl)

        if not get:
            return

        download_queue = []
        for show in get:
            try:
                download_queue.append(Queue(**show))
            except TypeError as e:
                self.logger.error('TypeError while creating Queue structure: %s - data: %s', e, show)
                
        if not download_queue:
            return

        for show in download_queue:
            try:
                episode = getattr(show, 'episode', {})
                sxe = f"S{episode.get('seasonNumber', 0):0>2}E{episode.get('episodeNumber', 0):0>2}"
                protocol = getattr(show, 'protocol', 'UNKNOWN').upper()
                protocol_id = 1 if protocol == 'USENET' else 0
                
                quality_data = getattr(show, 'quality', {})
                if isinstance(quality_data, dict):
                    quality_name = (
                        quality_data.get('quality', {}).get('name')
                        or quality_data.get('name')
                        or 'Unknown'
                    )
                else:
                    quality_name = str(quality_data)
                
                series_title = show.series.get('title', 'Unknown Series')
                episode_title = episode.get('title', 'Unknown Episode')
                
                queue.append((series_title, episode_title, protocol, protocol_id, sxe, show.id, quality_name))
                
        for series_title, episode_title, protocol, protocol_id, sxe, sonarr_id, quality in queue:
            hash_id = hashit(f'{self.server.id}{series_title}{sxe}')
            influx_payload.append(
                {
                    "measurement": "Sonarr",
                    "tags": {
                        "type": "Queue",
                        "sonarrId": sonarr_id,
                        "server": self.server.id,
                        "name": series_title,
                        "epname": episode_title,
                        "sxe": sxe,
                        "protocol": protocol,
                        "protocol_id": protocol_id,
                        "quality": quality
                    },
                    "time": now,
                    "fields": {
                        "hash": hash_id
                    }
                }
            )
            
        if influx_payload:
            self.dbmanager.write_points(influx_payload)
        else:
            self.logger.debug("No Sonarr queue data to send to InfluxDB for server %s.", self.server.id)
