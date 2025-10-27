from logging import getLogger
from requests import Session, Request
from datetime import datetime, timezone

from varken.structures import RadarrMovie, Queue
from varken.helpers import hashit, connection_handler


class RadarrAPI(object):
    def __init__(self, server, dbmanager):
        self.dbmanager = dbmanager
        self.server = server
        # Create session to reduce server web thread load, and globally define pageSize for all requests
        self.session = Session()
        self.session.headers = {'X-Api-Key': self.server.api_key}

        self.logger = getLogger()

    def __repr__(self):
        return f"<radarr-{self.server.id}>"

    def get_missing(self):
        """Collect missing monitored movies from Radarr"""
        endpoint = '/api/v3/movie'
        now = datetime.now(timezone.utc).astimezone().isoformat()
        influx_payload = []
        missing = []

        req = self.session.prepare_request(Request('GET', self.server.url + endpoint))
        get = connection_handler(self.session, req, self.server.verify_ssl)

        if not get:
            return

        try:
            movies = [RadarrMovie(**movie) for movie in get]
        except TypeError as e:
            self.logger.error('TypeError while creating RadarrMovie structure: %s', e)
            return

        for movie in movies:
            # Handle hasFile and isAvailable attributes for Radarr v5+
            has_file = getattr(movie, 'hasFile', getattr(movie, 'downloaded', False))
            is_available = getattr(movie, 'isAvailable', False)

            if movie.monitored and not has_file:
                ma = 0 if is_available else 1
                movie_name = f'{movie.title} ({movie.year})'
                missing.append((movie_name, ma, movie.tmdbId, movie.titleSlug))

        for title, ma, mid, title_slug in missing:
            hash_id = hashit(f'{self.server.id}{title}{mid}')
            influx_payload.append(
                {
                    "measurement": "Radarr",
                    "tags": {
                        "Missing": True,
                        "Missing_Available": ma,
                        "tmdbId": mid,
                        "server": self.server.id,
                        "name": title,
                        "titleSlug": title_slug
                    },
                    "time": now,
                    "fields": {
                        "hash": hash_id
                    }
                }
            )

        if influx_payload:
            self.dbmanager.write_points(influx_payload)

    def get_queue(self):
        """Collect queue information from Radarr"""
        endpoint = '/api/v3/queue'
        now = datetime.now(timezone.utc).astimezone().isoformat()
        influx_payload = []
        queue = []

        req = self.session.prepare_request(Request('GET', self.server.url + endpoint))
        get = connection_handler(self.session, req, self.server.verify_ssl)

        if not get:
            return

        for movie in get:
            try:
                if 'movie' in movie and isinstance(movie['movie'], dict):
                    movie['movie'] = RadarrMovie(**movie['movie'])
            except TypeError as e:
                self.logger.error('TypeError while creating RadarrMovie structure: %s', e)
                return

        try:
            download_queue = [Queue(**movie) for movie in get]
        except TypeError as e:
            self.logger.error('TypeError while creating Queue structure: %s', e)
            return

        for queue_item in download_queue:
            movie = queue_item.movie
            name = f'{movie.title} ({movie.year})'

            protocol = getattr(queue_item, 'protocol', 'UNKNOWN').upper()
            protocol_id = 1 if protocol == 'USENET' else 0

            # Handle quality field changes in Radarr v5
            quality_data = getattr(queue_item, 'quality', {})
            if isinstance(quality_data, dict):
                quality_name = (
                    quality_data.get('quality', {}).get('name')
                    or quality_data.get('name')
                    or 'Unknown'
                )
            else:
                quality_name = str(quality_data)

            queue.append(
                (name, quality_name, protocol, protocol_id, queue_item.id, movie.titleSlug)
            )

        for name, quality, protocol, protocol_id, qid, title_slug in queue:
            hash_id = hashit(f'{self.server.id}{name}{quality}')
            influx_payload.append(
                {
                    "measurement": "Radarr",
                    "tags": {
                        "type": "Queue",
                        "tmdbId": qid,
                        "server": self.server.id,
                        "name": name,
                        "quality": quality,
                        "protocol": protocol,
                        "protocol_id": protocol_id,
                        "titleSlug": title_slug
                    },
                    "time": now,
                    "fields": {
                        "hash": hash_id
                    }
                }
            )

        if influx_payload:
            self.dbmanager.write_points(influx_payload)
