import requests
from datetime import datetime, timedelta
import json
import psycopg2
from airflow.providers.http.hooks.http import HttpHook




# How many seconds before expiry to proactively refresh the token.
TOKEN_REFRESH_MARGIN = 30


class TokenManager:
    def __init__(self):
        self.token = None
        self.expires_at = None

    def get_token(self):
        """Return a valid access token, refreshing automatically if needed."""
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
    
        """Fetch a new access token from the OpenSky authentication server."""

        hook = HttpHook(http_conn_id='opensky_auth')
        conn = hook.get_connection('opensky_auth')

        #conn.host=https://opensky-network.org
        #Формируется полный путь host из connection + путь
        TOKEN_URL=f"{conn.host}/auth/realms/opensky-network/protocol/openid-connect/token"

        #берем client_id из json extra, если нет, то из login
        #client_id = conn.extra_dejson.get('client_id', conn.login)

        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": conn.login,
                "client_secret": conn.password,
            },
        )
        r.raise_for_status()

        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN)
        return self.token

    def headers(self):
        """Return request headers with a valid Bearer token."""
        return {"Authorization": f"Bearer {self.get_token()}"}