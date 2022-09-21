
"""Implements wrappers for the Uvicorn worker and server"""

import sys

from gunicorn.arbiter import Arbiter
from uvicorn.server import Server
from uvicorn.workers import UvicornWorker

from rest_server.main import RestMain


class PricerDogServer(Server):

    """Implements a wrapper for the Uvicorn server"""

    def handle_exit(self, sig, frame):
        RestMain.finalize()
        super().handle_exit(sig, frame)


class PricerUvicornWorker(UvicornWorker):

    """Implements a wrapper for the Uvicorn worker"""

    async def _serve(self) -> None:
        self.config.app = self.wsgi
        server = PricerDogServer(config=self.config)

        await server.serve(sockets=self.sockets)
        if not server.started:
            sys.exit(Arbiter.WORKER_BOOT_ERROR)
