import os
import sys
from subprocess import PIPE, Popen

from twisted.internet import reactor
from twisted.web.resource import Resource
from twisted.web.server import Site
from twisted.web.static import File

from tests import sample_data_dir, sample_parm
from tests.daily_data import daily_json


def mockserver_env() -> dict[str, str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = sys.executable
    return env


class DailyTrading(Resource):
    isLeaf = True

    def render(self, request):
        date = request.args[b"date"][0].decode()
        symbol = int(request.args[b"stockNo"][0])
        year = date[:4]
        month = date[4:6]
        day = date[-2:]
        content = daily_json(symbol, year, month, day)
        return content.encode()


class Root(Resource):
    def __init__(self):
        Resource.__init__(self)
        self.putChild(sample_parm.encode(), File(sample_data_dir))
        self.putChild(b"daily", DailyTrading())

    def getChild(self, name, request):
        return self

    def render(self, request):
        return b"Root page.\n"


class MockServer:
    def __enter__(self):
        self.proc = Popen(
            [sys.executable, "-u", "-m", "tests.mockserver", "-t", "http"],
            stdout=PIPE,
            env=mockserver_env(),
        )
        http_address = self.proc.stdout.readline().strip().decode("ascii")
        self.host = http_address.replace("0.0.0.0", "127.0.0.1")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.proc.kill()
        self.proc.communicate()

    def url(self, path):
        return self.host + path


if __name__ == "__main__":
    root = Root()
    factory = Site(root)
    httpPort = reactor.listenTCP(0, factory)

    def print_listening():
        httpHost = httpPort.getHost()
        httpAddress = f"http://{httpHost.host}:{httpHost.port}"
        print(httpAddress)

    reactor.callWhenRunning(print_listening)
    reactor.run()
