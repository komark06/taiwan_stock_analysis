from multiprocessing import Process, Queue

from twisted.internet import reactor
from twisted.web.resource import Resource
from twisted.web.server import Site
from twisted.web.static import File

from tests import sample_data_dir, sample_parm


class Root(Resource):
    def getChild(self, name, request):
        return self

    def render(self, request):
        return b"Root page.\n"


def run_server(queue: Queue):
    root = Root()
    root.putChild(sample_parm.encode(), File(sample_data_dir))
    factory = Site(root)
    httpHost = reactor.listenTCP(0, factory)
    queue.put(httpHost.getHost().port)
    reactor.run()


class MockServer:
    def __enter__(self):
        queue = Queue()
        self.proc = Process(target=run_server, args=(queue,))
        self.proc.start()
        http_port = queue.get()
        self.http_address = f"http://127.0.0.1:{http_port}"
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.proc.kill()

    def url(self, path, is_secure=False):
        host = self.http_address
        return host + path
