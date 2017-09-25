import gevent
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import server_discoverer
import server_discoverer_worker
from tornado.options import define, options
define("port", default=8000, help="run on the given port", type=int)

import random


class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        worker = self.settings["worker"]
        ret = worker.discover_server.get_by_name_roundrobin("test")
        self.write(str(ret))


if __name__ == "__main__":
    tornado.options.options.logging = "debug"
    tornado.options.parse_command_line()

    worker = server_discoverer_worker.ServerDiscovererWorker()
    app = tornado.web.Application(handlers=[(r"/", IndexHandler)],
                                  worker=worker)
    http_server = tornado.httpserver.HTTPServer(app)
    port = random.randint(1000, 10000)
    print port
    http_server.listen(port)

    try:
        worker.register("test", port, 0, None)
        #tornado.ioloop.IOLoop.instance().run_sync()
        worker.work()

    except Exception, e:
        print e

    tornado.ioloop.IOLoop.instance().start()