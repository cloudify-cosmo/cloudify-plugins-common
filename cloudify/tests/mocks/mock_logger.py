from cloudify import logs
from cloudify.logs import CloudifyBaseLoggingHandler

__author__ = 'maxim'


class MockCloudifyBaseLoggingHandler(CloudifyBaseLoggingHandler):

    def __init__(self, ctx, out_func, message_context_builder):
        super(MockCloudifyBaseLoggingHandler, self).__init__(ctx, logs.stdout_log_out, message_context_builder)
        print "aaaaaaaaaa"


