from cloudify import logs
from cloudify.logs import CloudifyBaseLoggingHandler

__author__ = 'maxim'


class MockCloudifyBaseLoggingHandler(CloudifyBaseLoggingHandler):

    def emit(self, record):
        message = self.format(record)
        log = {
            'context': self.context,
            'logger': record.name,
            'level': record.levelname.lower(),
            'message': {
                'text': message
            }
        }
        logs.stdout_log_out(log)
