__author__ = 'elip'


class HttpException(Exception):

    def __init__(self, url, code, message):
        self.url = url
        self.code = code
        self.message = message

    def __str__(self):
        return "{0} ({1}) : {2}".format(self.code, self.url, self.message)
