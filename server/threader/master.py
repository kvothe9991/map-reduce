from server.threader.threader import Threader


def file_system():
    pass


class Master(Threader):
    def __init__(self, port=8001) -> None:
        super().__init__(port)

        self._followers = []
        self._filesystem = file_system()
