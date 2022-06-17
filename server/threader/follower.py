from server.threader.threader import Threader


class Follower(Threader):
    def __init__(self, port=8001) -> None:
        super().__init__(port)

    def map(self, key, value):
        pass

    def reduce(self, key, values_list):
        pass
