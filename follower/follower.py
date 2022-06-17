from threader.threader import Threader

class Follower(Threader):
    def __init__(self, port=8001) -> None:
        super().__init__(port)