from threader.threader import Threader

class Master(Threader):
    def __init__(self, port=8001) -> None:
        super().__init__(port)