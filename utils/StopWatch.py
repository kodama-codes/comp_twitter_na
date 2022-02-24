import time


class StopWatch:
    """
    Stop Watch helper class to track computation time
    """

    def __init__(self):
        pass

    def start(self):
        self.start_time = time.time()

    def get_time(self):
        return round(float(time.time() - self.start_time), 5)

    def reset(self):
        self.start_time = time.time()
