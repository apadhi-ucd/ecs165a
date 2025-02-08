
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < 64

    def write(self, value):
        if self.has_capacity():
            self.num_records += 1
        pass

