
class Page:

    PAGE_SIZE = 4096 * 16  # 64KB
    RECORD_SIZE = 8  # Each record is 8 bytes
    RECORDS_PER_PAGE = PAGE_SIZE // RECORD_SIZE

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(self.PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < self.RECORDS_PER_PAGE

    def write(self, value):
        if not self.has_capacity():
            return False

        offset = self.num_records * self.RECORD_SIZE
        value_bytes = value.to_bytes(self.RECORD_SIZE, byteorder='big', signed=True)
        self.data[offset:offset + self.RECORD_SIZE] = value_bytes
        self.num_records += 1
        return True

    def read(self, index):
        if index >= self.num_records:
            return None

        offset = index * self.RECORD_SIZE
        value_bytes = self.data[offset:offset + self.RECORD_SIZE]
        return int.from_bytes(value_bytes, byteorder='big', signed=True)
