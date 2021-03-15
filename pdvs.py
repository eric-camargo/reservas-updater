from pdv_types import PDV_TYPES


class Pdv:

    def __init__(self, name):
        self.name = name
        self.type = self.get_type()

    def get_type(self):
        try:
            pdv_type = PDV_TYPES[self.name]
        except KeyError:
            pdv_type = self.name
        return pdv_type
