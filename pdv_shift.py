class PdvShift:

    def __init__(self, pdv, day, time):
        self.pdv = pdv
        self.day = day
        self.time = time
        self.shifts = []

    def add_shift(self, shift):
        self.shifts.append(shift)