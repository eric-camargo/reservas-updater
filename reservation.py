class Reservation:

    def __init__(self, reservation_id, pdv, day, shift_time, agent, reservation_time, position):
        self.id = reservation_id
        self.pdv = pdv
        self.day = day
        self.shift_time = shift_time
        self.agent = agent
        self.reservation_time = reservation_time
        self.position = position
