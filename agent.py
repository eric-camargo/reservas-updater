from agents_abbreviations import ABBREVIATIONS


class Agent:

    def __init__(self, name):
        self.name = name
        self.abbreviation = self.get_abbreviation()

    def get_abbreviation(self):
        try:
            if ABBREVIATIONS[self.name]:
                return ABBREVIATIONS[self.name]
            else:
                return self.name
        except KeyError:
            return self.name