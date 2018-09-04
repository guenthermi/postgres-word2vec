class Tracker:

    def __init__(self, con):
        self.con = con

    def get_tracking(self):
        tracking = dict()
        for notice in self.con.notices:
            splits = notice.split()
            if splits[1] == 'TRACK':
                if splits[2] in tracking:
                    tracking[splits[2]].append(splits[3:])
                else:
                    tracking[splits[2]] = [splits[3:]]
        return tracking
    def clear_track(self):
        self.con.notices.clear()
