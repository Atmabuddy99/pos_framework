class Straddle :

    def __init__(self):
        self.status="combined" # or individual
        self.OpenTime = None
        self.strike = 0
        self.callOpenPrc = 0
        self.putOpenPrc = 0
        self.callStopLoss = 0
        self.putStopLoss = 0
        self.callStatus = 0
        self.putStatus = 0
        self.callCloseTime = None
        self.putCloseTime = None
        self.callClosePrc = 0
        self.putClosePrc = 0
        self.callM2M = 0
        self.putM2M = 0
        self.straddle=0
        self.straddle_sl=0