def wait_for_input():
    input("{0:-^80}\n".format("HIT CR TO UNSUBSCRIBE AND DISCONNECT"))

epics = [
    # spreadbet Index epics
    "IX.D.FTSE.DAILY.IP",
    "IX.D.NASDAQ.CASH.IP",
    "IX.D.DAX.DAILY.IP",
    "IX.D.SPTRD.DAILY.IP",
    "IX.D.NIKKEI.DAILY.IP",
    "IX.D.HANGSENG.DAILY.IP",
    "IX.D.RUSSELL.DAILY.IP",
    "IX.D.STXE.CASH.IP",
    "IX.D.ASX.MONTH1.IP",
    # spreadbet FX epics
    "CS.D.GBPUSD.TODAY.IP",
    "CS.D.USDJPY.TODAY.IP",
    "CS.D.EURUSD.TODAY.IP",
    "CS.D.AUDUSD.TODAY.IP",
    "CS.D.EURJPY.TODAY.IP",
    "CS.D.EURGBP.TODAY.IP",
    "CS.D.USDCAD.TODAY.IP",
    "CS.D.USDCHF.TODAY.IP",
    "CS.D.EURCHF.TODAY.IP",
    "CS.D.CHFJPY.TODAY.IP",
    "CS.D.GBPJPY.TODAY.IP",
    "CS.D.CADJPY.TODAY.IP",
    "CS.D.GBPCAD.TODAY.IP",
]
