def compute_hktech_signal(pct):
    if pct>1: return 1
    if pct<-1: return -1
    return 0
