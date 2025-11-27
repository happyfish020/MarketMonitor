def compute_us_after(change_pct):
    if change_pct>0.5: return 1
    if change_pct<-0.5: return -1
    return 0
