def compute_futures_signal(fut):
    if fut>0.5: return 1
    if fut<-0.5: return -1
    return 0
