def compute_a50_signal(a50):
    if a50>0.5: return 1
    if a50<-0.5: return -1
    return 0
