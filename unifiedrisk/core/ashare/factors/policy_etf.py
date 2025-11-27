def compute_policy_etf(flow300, flow1000):
    s=0
    if flow300>0: s+=1
    if flow1000>0: s+=1
    return s
