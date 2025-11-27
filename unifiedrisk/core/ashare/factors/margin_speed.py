def compute_margin_speed(today, yest):
    speed=(today-yest)/max(yest,1)*100
    if speed>5: return 1
    if speed<-5: return -1
    return 0
