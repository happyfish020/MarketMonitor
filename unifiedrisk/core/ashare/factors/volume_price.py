def compute_volume_price(turnover_today, turnover_yest):
    if turnover_today < turnover_yest*0.6: return -1
    return 0
