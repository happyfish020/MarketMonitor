def compute_style_switch(growth, value):
    diff = growth - value
    if diff > 1: return 2
    if diff < -1: return -2
    return 0
