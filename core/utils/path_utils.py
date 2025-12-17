import os


def ensure_parent_dir(path: str):
    """确保目录存在"""
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def ensure_dir(path: str):
    """确保目录存在"""
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)
