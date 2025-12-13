"""
Sector Map Builder - 申万一级行业 (AkShare 1.17.90)
--------------------------------------------------
一次性生成 sector_map.json:
    data/cn/history/sector/sector_map.json
"""

import os
import json
import akshare as ak


def normalize_symbol(code: str) -> str:
    """
    将 600000 → 600000.SH
       000001 → 000001.SZ
    """
    code = code.strip()
    if code.startswith("6"):
        return code + ".SH"
    elif code.startswith(("0", "3")):
        return code + ".SZ"
    return code


def build_sector_map() -> dict:
    print(">>> 获取申万行业列表 (sw_index_info) ...")
    df_industry = ak.sw_index_info()

    sector_map = {}

    for _, row in df_industry.iterrows():
        index_code = str(row["指数代码"])
        industry_name = row["指数名称"]

        print(f">>> 获取行业成分股: {index_code} {industry_name}")

        try:
            df_cons = ak.sw_index_cons(index_code=index_code)
        except Exception as e:
            print(f"[WARN] 获取行业 {industry_name} 失败: {e}")
            continue

        # df_cons["股票代码"] 示例：600000.SH 或 000001.SZ
        for code in df_cons["股票代码"]:
            symbol = normalize_symbol(code.replace(".SH", "").replace(".SZ", ""))
            sector_map[symbol] = industry_name

    print(f">>> 共收集 {len(sector_map)} 条行业映射")
    return sector_map


def save_sector_map(sector_map: dict):
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    target = os.path.join(root, "data", "cn", "history", "sector")
    os.makedirs(target, exist_ok=True)

    path = os.path.join(target, "sector_map.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sector_map, f, ensure_ascii=False, indent=2)

    print(f">>> 保存成功: {path}")


if __name__ == "__main__":
    sector_map = build_sector_map()
    save_sector_map(sector_map)
    print(">>> Sector Map 生成完毕")
