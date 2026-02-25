import json
import os
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# V12 标准路径（静态数据）
TEMPLATE_PATH = os.path.join(ROOT, "data", "cn", "static", "sector", "sector_map.json")
SPOT_PATH = os.path.join(ROOT, "data", "cn", "spot")

OUTPUT_PATH = TEMPLATE_PATH  # 直接覆盖模板


def load_template():
    if not os.path.exists(TEMPLATE_PATH):
        raise FileNotFoundError(f"缺少模板文件: {TEMPLATE_PATH}")
    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("mapping", {})


def load_all_symbols():
    files = [f for f in os.listdir(SPOT_PATH) if f.endswith(".parquet")]
    if not files:
        raise FileNotFoundError(f"没有找到 parquet 文件: {SPOT_PATH}")

    files.sort()
    latest = files[-1]
    df = pd.read_parquet(os.path.join(SPOT_PATH, latest))

    symbols = []
    for code in df["代码"]:
        code = str(code)
        if code.startswith("6"):
            symbols.append(code + ".SH")
        else:
            symbols.append(code + ".SZ")
    return symbols


def autofill():
    mapping = load_template()
    symbols = load_all_symbols()

    missing = [s for s in symbols if s not in mapping]

    print("总股票数:", len(symbols))
    print("模板已有行业分类:", len(mapping))
    print("缺失行业分类:", len(missing))

    if missing:
        print("\n缺失行业分类 symbol（前 200 项）:")
        for s in missing[:200]:
            print("  ", s)

        print("\n请将这些 symbol 列表贴给我，我会为你按申万行业补齐。")

    # 保存
    print(OUTPUT_PATH)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump({"template_version": "SW1_v12", "mapping": mapping},
                  f, ensure_ascii=False, indent=2)

    print("\n>>> sector_map.json 已写入（仍需要补齐缺失行业）")


if __name__ == "__main__":
    autofill()
