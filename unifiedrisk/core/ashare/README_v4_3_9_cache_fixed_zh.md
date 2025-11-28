
UnifiedRisk v4.3.9 - cache-fixed

本包仅包含两个“修复版”模块：

- unifiedrisk/common/cache_manager.py
- unifiedrisk/common/ak_cache.py

用途：
1. 修复你现有工程中缓存不生效的问题；
2. 把所有 akshare 接口统一改为“日级缓存”，缓存位置为：

    <项目根目录>/cache_ak/YYYYMMDD/*.json

使用方法：
1. 把本 zip 中的 unifiedrisk/common/*.py 覆盖到你本地工程的同名文件；
2. 保持你当前的 data_fetcher.py 不变（它已经正确调用了 ak_cache.xxx_cached）；
3. 再运行一次：

    python run_ashare_daily.py

4. 然后到你工程根目录下查看：

    cache_ak/2025xxxx/

   是否生成了：
    - spot_all.json
    - sse_deal_daily.json
    - szse_summary.json
    - index_daily_sh000001.json
    - index_daily_sz399001.json

只要看到这些文件，就说明 AkCache + CacheManager 已经正常工作。
