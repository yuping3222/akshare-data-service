import os
import sys

# 添加 src 到路径
sys.path.append(os.path.join(os.getcwd(), "src"))

from akshare_data import get_daily, get_index_stocks
from akshare_data.core.config import set_config, ServiceConfig, CacheConfig

def verify():
    print("Testing akshare-data-service (Offline Mode with Mock Source)...")
    
    # 修改缓存路径为当前目录下的临时文件夹
    test_cache = os.path.join(os.getcwd(), "test_verify_cache")
    cfg = ServiceConfig(cache=CacheConfig(base_dir=test_cache))
    # 强制使用 mock 源
    cfg.source.stock_daily_sources = ["mock"]
    set_config(cfg)
    
    print(f"Cache directory: {test_cache}")

    # 1. 测试获取指数成分
    print("\nFetching index stocks (A-shares)...")
    stocks = get_index_stocks("A股")
    print(f"Total stocks fetched: {len(stocks)}")
    assert len(stocks) > 4000
    print("✅ Index stocks fetch successful")

    # 2. 测试获取日线并缓存
    symbol = "000001"
    start = "2024-01-01"
    end = "2024-01-10"
    
    print(f"\nFetching daily data for {symbol} ({start} to {end})...")
    df1 = get_daily(symbol, start, end)
    print(f"Dataframe size: {len(df1)}")
    assert not df1.empty
    print("✅ Daily data fetch successful")

    # 3. 验证本地文件是否存在
    parquet_path = os.path.join(test_cache, "daily", "stock_daily", f"{symbol}.parquet")
    print(f"Checking parquet file: {parquet_path}")
    assert os.path.exists(parquet_path)
    print("✅ Parquet file persistence successful")

    # 4. 第二次拉取 (应命中缓存)
    print("\nfetching again (should hit cache)...")
    df2 = get_daily(symbol, start, end)
    assert len(df1) == len(df2)
    print("✅ Cache-first retrieval successful")

    print("\nSummary: Basic E2E verification PASSED.")

if __name__ == "__main__":
    try:
        verify()
    except Exception as e:
        print(f"\n❌ Verification failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
