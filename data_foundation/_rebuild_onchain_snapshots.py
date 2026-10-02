"""只重建 onchain 快照的 L1/L2 (solana/dex/btc_blocks/btc_fees/oracle),
跳过 5500 万行 token_transfer 流式重建 (那个由 nightly rebuild 负责)。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from data_foundation.l1_onchain import (normalize_dex_volume,  # noqa: E402
                                        normalize_mempool_blocks,
                                        normalize_mempool_fees,
                                        normalize_oracle_snapshot,
                                        normalize_solana_snapshot,
                                        write_onchain_parquet)
from data_foundation.l2 import (certify_derivatives,  # noqa: E402
                                write_certified_derivatives)
import pandas as pd  # noqa: E402


def rebuild():
    items = [
        (normalize_solana_snapshot(), "solana_snapshot", "solana", "fetched_at",
         [], ["fetched_at"]),
        (normalize_dex_volume(), "dex_volume", "defillama", "date_utc",
         ["volume_usd"], ["dex_name", "date_utc"]),
        (normalize_mempool_blocks(), "btc_blocks", "mempool", "block_timestamp_utc",
         [], ["block_height"]),
        (normalize_mempool_fees(), "btc_fees", "mempool", "fetched_at",
         [], ["fetched_at"]),
        (normalize_oracle_snapshot(), "oracle_snapshot", "chainlink", "fetched_at",
         ["price"], ["pair", "fetched_at"]),
    ]
    for df, ds, venue, tc, core, keys in items:
        if df is None or df.empty:
            print(f"  {ds}: 空, 跳过")
            continue
        cdf = certify_derivatives(df, tc, core_numeric_cols=core, key_cols=keys)
        write_onchain_parquet(df, ds, venue, tc)
        write_certified_derivatives(cdf, ds, venue, "all", tc)
        mx = pd.to_datetime(cdf[tc].max(), utc=True)
        print(f"  {ds}: {len(cdf):,} 行, max={mx}, suspect={int(cdf['is_suspect'].sum())}")


if __name__ == "__main__":
    rebuild()
    print("done")
