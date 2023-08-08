{% docs eth_dex_lp_tokens %}

The address for the token included in the liquidity pool, as a JSON object. 

Query example to access the key:value pairing within the object:
SELECT
    DISTINCT pool_address AS unique_pools,
    tokens :token0 :: STRING AS token0,
    symbols: token0 :: STRING AS token0_symbol,
    decimals: token0 :: STRING AS token0_decimal
FROM bsc.defi.dim_dex_liquidity_pools
WHERE token0 = '0x4db5a66e937a9f4473fa95b1caf1d1e1d62e29ea'
;

{% enddocs %}