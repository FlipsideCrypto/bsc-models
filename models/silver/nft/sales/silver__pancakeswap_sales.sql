{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH trade_details AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_log,
        decoded_log :askPrice :: INT AS total_price_raw,
        decoded_log :netPrice :: INT AS net_price_raw,
        decoded_log :buyer :: STRING AS buyer_address,
        decoded_log :seller :: STRING AS seller_address,
        decoded_log :collection :: STRING AS nft_address,
        decoded_log :tokenId :: STRING AS tokenId,
        IFF(
            decoded_log :withBNB = TRUE,
            'BNB',
            '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
        ) AS currency_address,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = '0x17539cca21c7933df5c980172d22659b8c345c5a'
        AND block_timestamp >= '2021-09-30'
        AND event_name = 'Trade'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
collection_details_fill AS (
    SELECT
        block_number,
        block_timestamp AS collection_update_timestamp,
        event_index,
        tx_hash,
        nft_address,
        creator_fee,
        trading_fee,
        LEAD(collection_update_timestamp) over (
            PARTITION BY nft_address
            ORDER BY
                collection_update_timestamp ASC,
                event_index ASC
        ) AS next_collection_update_timestamp
    FROM
        {{ ref('silver__pancakeswap_collection_details') }}
),
base AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.tx_hash,
        t.event_index,
        t.event_name,
        t.contract_address AS platform_address,
        'pancakeswap' AS platform_name,
        'pancakeswap v1' AS platform_exchange_version,
        t.decoded_log,
        buyer_address,
        seller_address,
        t.nft_address,
        t.tokenId,
        NULL AS erc1155_value,
        currency_address,
        total_price_raw,
        net_price_raw,
        creator_fee,
        trading_fee,
        (
            total_price_raw * creator_fee
        ) :: INT AS creator_fee_raw,
        (
            total_price_raw * trading_fee
        ) :: INT AS platform_fee_raw,
        creator_fee_raw + platform_fee_raw AS total_fees_raw,
        total_price_raw - total_fees_raw - net_price_raw AS sanity_check,
        collection_update_timestamp,
        _log_id,
        _inserted_timestamp
    FROM
        trade_details t
        INNER JOIN collection_details_fill C
        ON t.nft_address = C.nft_address
        AND (
            (
                t.block_timestamp >= C.collection_update_timestamp
                AND t.block_timestamp < C.next_collection_update_timestamp
            )
            OR (
                t.block_timestamp >= C.collection_update_timestamp
                AND C.next_collection_update_timestamp IS NULL
            )
        )
),
tx_data AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp :: DATE >= '2021-09-30'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    'sale' AS event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    buyer_address,
    seller_address,
    nft_address,
    tokenId,
    erc1155_value,
    currency_address,
    total_price_raw,
    net_price_raw,
    creator_fee,
    trading_fee,
    creator_fee_raw,
    platform_fee_raw,
    total_fees_raw,
    collection_update_timestamp,
    b._log_id,
    b._inserted_timestamp,
    from_address AS origin_from_address,
    to_address AS origin_to_address,
    origin_function_signature,
    tx_fee,
    input_data,
    CONCAT(
        nft_address,
        '-',
        tokenId,
        '-',
        platform_exchange_version,
        '-',
        b._log_id
    ) AS nft_log_id
FROM
    base b
    INNER JOIN tx_data USING (tx_hash)
