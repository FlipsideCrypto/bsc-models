{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH logs_raw AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_log,
        decoded_log :inventory :kind :: INT AS kind,
        decoded_log :inventory :status :: INT AS status,
        decoded_log :inventory :buyer :: STRING AS buyer_address,
        decoded_log :inventory :seller :: STRING AS seller_address,
        decoded_log :inventory :currency :: STRING AS currency_address,
        decoded_log :inventory :netPrice :: INT AS net_price_raw,
        decoded_log :inventory :price :: INT AS price_raw,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = '0x449d05c544601631785a7c062dcdff530330317e'
        AND block_timestamp :: DATE >= '2021-10-01'
        AND event_name = 'EvInventoryUpdate'

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
logs_raw_rn AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_tx_grouping
    FROM
        logs_raw
    WHERE
        status = 1
),
traces_raw AS (
    SELECT
        tx_hash,
        trace_index,
        from_address,
        to_address,
        input,
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_data,
        (
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) / 32
        ) :: INT AS intent_start_index,
        -- start of intent
        (
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) / 32
        ) :: INT AS detail_start_index,
        -- detail
        (
            utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            ) / 32
        ) :: INT AS sigintent_start_index,
        --sigIntent
        (
            utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            ) / 32
        ) :: INT AS sigdetail_start_index,
        --sigDetail
        '0x' || SUBSTR(
            segmented_data [intent_start_index],
            25
        ) :: STRING AS user_address,
        '0x' || SUBSTR(
            segmented_data [detail_start_index+1],
            25
        ) :: STRING AS signer_address,
        '0x' || SUBSTR(
            segmented_data [detail_start_index+6],
            25
        ) :: STRING AS caller_address,
        '0x' || SUBSTR(
            segmented_data [detail_start_index+7],
            25
        ) :: STRING AS currency_address,
        (
            utils.udf_hex_to_int(
                segmented_data [detail_start_index + 8] :: STRING
            )
        ) :: INT AS price_raw,
        (
            (
                utils.udf_hex_to_int(
                    segmented_data [detail_start_index + 9] :: STRING
                )
            ) / 1e6
        ) AS incentive_rate,
        (
            utils.udf_hex_to_int(
                segmented_data [detail_start_index + 11] :: STRING
            ) / 32
        ) :: INT AS bundle_index,
        (
            (
                utils.udf_hex_to_int(
                    segmented_data [detail_start_index + 14] :: STRING
                )
            ) / 1e6
        ) AS fee_rate,
        (
            (
                utils.udf_hex_to_int(
                    segmented_data [detail_start_index + 15] :: STRING
                )
            ) / 1e6
        ) AS royalty_rate,
        '0x' || SUBSTR(
            segmented_data [detail_start_index+17],
            25
        ) :: STRING AS fee_receiver_address,
        '0x' || SUBSTR(
            segmented_data [detail_start_index+18],
            25
        ) :: STRING AS royalty_receiver_address,
        (
            utils.udf_hex_to_int(
                segmented_data [detail_start_index + bundle_index] :: STRING
            )
        ) :: INT AS bundle_array_size
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        block_timestamp :: DATE >= '2021-10-01'
        AND to_address = '0x449d05c544601631785a7c062dcdff530330317e'
        AND LEFT(
            input,
            10
        ) = '0xba847759'
        AND trace_status = 'SUCCESS'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
traces_raw_rn AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping
    FROM
        traces_raw
),
flattened_details AS (
    SELECT
        tx_hash,
        intra_tx_grouping,
        INDEX,
        VALUE,
        TRUNC(
            (ROW_NUMBER() over (PARTITION BY tx_hash, intra_tx_grouping
            ORDER BY
                INDEX ASC) - 1) / 7
        ) AS trunc_grouping
    FROM
        traces_raw_rn,
        LATERAL FLATTEN (
            input => segmented_data
        )
    WHERE
        INDEX BETWEEN (
            detail_start_index + bundle_index + bundle_array_size + 1
        )
        AND (
            detail_start_index + bundle_index + bundle_array_size + (
                bundle_array_size * 7
            )
        )
),
agg_details AS (
    SELECT
        tx_hash,
        intra_tx_grouping,
        trunc_grouping,
        ARRAY_AGG(VALUE) within GROUP (
            ORDER BY
                INDEX ASC
        ) AS grouped_data
    FROM
        flattened_details
    GROUP BY
        ALL
),
base AS (
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        event_index,
        contract_address,
        intra_tx_grouping,
        trunc_grouping,
        user_address,
        signer_address,
        caller_address,
        fee_rate,
        royalty_rate,
        fee_receiver_address,
        royalty_receiver_address,
        bundle_array_size,
        '0x' || SUBSTR(
            grouped_data [0],
            25
        ) :: STRING AS nft_address,
        (
            utils.udf_hex_to_int(
                grouped_data [1] :: STRING
            )
        ) :: STRING AS tokenId,
        (
            utils.udf_hex_to_int(
                grouped_data [2] :: STRING
            )
        ) :: STRING AS erc1155_value_raw,
        kind,
        status,
        buyer_address,
        seller_address,
        IFF(
            l.currency_address = '0x0000000000000000000000000000000000000000',
            'BNB',
            l.currency_address
        ) AS currency_address,
        t.price_raw AS price_raw_traces,
        net_price_raw,
        l.price_raw AS price_raw_logs,
        (
            net_price_raw / bundle_array_size
        ) :: INT AS total_price_raw,
        (
            price_raw_logs * fee_rate / bundle_array_size
        ) :: INT AS platform_fee_raw,
        (
            price_raw_logs * royalty_rate / bundle_array_size
        ) :: INT AS creator_fee_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        _log_id,
        _inserted_timestamp
    FROM
        agg_details A
        INNER JOIN traces_raw_rn t USING (
            tx_hash,
            intra_tx_grouping
        )
        INNER JOIN logs_raw_rn l USING (
            tx_hash,
            intra_tx_grouping
        )
),
nft_details AS (
    SELECT
        contract_address AS nft_address,
        token_transfer_type
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2021-10-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY contract_address
    ORDER BY
        event_index ASC
) = 1
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
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2021-10-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    contract_address AS platform_address,
    'tofunft' AS platform_name,
    'tofunft v1' AS platform_exchange_version,
    intra_tx_grouping,
    trunc_grouping,
    user_address,
    signer_address,
    caller_address,
    fee_rate,
    royalty_rate,
    fee_receiver_address,
    royalty_receiver_address,
    bundle_array_size,
    nft_address,
    tokenId,
    token_transfer_type,
    IFF(
        token_transfer_type = 'erc721_Transfer',
        NULL,
        erc1155_value_raw
    ) AS erc1155_value,
    kind,
    CASE
        WHEN kind = 1 THEN 'sale'
        ELSE 'bid_won'
    END AS event_type,
    status,
    buyer_address,
    seller_address,
    currency_address,
    price_raw_traces,
    net_price_raw,
    price_raw_logs,
    total_price_raw,
    platform_fee_raw,
    creator_fee_raw,
    total_fees_raw,
    _log_id,
    _inserted_timestamp,
    CONCAT(
        nft_address,
        '-',
        tokenId,
        '-',
        platform_exchange_version,
        '-',
        _log_id
    ) AS nft_log_id,
    from_address AS origin_from_address,
    to_address AS origin_to_address,
    origin_function_signature,
    tx_fee,
    input_data
FROM
    base
    INNER JOIN nft_details USING (nft_address)
    INNER JOIN tx_data USING (tx_hash)
