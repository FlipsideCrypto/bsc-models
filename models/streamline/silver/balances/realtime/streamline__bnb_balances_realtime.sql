{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'bnb_balances', 'method', 'eth_getBalance', 'producer_batch_size',5000, 'producer_limit_size', 5000000, 'worker_batch_size',500))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_balances_realtime']
) }}

WITH last_3_days AS (

    SELECT
        block_number
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_number DESC
        ) = 3
),
traces AS (
    SELECT
        block_number,
        from_address,
        to_address
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        VALUE > 0
        AND trace_succeeded
        AND tx_succeeded
        AND block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND block_timestamp :: DATE >= DATEADD(
            'day',
            -5,
            CURRENT_TIMESTAMP
        )
),
stacked AS (
    SELECT
        DISTINCT block_number,
        from_address AS address
    FROM
        traces
    WHERE
        from_address IS NOT NULL
        AND from_address <> '0x0000000000000000000000000000000000000000'
    UNION
    SELECT
        DISTINCT block_number,
        to_address AS address
    FROM
        traces
    WHERE
        to_address IS NOT NULL
        AND to_address <> '0x0000000000000000000000000000000000000000'
),
FINAL AS (
    SELECT
        block_number,
        address
    FROM
        stacked
    WHERE
        block_number IS NOT NULL
    EXCEPT
    SELECT
        block_number,
        address
    FROM
        {{ ref("streamline__complete_bnb_balances") }}
    WHERE
        block_number >= (
            SELECT
                block_number
            FROM
                last_3_days
        )
        AND _inserted_timestamp :: DATE >= DATEADD(
            'day',
            -7,
            CURRENT_TIMESTAMP
        )
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "eth_getBalance", "params": ["',
            address,
            '", "',
            REPLACE(
                CONCAT(
                    '0x',
                    to_char(
                        block_number :: INTEGER,
                        'XXXXXXXX'
                    )
                ),
                ' ',
                ''
            ),
            '"], "id": "',
            block_number :: STRING,
            '"}'
        )
    ) AS request
FROM
    FINAL
ORDER BY
    block_number ASC
LIMIT
    10 -- TODO: remove this limit
