{{ config(
    materialized = 'incremental',
    unique_key = "contract_address",
    full_refresh = false,
    tags = ['non_realtime']
) }}

WITH node_keys AS (

    SELECT
        node_url,
        headers
    FROM
        {{ source(
            'streamline_crosschain',
            'node_mapping'
        ) }}
    WHERE
        chain = 'bsc'
),
base AS (
    SELECT
        contract_address,
        latest_block
    FROM
        {{ ref('silver__relevant_contracts') }}

{% if is_incremental() %}
WHERE
    contract_address NOT IN (
        SELECT
            contract_address
        FROM
            {{ this }}
    )
{% endif %}
LIMIT
    1000
), function_sigs AS (
    SELECT
        '0x313ce567' AS function_sig,
        'decimals' AS function_name
    UNION
    SELECT
        '0x06fdde03',
        'name'
    UNION
    SELECT
        '0x95d89b41',
        'symbol'
),
all_reads AS (
    SELECT
        *
    FROM
        base
        JOIN function_sigs
        ON 1 = 1
),
ready_reads AS (
    SELECT
        contract_address,
        latest_block,
        function_sig,
        CONCAT(
            '[\'',
            contract_address,
            '\',',
            latest_block,
            ',\'',
            function_sig,
            '\',\'\']'
        ) AS read_input
    FROM
        all_reads
),
row_nos AS (
    SELECT
        contract_address,
        latest_block,
        function_sig,
        read_input,
        ROW_NUMBER() over (
            ORDER BY
                contract_address,
                function_sig
        ) AS row_no,
        FLOOR(
            row_no / 300
        ) + 1 AS batch_no,
        node_url,
        headers
    FROM
        ready_reads
        JOIN node_keys
        ON 1 = 1
),
groups AS (
    SELECT
        batch_no,
        node_url,
        headers,
        PARSE_JSON(CONCAT('[', LISTAGG(read_input, ','), ']')) AS read_input
    FROM
        row_nos
    GROUP BY
        batch_no,
        node_url,
        headers
),
batched AS ({% for item in range(11) %}
SELECT
    ethereum.streamline.udf_json_rpc_read_calls(node_url, headers, read_input) AS read_output
FROM
    groups
WHERE
    batch_no = {{ item }} + 1
    AND EXISTS (
SELECT
    1
FROM
    row_nos
WHERE
    batch_no = {{ item }} + 1
LIMIT
    1) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}),
FINAL AS (
    SELECT
        VALUE :id :: STRING AS read_id,
        VALUE :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        read_id_object [0] :: STRING AS contract_address,
        read_id_object [1] :: STRING AS block_number,
        read_id_object [2] :: STRING AS function_sig,
        read_id_object [3] :: STRING AS function_input
    FROM
        batched,
        LATERAL FLATTEN(
            input => read_output [0] :data
        )
)
SELECT
    contract_address,
    block_number,
    function_sig,
    function_input,
    read_result,
    SYSDATE() :: TIMESTAMP AS _inserted_timestamp
FROM
    FINAL
