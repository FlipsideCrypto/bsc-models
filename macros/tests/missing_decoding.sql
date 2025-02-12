{% test missing_decoded_logs(model) %}
SELECT
    l.block_number,
    CONCAT(
        l.tx_hash,
        '-',
        l.event_index
    ) AS log_id
FROM
    {{ ref('core__fact_event_logs') }}
    l
    LEFT JOIN {{ model }}
    d
    ON l.block_number = d.block_number
    AND CONCAT(
        l.tx_hash,
        '-',
        l.event_index
    )  = d._log_id
WHERE
    l.contract_address = LOWER('0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c') -- WBNB
    AND l.topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- Transfer
    AND l.block_timestamp BETWEEN DATEADD('hour', -48, SYSDATE())
    AND DATEADD('hour', -6, SYSDATE())
    AND d._log_id IS NULL {% endtest %}
