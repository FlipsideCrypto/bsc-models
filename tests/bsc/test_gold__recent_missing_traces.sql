-- depends_on: {{ ref('test_silver__transactions_recent') }}
{{ recent_missing_traces(ref("test_gold__fact_traces_recent"), 10000) }}
