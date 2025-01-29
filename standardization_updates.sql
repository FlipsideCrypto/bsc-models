#!/bin/bash

if [ -z "$1" ]; then
    echo "Please provide a directory path"
    exit 1
fi

find "$1" -type f -name "*.sql" | while read -r file; do
    # Replace the _log_id pattern in files referencing silver__logs
    sed -i '
        /{{ ref (.silver__logs.|.silver__decoded_logs.)/{
            N;N;N;
            s/_log_id,\n.*_inserted_timestamp\n.*FROM/CONCAT(\ntx_hash :: STRING,\n'\''-'\'',\nevent_index :: STRING\n) AS _log_id,\nmodified_timestamp\nFROM/g
        }
    ' "$file"
    
    # Replace remaining _inserted_timestamp with modified_timestamp
    sed -i 's/_inserted_timestamp/modified_timestamp/g' "$file"
    
    echo "Processed: $file"
done