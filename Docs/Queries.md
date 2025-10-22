<!-- Source azure sql query -->
SELECT * FROM @{item().schema}.@{item().table} WHERE @{item().cdc_col} > '@{if(empty(item().from_date),activity('last_cdc').output.value[0].cdc,item().from_date)}'

<!-- Script for if new record query -->
SELECT MAX(@{item().cdc_col}) as cdc FROM @{item().schema}.@{item().table}
