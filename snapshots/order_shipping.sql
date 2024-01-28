{% snapshot order_shipping_snapshot %}
    {{
        config(
        target_database='arizona_staging',
        target_schema='snapshots',
          unique_key='order_number',

          strategy='timestamp',
          updated_at = 'updated_at'
        )
    }}
    -- Pro-Tip: Use sources in snapshots!
    select * from {{ source('arizona', 'order_shipping') }}
{% endsnapshot %}