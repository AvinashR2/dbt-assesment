WITH provider_address_data AS (
    SELECT
        p.provider_id,
        p.provider_name,
        COALESCE(ARRAY_AGG(a.address), ARRAY[]::TEXT[]) AS addresses
    FROM
        {{ ref('providers') }} AS p
    LEFT JOIN
        {{ ref('addresses') }} AS a
        ON p.provider_id = a.provider_id
    GROUP BY
        p.provider_id, p.provider_name
)

SELECT * FROM provider_address_data;
