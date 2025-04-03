WITH provider_address_data AS (
    SELECT
        p.provider_id,
        p.provider_name,
        COALESCE(ARRAY_AGG(a.address), ARRAY[]::TEXT[]) AS addresses
    FROM
        providers AS p  -- No ref function if it's a table
    LEFT JOIN
        addresses AS a   -- No ref function if it's a table
        ON p.provider_id = a.provider_id
    GROUP BY
        p.provider_id, p.provider_name
)

SELECT * FROM provider_address_data;


