WITH provider_degree_data AS (
    SELECT
        p.provider_id,
        p.provider_name,
        unnest(string_to_array(p.degrees, ',')) AS degree
    FROM
        {{ ref('providers') }} AS p
)

SELECT
    pd.provider_id,
    pd.provider_name,
    MIN(pd.degree) AS ptui
FROM
    provider_degree_data pd
GROUP BY
    pd.provider_id, pd.provider_name;
