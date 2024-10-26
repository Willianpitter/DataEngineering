select datasource, max(lastests) as latest
from (
    SELECT region, datasource, MAX(datetime) as lastests
    FROM trips
    WHERE region IN (
        SELECT region
        FROM trips
        GROUP BY region
        ORDER BY COUNT(*) DESC
        LIMIT 2
    )
    GROUP BY region, datasource
)
GROUP BY datasource order by latest desc limit 1;