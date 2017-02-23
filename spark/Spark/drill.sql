SELECT monthDate, count(clientIpAddress) AS c
FROM (
  SELECT DISTINCT columns[3] as monthDate, columns[0] as clientIpAddress
  FROM dfs.`/datalake/prepared/big/data.csv`)
GROUP BY monthDate
ORDER BY monthDate ASC;