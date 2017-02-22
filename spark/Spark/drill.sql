SELECT monthDate, count(clientIpAddress) AS c
FROM (
  SELECT DISTINCT monthDate, clientIpAddress
  FROM dfs.`/user/ubuntu/full/prepared/data.csv`
  WHERE domainName = 'www.mywebsite.com')
GROUP BY monthDate
ORDER BY monthDate ASC;
