use loglight;

SELECT monthDate, count(clientIpAddress) AS c
FROM (
  SELECT DISTINCT monthDate, clientIpAddress
  FROM light
  WHERE domainName = 'www.mywebsite.com') as d
GROUP BY monthDate
ORDER BY monthDate ASC
