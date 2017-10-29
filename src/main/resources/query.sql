SELECT IP, count(IP) from MAIN_LOG where START_DATE between ? and ? GROUP BY IP HAVING count(IP) >= ?;
SELECT * FROM MAIN_LOG WHERE IP = ?;