SELECT hometown
FROM Trainer 
GROUP BY hometown 
HAVING COUNT(*) = (
SELECT MAX(c) FROM (SELECT COUNT(*) c FROM Trainer GROUP BY hometown) tb1);