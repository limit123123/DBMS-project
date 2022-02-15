SELECT nickname
FROM (SELECT t1.hometown, c1.level, c1.nickname FROM Trainer t1 JOIN CatchedPokemon c1 ON t1.id = c1.owner_id) tb1
JOIN (
  SELECT t2.hometown, MAX(level) AS MAX_level
  FROM Trainer t2 JOIN CatchedPokemon c2 ON t2.id = c2.owner_id
  GROUP BY t2.hometown
 ) tb2 ON tb1.hometown = tb2.hometown AND tb1.level = tb2.MAX_level
ORDER BY nickname;