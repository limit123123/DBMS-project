SELECT Trainer.name, CatchedPokemon.nickname
FROM Trainer, CatchedPokemon, 
  (SELECT Trainer.name, MAX(level) AS max_level
  FROM Trainer, CatchedPokemon
  WHERE Trainer.id = CatchedPokemon.owner_id
      AND Trainer.name IN 
      (SELECT Trainer.name
      FROM Trainer, CatchedPokemon
      WHERE Trainer.id = CatchedPokemon.owner_id
      GROUP BY Trainer.name
      HAVING COUNT(*) >= 4)
  GROUP BY Trainer.name ) tb1
WHERE Trainer.name = tb1.name AND CatchedPokemon.level = tb1.max_level
ORDER BY CatchedPokemon.nickname;