SELECT Trainer.name AS Trainer_name, Pokemon.name AS Pokemon_name, COUNT(*)
FROM Trainer, CatchedPokemon, Pokemon
WHERE Trainer.id = CatchedPokemon.owner_id
        AND CatchedPokemon.pid = Pokemon.id
        AND Trainer.name IN (
          SELECT tb1.name
          FROM (
              SELECT Trainer.name
              FROM Trainer, CatchedPokemon, Pokemon
              WHERE Trainer.id = CatchedPokemon.owner_id
                  AND CatchedPokemon.pid = Pokemon.id
              GROUP BY Trainer.name, type ) tb1
          GROUP BY tb1.name
          HAVING COUNT(*) = 1)
GROUP BY Trainer.name, Pokemon.name
ORDER BY Trainer.name;
