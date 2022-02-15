SELECT AVG(level)
FROM Trainer JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
WHERE Trainer.name = 'Red';