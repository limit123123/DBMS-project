SELECT SUM(level)
FROM Trainer, CatchedPokemon
WHERE Trainer.id = owner_id AND name = 'Matis';