SELECT Trainer.name, COUNT(*)
FROM Trainer, CatchedPokemon
WHERE Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.name
ORDER BY Trainer.name;