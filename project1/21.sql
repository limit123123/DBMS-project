SELECT name
FROM Trainer, CatchedPokemon
WHERE Trainer.id = owner_id
GROUP BY name, pid
HAVING COUNT(*) >= 2
ORDER BY name;