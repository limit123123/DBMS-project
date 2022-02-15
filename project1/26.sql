SELECT name, SUM(level)
FROM Trainer, CatchedPokemon
WHERE Trainer.id = owner_id
GROUP BY name
HAVING SUM(level) = ( SELECT MAX(sum) FROM (
	SELECT SUM(level) AS sum
	FROM Trainer, CatchedPokemon
	WHERE Trainer.id = owner_id
	GROUP BY name ) tb1);
