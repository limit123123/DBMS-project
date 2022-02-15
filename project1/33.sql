SELECT Trainer.name
FROM Trainer, CatchedPokemon, Pokemon
WHERE Trainer.id = CatchedPokemon.owner_id
	AND CatchedPokemon.pid = Pokemon.id
    AND Pokemon.type = 'Psychic'
ORDER BY Trainer.name;