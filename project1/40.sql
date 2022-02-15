SELECT Trainer.name
FROM Trainer, CatchedPokemon, Pokemon
WHERE Trainer.id = CatchedPokemon.owner_id 
	AND CatchedPokemon.pid = Pokemon.id
    AND Pokemon.name = 'Pikachu'
ORDER BY Trainer.name;