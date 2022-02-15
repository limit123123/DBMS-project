SELECT Pokemon.name
FROM Trainer, CatchedPokemon, Pokemon
WHERE Trainer.id = CatchedPokemon.owner_id 
	AND CatchedPokemon.pid = Pokemon.id
    AND Trainer.hometown = 'Sangnok city'
    AND Pokemon.name IN ( SELECT Pokemon.name
		FROM Trainer, CatchedPokemon, Pokemon
		WHERE Trainer.id = CatchedPokemon.owner_id 
			AND CatchedPokemon.pid = Pokemon.id
    		AND Trainer.hometown = 'Blue city' )
ORDER BY Pokemon.name;