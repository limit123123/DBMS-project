SELECT Pokemon.name
FROM Gym, Trainer, CatchedPokemon, Pokemon
WHERE Gym.leader_id = Trainer.id 
	AND Trainer.id = CatchedPokemon.owner_id
    AND CatchedPokemon.pid = Pokemon.id
    AND Gym.city = 'Rainbow City'
ORDER BY Pokemon.name;