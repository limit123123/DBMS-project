SELECT CatchedPokemon.nickname 
FROM CatchedPokemon, Pokemon
WHERE CatchedPokemon.pid = Pokemon.id
	AND CatchedPokemon.owner_id IN 
    (SELECT Trainer.id 
    FROM Trainer, Gym
	WHERE Trainer.id = Gym.leader_id AND hometown = 'Sangnok City')
    AND Pokemon.type = 'water'
ORDER BY CatchedPokemon.nickname;