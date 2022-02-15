SELECT Trainer.name, AVG(level)
FROM Gym JOIN Trainer ON Gym.leader_id = Trainer.id 
JOIN CatchedPokemon ON Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.name
ORDER BY Trainer.name;