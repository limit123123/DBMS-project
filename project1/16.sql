SELECT Trainer.name
FROM Trainer, Gym, City
WHERE Trainer.id = Gym.leader_id AND Gym.city = City.name AND description = 'Amazon';