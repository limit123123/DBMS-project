SELECT SUM(level)
FROM CatchedPokemon, Pokemon
WHERE pid = Pokemon.id AND type = 'fire';