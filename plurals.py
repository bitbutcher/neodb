import re

singular_transforms = (
    (re.compile('.+ies$'), lambda word: word[:-3] + 'y'),
    (re.compile('.+ives$'), lambda word: word[:-4] + 'ife'),
    (re.compile('.+ves$'), lambda word: word[:-3] + 'f'),
    (re.compile('.+s$'), lambda word: word[:-1])
)

def singularize(plural):
    for pattern, transform in singular_transforms:
        if pattern.match(plural):
            return transform(plural)
    return plural

plural_transforms = (
	(re.compile('.+[^aeiou]y$'), lambda word: word[:-1] + 'ies'),
	(re.compile('.+f$'), lambda word: word[:-1] + 'ves'),
	(re.compile('.+fe$'), lambda word: word[:-2] + 'ves')
)

def pluralize(singular):
	for pattern, transform in plural_transforms:
		if pattern.match(singular):
			return transform(singular)
	return singular + 's'
