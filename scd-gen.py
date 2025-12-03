#!/usr/bin/env python
# vim: set ft=python:

# <----------IMPORTS---------->

import	json
from	contextlib	import suppress
from	statistics	import mean, median
from	sys			import argv, stderr
from	pathlib		import Path
from	itertools	import tee
from	collections.abc import Iterator

# <----------GLOBALS---------->

# Dirty CLI
GOP_SIZE = 512
for x in argv:
	if '-g' in x[:2]:
		GOP_SIZE = int(x[2:])

DISCARD_SHORT_GOPS = not '-no-discard' in argv
REEVALUATE_DISCARD_GOPS = not '-no-reeval' in argv
METRIC = 2 if '-mixed' in argv else 1 if '-imp' in argv else 0
DEBUG = 2 if '-v-debug' in argv else 1 if '-debug' in argv else 0
WRITE_CONFIG = not '-no-config' in argv

# <----------HELPERS---------->

def logb(x: int):
	'quick minimum binary logarithm'
	i = 0
	while (x := x >> 1): i += 1;
	return i

def metric(x):
	global METRIC
	return x['inter_cost'] if METRIC == 0 \
		else x['imp_block_cost'] if METRIC == 1 \
		else x['imp_block_cost']*x['inter_cost']

def pairwise(iterable):
    's -> (s0, s1), (s1, s2), (s2, s3), ...'
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)

def debug(s: str, level: int = 0):
	global DEBUG
	if DEBUG > level:
		print(s, file=stderr)

# <----------ITERATORS---------->

def merge_small(scenes: Iterator, maximum: int = GOP_SIZE) -> Iterator:
	'merges small scenes up to the requested gop size'
	global frame_count
	yield 0

	accumulated: int = 0
	for last, this in pairwise(scenes):
		length:	int = this - last
		if (accumulated + length) >= maximum:
			debug(f"[{this}] merged -> {accumulated}")
			accumulated = 0
			yield last
		else:
			accumulated += length
			debug(f"[{this}] accumulating -> {accumulated}")

	yield frame_count


def generate_candidates(last: int, this: int, idrs: int, discarded_idrs: int) -> list:
	'yields split candidates in order of score'
	global scores, GOP_SIZE
	
	factor: float = 1 if '-exact' in argv else 1.122462
	offset: int = last + idrs - discarded_idrs

	hierarchy = lambda x: \
		5 if x%32 == 0	else \
		4 if x%16 == 0	else \
		3 if x%8 == 0	else \
		2 if x%4 == 0	else \
		1 if x%2 == 0	else 0

	return (candidate for candidate in reversed(sorted(
		scores[
			max(last, offset + ((idrs-1)*GOP_SIZE)):
			min(this, offset + ((idrs+1)*GOP_SIZE))
		],	key = lambda x: metric(x) * (factor ** hierarchy(x['frame']))
	)))


def split_large(scenes: Iterator, minimum_size: int = GOP_SIZE//2) -> Iterator:
	'splits large gops into smaller ones'
	global DISCARD_SHORT_GOPS, REEVALUATE_DISCARD_GOPS, GOP_SIZE
	lastkey: int = 0
	yield 0

	for i, (last, this) in enumerate(pairwise(scenes)):
		length: int = this - last
		required: int = length // GOP_SIZE
		debug(f"------------\nSCENE {i} : [{last} -> {this}] length: {length} required: {required}")

		discarded_idrs: int	= 0
		discarded_frms: int	= 0

		if last:
			lastkey = last
			yield last

		for j in range(1, required+1):
			candidates = generate_candidates(last, this, j, discarded_idrs)
			
			if not DISCARD_SHORT_GOPS: yield next(candidates); continue

			for c, candidate in enumerate(candidates):
				if candidate["frame"] - lastkey >= minimum_size and this - candidate["frame"] >= minimum_size:
					debug(f"[{this}] =>> {candidate['frame']} (C{c}:{metric(candidate):.0f})")
					debug(f"{candidate['frame'] - lastkey} frames from key, {this - candidate['frame']} frames to key", 1)
					discarded_frms = 0
					yield (lastkey := candidate["frame"]); break
			else:
				discarded_idrs += 1
				discarded_frms += GOP_SIZE if REEVALUATE_DISCARD_GOPS else 0
				debug(f"[{this}] all candidates discarded -> {discarded_frms}")


# <----------MAIN---------->

avsc = {}
with open(argv[1]) as f: avsc = json.load(f)
frame_count	= avsc["frame_count"]
debug(f"frame count: {frame_count}")
avsc_scenes	= avsc["scene_changes"]
avsc_scenes.append(frame_count)
scores = []
for i in range(frame_count):
	with suppress(KeyError): 
		score = avsc["scores"][str(i)]
		score["frame"] = i
		scores.append(score)

keyframes = list(split_large(merge_small(avsc_scenes)))
key_str = f"ForceKeyFrames : {'f,'.join(str(i) for i in keyframes)}f"

if WRITE_CONFIG:
	svt_config: Path = Path(argv[1]).with_suffix('.conf')
	with open(svt_config, 'w') as f: f.write(key_str)
print (key_str)

lengths = list(x[1] - (x[0]+1) for x in zip([0]+keyframes, keyframes+[frame_count]))[1:]
print(lengths)

print(f"{len(keyframes)} scenes")
print(f"mean: {mean(lengths)}")
print(f"median: {median(lengths)}")