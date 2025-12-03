#!/usr/bin/env python
# vim: set ft=python:

# <----------IMPORTS---------->

import	json
from	contextlib	import suppress
from	statistics	import mean, median
from	sys			import argv
from	pathlib		import Path
from	itertools	import tee
from	collections.abc import Iterator

# <----------GLOBALS---------->

GOP_SIZE = 512
for x in argv:
	if '-g' in x[:2]:
		GOP_SIZE = int(x[2:])

HIERARCHICAL_LEVELS = 4 if '-strict' in argv else 0
DISCARD_SHORT_GOPS = not '-no-discard' in argv
REEVALUATE_DISCARD_GOPS = not '-no-reeval' in argv
METRIC = 2 if '-mixed' in argv else 1 if '-imp' in argv else 0
DEBUG = '-debug' in argv
WRITE_CONFIG = not '-no-config' in argv

# <----------HELPERS---------->

def metric(x):
	global METRIC
	return [
		x['inter_cost'],
		x['imp_block_cost'],
		x['imp_block_cost']*x['inter_cost']
	][METRIC]

def pairwise(iterable):
    "s -> (s0, s1), (s1, s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)

def debug(s: str):
	global DEBUG
	if DEBUG: print(s)

# <----------ITERATORS---------->

def merge_small(scenes: Iterator) -> Iterator:
	global GOP_SIZE, frame_count
	yield 0

	accumulated: int = 0
	for last, this in pairwise(scenes):
		length:	int = this - last
		if (accumulated + length) >= GOP_SIZE:
			debug(f"[{this}] merged -> {accumulated}")
			accumulated = 0
			yield last
		else:
			accumulated += length
			debug(f"[{this}] accumulating -> {accumulated}")

	yield frame_count


def generate_candidates(last: int, this: int, idrs: int, discarded_idrs: int, limit: int = 64) -> list:
	global scores, GOP_SIZE, HIERARCHICAL_LEVELS
	factor: float = 1 if '-exact' in argv else 1.122462

	candidates: list = []
	for k in reversed(range(HIERARCHICAL_LEVELS,6)):
			offset: int = last + idrs - discarded_idrs
			costs: list[dict] = sorted((
					score for score in scores[
						max(last, offset + ((idrs-1)*GOP_SIZE)):
						min(this, offset + ((idrs+1)*GOP_SIZE))
					] if (score['frame'] - offset) % (1<<k) == 0
				), key=metric
			)

			if candidates == []:
				candidates = costs[:limit]
			else:
				for c in range(len(candidates)):
					candidates[c] = costs[c] if metric(costs[c])*factor < metric(candidates[c]) else candidates[c]
				factor *= factor

	return candidates


def split_large(scenes: Iterator, minimum_size: int = GOP_SIZE//2) -> Iterator:
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
			
			if not DISCARD_SHORT_GOPS:
				yield (lastkey := candidates[0]["frame"])
				continue
			
			if all(discard := [
				candidate["frame"] - lastkey < minimum_size or this - candidate["frame"] < minimum_size
				for candidate in candidates
			]):
				discarded_idrs += 1
				discarded_frms += GOP_SIZE if REEVALUATE_DISCARD_GOPS else 0
				debug(f"[{this}] all candidates discarded -> {discarded_frms}")
			else:
				for c, candidate in enumerate(candidates):
					if discard[c]: continue
					debug(f"[{this}] =>> {candidate['frame']} (C{c}:{metric(candidate):.0f})")
					discarded_frms = 0
					yield (lastkey := candidate["frame"])
					break

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