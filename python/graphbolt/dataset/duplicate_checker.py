


start_edges = []
with open("Cit-HepPh-10000-start.tsv", 'r') as start:
	lines = start.readlines()
	for l in lines:
		start_edges.append(l.strip())


stream_edges = []
with open("Cit-HepPh-10000-stream.tsv", 'r') as stream:
	lines = stream.readlines()
	for l in lines:
		if l.strip() in start_edges:
			print("Duplicate: " + l.strip())
		stream_edges.append(l.strip())


deletion_edges = []
deleted_map = {}
with open("Cit-HepPh-10000-deletions.tsv", 'r') as deletions:
	lines = deletions.readlines()
	for l in lines:
		stipped_line = l.strip()
		if (stipped_line in start_edges) or (stipped_line in stream_edges):
			#print("DELETION: " + l.strip())
			pass
		deletion_edges.append(stipped_line)

		if not stipped_line in deleted_map:
			deleted_map[stipped_line] = 1
		else: 
			deleted_map[stipped_line] = deleted_map[stipped_line] + 1


print("DELETION MAP")
repetitions = [edge for edge, ctr in deleted_map.items() if ctr > 1]
for e in repetitions:
	print("{}\t{}".format(e, str(deleted_map[e])))

print("DELETION SET")
#print(deletion_edges)
print("Sans duplicates:" + str(len(set(deletion_edges))))
print("All:" + str(len(deletion_edges)))