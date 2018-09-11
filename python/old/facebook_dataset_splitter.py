
# open facebook new orleans dataset
with open('facebook-links.txt.anon', 'r') as fb_links:
	fb_lines = fb_links.readlines()
	fb_line_count = len(fb_lines)


p40_graph_size = 640122 # initial graph of dataset had 640122 lines which is 40% of original file
p40_stream_size = 905566 # initial stream of dataset had 905566 lines which is 60% of original file
stream_10p_decrement = p40_stream_size / 6 # divide by 6 to get 10% of original file

# calculate initial graph lines for different percentages of initial dataset
graph_percentages = [0.5, 0.6, 0.7, 0.8]
for p in graph_percentages:
	graph_size = fb_line_count * p # current starting graph size
	number_of_ignored_timestamps = ((6 - ((1 - p) * 10)) *  stream_10p_decrement)
	stream_size = p40_stream_size -  number_of_ignored_timestamps# current stream size
	print('Keeping first {0} timestamps as normal edges'.format(str(number_of_ignored_timestamps)))
	graph_lines = []
	stream_lines = []
	for l in fb_lines:
		tokens = l.strip().split('\t')
		if tokens[2] == '\\N':
			#processing normal interaction
			graph_lines.append([tokens[0], tokens[1]])
		elif number_of_ignored_timestamps > 0:
			#processing timestamped interaction
			number_of_ignored_timestamps = number_of_ignored_timestamps - 1
			graph_lines.append([tokens[0], tokens[1]])
		else:
			stream_lines.append([tokens[0], tokens[1], tokens[2]])
	graph_name = 'facebook-links-init-{0}.txt'.format(str(int(p * 100)))
	stream_name = 'facebook-links-cont-{0}.txt'.format(str(int(p * 100)))
	print('{2}: {0} ({4}%)\n{3}: {1} ({5}%)\n'.format(str(len(graph_lines)), str(len(stream_lines)), graph_name, stream_name, str(100*len(graph_lines)/fb_line_count), str(100*len(stream_lines)/fb_line_count)))
	
	
	with open(graph_name, 'w') as graph:
		for e in graph_lines:
			graph.write('{0}\t{1}\n'.format(e[0], e[1]))
			
	with open(stream_name, 'w') as stream:
		for e in stream_lines:
			stream.write('{0}\t{1}\t{2}\n'.format(e[0], e[1], e[2]))
			
	#break