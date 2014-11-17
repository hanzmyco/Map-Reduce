import sys

lines = open(sys.argv[1]).readlines()
lens = [len(line) for line in lines]
word_lens = [max([len(word) for word in line.split(' ')]) for line in lines]
value_size = max(lens)
key_size = max(word_lens)
print "Key size is {0}, value size is {1}".format(key_size, value_size)

with open('map_input', 'w') as output:
	for i, line in enumerate(lines):
		output.write(str(i).rjust(key_size, '0'))
		output.write(line.strip().ljust(value_size, ' '))