import sys

lines = open(sys.argv[1]).readlines()
lens = [len(line) for line in lines]
max_i = lens.index(max(lens))
value_size = max(lens)
key_size = len(str(len(lines)))
print "Key size is {0}, value size is {1}".format(key_size, value_size)

with open('map_input.txt', 'w') as output:
	for i, line in enumerate(lines):
		output.write(str(i).rjust(key_size, '0'))
		output.write(line.ljust(value_size, ' '))