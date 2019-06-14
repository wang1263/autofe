"""
Combine .py files into one
"""

import os
import re
import argparse

def build_parser():
	parser = argparse.ArgumentParser()
	parser.add_argument('-source', nargs='+',
		help='List of path of source file to combine')
	parser.add_argument('-target', default='./dist.py',
		help='Target file path')

	return parser

def combine(source, target):
	"""
	:param files: list of `.py` file path
	"""

	basenames = [os.path.basename(file) for file in source]
	filenames = [os.path.splitext(basename)[0] for basename in basenames]
	imports = []
	rest = []
	p = re.compile('(?<=\W)({})[.]'.format('|'.join(filenames)))
	for i,file in enumerate(source):
		with open(file, 'r') as f:
			for line in f:
				if 'import' in line:
					if any([filename in line for filename in filenames]) and 'pyspark' not in line:
						continue
					imports.append(line)
				else:
					if '__package__' in line:
						continue
					rest.append(re.sub(p, '', line))

	with open(target, 'w') as f:
		for line in (imports+rest):
			f.write(line)


if __name__ == '__main__':
	parser = build_parser()
	args = parser.parse_args()
	combine(args.source, args.target)
