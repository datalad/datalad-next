"""
This program emulates a shell like behavior.

Shell-like behavior means that a single line of input (From
stdin) leads to a random number of output lines, followed by
a known "marker".

The program reads a single lines from stdin and uses the
stripped content as "marker". It then emits a random number
of output-lines.

If the number of random output lines is odd, the last random output
lines is terminated with a newline, then the marker and a newline is
written out.

If number of random output lines is even, the last random output
line is terminated by the marker and a newline.

The program randomly outputs one additional, newline-terminated,
line after the marker
"""
import random
import sys
import time


marker = sys.stdin.readline().strip()
output_line_count = 1 + random.randrange(8)
last_end = '\n' if output_line_count % 2 == 1 else ''
for i in range(output_line_count):
    print(time.time(), end='\n' if i < output_line_count - 1 else last_end)
print(marker)
if random.randrange(2) == 1:
    print(f'random additional output {time.time()}')
