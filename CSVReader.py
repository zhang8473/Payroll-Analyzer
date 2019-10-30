#!/usr/bin/env python
import sys
import csv
def mapper():
    reader = csv.reader(sys.stdin,strict=True, skipinitialspace = True, quoting = csv.QUOTE_ALL, quotechar = '"', delimiter = ',')
    writer = csv.writer(sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    for line in reader:
        print line

def main():
    import StringIO
    #sys.stdin = StringIO.StringIO(test_text)
    sys.stdin = sys.__stdin__
    mapper()
if __name__ == "__main__":
    main()

# test_text = """\"\"\t\"\"\t\"\"\t\"\"\t\"This is one sentence\"\t\"\"
# \"\"\t\"\"\t\"\"\t\"\"\t\"Also one sentence!\"\t\"\"
# \"\"\t\"\"\t\"\"\t\"\"\t\"Hey!\nTwo sentences!\"\t\"\"
# \"\"\t\"\"\t\"\"\t\"\"\t\"One. Two! Three?\"\t\"\"
# \"\"\t\"\"\t\"\"\t\"\"\t\"One Period. Two Sentences\"\t\"\"
# \"\"\t\"\"\t\"\"\t\"\"\t\"Three\nlines, one sentence\n\"\t\"\"
# """
