from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
 #          word = unicode(word, "utf-8", errors="ignore") #avoids issues in mrjob 5.0
            yield word.lower(), 1
        

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
