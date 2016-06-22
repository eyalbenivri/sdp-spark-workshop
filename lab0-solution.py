# Task 1: Implement Word Count in plain python where input is a list of words
# tip: in the python REPL, enter help(dict.setdefault)
def wordcount(words):
    wc = {}
    for word in words:
        wc[word] = wc.setdefault(word, 0) + 1
    return wc


# Task 2: Implement Word Count in plain python where input is a list of sentences
def flatmap(flatfunc, collection):
    return [icol for item in collection for icol in flatfunc(item)]


def swordcount(sentences):
    words = flatmap(lambda str: str.split(" "), sentences)
    return wordcount(words)


# Task 3: Implement the following functions, using map, reduce, filter, etc. but NO LOOPS!!

# Given a list of numbers, return only the even numbers.
def evens(numbers):
    return filter(lambda n: n % 2 == 0, numbers)


# Given a list of numbers, raise each number to the power of 2.
def squares(numbers):
    return map(lambda n: n * n, numbers)


# Given a list of words, find the average word length.
def avg_length(words):
    return reduce(lambda sum, word: sum + len(word), words, 0) / float(len(words))


# for the range of 1 to 100, find the difference between the sum of the squares and the square of the sum.
# origin (Project Euler): https://projecteuler.net/problem=6
def problem6():
    def _sum(numbers):
        return reduce(lambda a, b: a + b, numbers)  # or use built-in sum()

    def square(n):
        return n * n

    return _sum(squares(xrange(1, 100))) - square(_sum(xrange(1, 100)))


if __name__ == "__main__":
    print(wordcount(["hello", "world", "hello", "sdp"]))
    # should print {'world': 1, 'sdp': 1, 'hello': 2}

    print(swordcount(["hello world", "hello sdp", "hello everyone"]))
    # should print {'world': 1, 'everyone': 1, 'sdp': 1, 'hello': 3}

    print(evens([1, 2, 6, 5, 4, 8, 7, 4, 3, 2]))
    # should print [2, 6, 4, 8, 4, 2]

    print(squares([-1, 2, 3, -4]))
    # should print [1, 4, 9, 16]

    print(avg_length(["hello", "world", "hello", "sdp"]))
    # should print 4.5

    print(problem6())
    # should print -24174150