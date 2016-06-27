# Split a text into pairs of words
# For example, "My name is Clare"
# should return ["My name", "name is", "is Clare"] 
def splitReviewIntoPair(review, size):
    splitReviewWords = review.split() # list of words
    output = []
    numWords = len(splitReviewWords)
    for i in xrange(numWords-size+1):
        element = []
        for j in xrange(size):
            element.append(splitReviewWords[i+j])
        output.append(" ".join(element))
    return output 


