
import nltk

def Preprocessing(file):

    # read text file
    with open(file, "r", encoding = "utf-8") as file:
        text = file.read()

    # tokenize using nltk
    words = nltk.word_tokenize(text)

    # remove special chars and punctuation, lowercase tokens
    tokens = [word.lower() for word in words if word.isalpha()]

    return tokens

