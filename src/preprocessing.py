import sys
import re
from re import sub

import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer


nltk.download('wordnet')
nltk.download('stopwords')

stop = stopwords.words('english')
wnl = WordNetLemmatizer()
stemmer = PorterStemmer()


class Preprocessing:
    """Preprocess a data frame."""

    def __init__(self, df) -> None:
        """Initilize df."""
        try:
            self.df = df
            
        except Exception:
            sys.exit(1)

    def removePunc(self, myWord):
        """Remove punctuation from string inputs."""
        if myWord is None:
            return myWord
        else:
            return sub('[.:;()/!&-*@$,?^\d+]', '', myWord)

    def removeAscii(self, myWord):
        """Remove ascii from string input."""
        if myWord is None:
            return myWord
        else:
            return str(sub(r'[^\x00-\x7F]+', '', myWord.strip()))

    def lemmatize(self, myWord):
        """Lemmatize words."""
        if myWord is None:
            return myWord
        else:
            return str(wnl.lemmatize(myWord))

    def removeStopWords(self, myWord):
        """Remove stop words."""
        if myWord is None:
            return myWord
        if myWord not in str(stopwords.words('english')):
            return myWord

    def removeLinkUser(self, myWord):
        """Remove web addresses and handles."""
        if not myWord.startswith('@') and not myWord.startswith('http'):
            return myWord

    def prepText(self, myWord):
        """Call other text pre-processing function."""
        try:
            prep = self.removeStopWords(
                self.lemmatize(
                    self.removeAscii(
                        self.removePunc(
                            self.removeLinkUser(
                                myWord.lower()
                            )
                        )
                    )
                )
            )
                        
            return prep
        except Exception:
                        
            sys.exit(1)

    def filterSlackList(self, slacklist):
        """Remove stop words, lemmatize, and clean all Slack messages."""
        try:
            # Apply prepText to each word in each Slack message
            processed_messages = [
                [self.prepText(word) for word in slackmessage.split()]
                for slackmessage in slacklist
            ]

            # Find the maximum length of any list in processed_messages
            max_length = max(len(message) for message in processed_messages)

            # Pad each inner list with None values to match the maximum length
            cleaned_messages = [
                message + [None] * (max_length - len(message))
                for message in processed_messages
            ]

            return cleaned_messages
        except Exception as e:
            print(f"An error occurred: {e}")
            sys.exit(1)

    # def preprocess(self, df):
    #     """Preprocess a data frame."""
    #     df['text'] = df['text'].apply(self.prepText)
    #     return df

    def cleantext(self, org_col, new_col):
        """Clean text.

        Args:
            org_col: Original column to be cleaned
            new_col: New column to hold cleaned text

        Returns:
            df: Dataframe with new column
        """
        try:
            self.df[org_col] = self.df[org_col].astype(str)
            self.df[new_col] = self.df[org_col].str.replace('[^\w\s]', '')
            self.df[new_col] = self.df[new_col].str.replace('\w*\d\w*', '')
            self.df[new_col] = self.df[new_col].apply(
                lambda x: " ".join(x.lower() for x in x.split()))
            self.df[new_col] = self.df[new_col].apply(
                lambda x: " ".join(x for x in x.split() if x not in stop))
            return self.df
        except Exception:
            sys.exit(1)

    def stem(self, col):
        """Stemm a word.

        Args:
            col: Column to be stemmed

        Returns:
            df: Dataframe with new column
        """
        try:
            # tokenize each slack message to its root word
            self.df[col] = self.df[col].apply(
                lambda x: " ".join([stemmer.stem(word) for word in x.split()]))
            return self.df
        except Exception:
            sys.exit(1)
            
    def preprocess_messages(self,df, col):
        
        
        df[col] = df[col].apply(lambda x: x.lower())
        df[col] = df[col].apply(lambda x: re.sub('[^a-zA-z0-9\s]', '', x))
        df[col] = df[col].apply(lambda x: re.sub(' +', ' ', x))
        df[col] = df[col].apply(lambda x: x.strip())
        df[col] = df[col].apply(lambda x: re.sub(r'\s+[a-zA-Z]\s+', ' ', x))
        
        return df