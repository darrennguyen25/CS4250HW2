#-------------------------------------------------------------------------
# AUTHOR: Darren Nguyen
# FILENAME: db_connection.py
# SPECIFICATION: Methods for index.py to use to interact with the database
# FOR: CS 4250 - Assignment #2
# TIME SPENT: 6 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2
from psycopg2.extras import RealDictCursor

def connectDataBase():
    # Create a database connection object using psycopg2
    DB_NAME = "corpus"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    try:
        conn = psycopg2.connect(
            database=DB_NAME, 
            user=DB_USER, 
            password=DB_PASS, 
            host=DB_HOST, 
            port=DB_PORT, 
            cursor_factory=RealDictCursor)
        return conn
    except:
        print("Database not connected successfully")

def createCategory(cur, catId, catName):

    # Insert a category in the database
    cur.execute("INSERT INTO Category (id, name) VALUES (%s, %s)", [catId, catName])

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    cur.execute("SELECT id FROM Category WHERE name = %(docCat)s", {'docCat': docCat})
    recset = cur.fetchall()
    catId = recset[0]['id']

    # 2 Insert the document in the database
    cur.execute("INSERT INTO Documents (doc, text, title, num_chars, date, id_category) VALUES (%s, %s, %s, %s, %s, %s)",
                [docId, docText, docTitle, len(docText), docDate, catId])

    # 3 Update the potential new terms. Remember to format the terms to lowercase and to remove punctuation marks.
    # 3.1 Find all terms that belong to the document
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database
    docTextList = docText.lower().replace(".", "").replace("?", "").replace("!", "").replace(",", "").split()
    noDupes = list(set(docTextList))

    cur.execute("SELECT term FROM Terms")
    recset = cur.fetchall()

    for text in noDupes:
        cur.execute("INSERT INTO Terms (term, num_chars) SELECT %s, %s WHERE NOT EXISTS (SELECT 1 FROM Terms WHERE term = %s)", 
                    [text, len(text), text])

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    docDict = {}
    for text in docTextList:
        if text in docDict:
            docDict[text] += 1
        else:
            docDict[text] = 1

    for key in list(docDict.keys()):
        cur.execute("INSERT INTO Index (term, id_doc, count) VALUES (%s, %s, %s)",
                    [key, docId, docDict[key]])

def deleteDocument(cur, docId):

    # 1 Query the index based on the document
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    cur.execute("SELECT term FROM Index WHERE id_doc = %(docId)s", {'docId': docId})
    termsCurrDoc = [item['term'] for item in cur.fetchall()]
    cur.execute("DELETE FROM Index WHERE id_doc = %(docId)s", {'docId': docId})

    cur.execute("SELECT term FROM Index")
    termsOtherDoc = [item['term'] for item in cur.fetchall()]
    for term in termsCurrDoc:
        if term not in termsOtherDoc:
            cur.execute("DELETE FROM Terms WHERE term = %(term)s", {'term': term})

    # 2 Delete the document from the database
    cur.execute("DELETE FROM Documents WHERE doc = %(docId)s", {'docId': docId})

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    invertedIndex = {}
    cur.execute("SELECT Index.term, Documents.title, Index.count FROM Index INNER JOIN Documents ON Index.id_doc = Documents.doc ORDER BY Index.term ASC")
    recset = cur.fetchall()
    for item in recset:
        if item['term'] in invertedIndex:
            invertedIndex[item['term']] += "," + item['title'] + ":" + str(item['count'])
        else:
            invertedIndex[item['term']] = item['title'] + ":" + str(item['count'])
    return invertedIndex