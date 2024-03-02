import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(
    host="localhost",
    database="corpus",
    user="postgres",
    password="123",
    port="5432",
    cursor_factory=RealDictCursor
)

cur = conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS public.Category (
            id INT NOT NULL,
            name text NOT NULL,
            CONSTRAINT category_pkey PRIMARY KEY (id)
);
""")

cur.execute("""CREATE TABLE IF NOT EXISTS public.Documents (
            doc INT NOT NULL,
            text text NOT NULL,
            title text NOT NULL,
            num_chars INT NOT NULL,
            date date NOT NULL,
            id_category INT NOT NULL,
            CONSTRAINT documents_pk PRIMARY KEY (doc),
            CONSTRAINT id_category FOREIGN KEY (id_category) 
                REFERENCES public.Category (id) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
);
""")

cur.execute("""CREATE TABLE IF NOT EXISTS public.Terms(
            term text NOT NULL,
            num_chars INT NOT NULL,
            CONSTRAINT terms_pk PRIMARY KEY (term)
);
 """)

cur.execute("""CREATE TABLE IF NOT EXISTS public.INDEX(
            term text NOT NULL,
            id_doc INT NOT NULL,
            count INT NOT NULL,
            CONSTRAINT index_pk PRIMARY KEY (term, id_doc),
            CONSTRAINT id_doc FOREIGN KEY (id_doc) 
                REFERENCES public.Documents (doc) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT term FOREIGN KEY (term)
                REFERENCES public.Terms (term) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
);
""")

conn.commit()

cur.close()

