
This example is assuming you have a PostgreSQL table matching this schema:

```sql 
CREATE TABLE public.ch17_lab900_pkey
(
    fname text COLLATE pg_catalog."default" NOT NULL,
    lname text COLLATE pg_catalog."default" NOT NULL,
    id integer NOT NULL,
    score integer NOT NULL,
    CONSTRAINT ch17_lab900_pkey_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.ch17_lab900_pkey
    OWNER to jgp;
```  

Then run several time the application `appendDataJdbcPrimaryKeyApp.py` and change one, several, or none of the primary key to see how Spark behaves.
 
