CREATE DATABASE assignment;
CREATE SCHEMA homebase;

CREATE TABLE IF NOT EXISTS homebase.bank_marketing
(	id int not null primary key , 
    age integer NOT NULL,
    job character varying(15) COLLATE pg_catalog."default",
    marital character varying(10) COLLATE pg_catalog."default",
    education character varying(20) COLLATE pg_catalog."default",
    "default" boolean,
    balance integer,
    housing boolean,
    loan boolean,
    contact character varying(15) COLLATE pg_catalog."default",
    day integer,
    month character varying(3) COLLATE pg_catalog."default",
    duration integer,
    campaign integer,
    pdays integer,
    previous integer,
    poutcome character varying(15) COLLATE pg_catalog."default",
    deposit boolean
)

ALTER TABLE IF EXISTS homebase.bank_marketing
  ADD COLUMN IF NOT EXISTS  created_at timestamp without time zone DEFAULT now(),
  ADD COLUMN IF NOT EXISTS  updated_at timestamp without time zone DEFAULT now();

CREATE OR REPLACE FUNCTION public.update_timestamp()
    RETURNS trigger
    LANGUAGE plpgsql
AS
$function$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
RETURN NEW;
END;
$function$
;
create trigger update_timestamp
    before update
    on homebase.bank_marketing
    for each row
execute procedure update_timestamp();


ALTER TABLE IF EXISTS homebase.bank_marketing
    OWNER to linhtb;
