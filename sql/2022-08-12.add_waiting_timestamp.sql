ALTER TABLE IF EXISTS public.page
    ADD COLUMN waiting_timestamp timestamp without time zone;


CREATE INDEX IF NOT EXISTS ix_page_waiting_timestamp
    ON public.page USING btree
    (waiting_timestamp ASC NULLS LAST)
    TABLESPACE pg_default;
