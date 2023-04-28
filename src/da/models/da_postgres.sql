CREATE TABLE IF NOT EXISTS public.event_types
(
    id integer NOT NULL,
    event text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT event_types_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.raw_events
(
    event_id text COLLATE pg_catalog."default" NOT NULL,
    longitude double precision NOT NULL,
    latitude double precision NOT NULL,
    coordinates point,
    event_type_id integer NOT NULL,
    event_date timestamp without time zone,
    CONSTRAINT raw_events_pkey PRIMARY KEY (event_id),
    CONSTRAINT fk_event_types_key FOREIGN KEY (event_type_id)
        REFERENCES public.event_types (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.taxon_id_verbatim
(
    id text COLLATE pg_catalog."default" NOT NULL,
    verbatim_source text COLLATE pg_catalog."default" NOT NULL,
    verbatim_id text COLLATE pg_catalog."default" NOT NULL,
    taxon_id text COLLATE pg_catalog."default",
    CONSTRAINT taxon_id_verbatim_pkey PRIMARY KEY (id),
    CONSTRAINT ix_source_id UNIQUE (verbatim_source, verbatim_id)
);

CREATE TABLE IF NOT EXISTS public.raw_occurrences
(
    occurrence_id text COLLATE pg_catalog."default" NOT NULL,
    taxon_verbatim_id text COLLATE pg_catalog."default" NOT NULL,
    event_id text COLLATE pg_catalog."default" NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    CONSTRAINT raw_occurrences_pkey PRIMARY KEY (occurrence_id, taxon_verbatim_id),
    CONSTRAINT fk_raw_events_key FOREIGN KEY (event_id)
        REFERENCES public.raw_events (event_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.raw_taxa
(
    taxon_id text COLLATE pg_catalog."default" NOT NULL,
    kingdom text COLLATE pg_catalog."default",
    phylum text COLLATE pg_catalog."default",
    class_taxon text COLLATE pg_catalog."default",
    "order" text COLLATE pg_catalog."default",
    family text COLLATE pg_catalog."default",
    subfamily text COLLATE pg_catalog."default",
    genus text COLLATE pg_catalog."default",
    subgenus text COLLATE pg_catalog."default",
    species text COLLATE pg_catalog."default",
    scientific_name text COLLATE pg_catalog."default" NOT NULL,
    canonical_name text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT raw_taxa_pkey PRIMARY KEY (taxon_id),
    CONSTRAINT fk_taxon_id_verbatim_key FOREIGN KEY (taxon_id)
        REFERENCES public.taxon_id_verbatim (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

INSERT INTO public.event_types(
	id, event)
	VALUES (1, 'catalog');

CREATE OR REPLACE FUNCTION public.add_raw_occurrence(
	in_occurrence_id text,
	in_taxon_verbatim_id text,
	in_event_id text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN

    IF NOT EXISTS(SELECT occurrence_id FROM public.raw_occurrences where occurrence_id=in_occurrence_id)
    THEN
		INSERT INTO public.raw_occurrences(
			occurrence_id, taxon_verbatim_id, event_id, updated_at)
		VALUES (in_occurrence_id, in_taxon_verbatim_id, in_event_id, NOW());
	ELSE
		UPDATE public.raw_occurrences
		set updated_at = NOW()
		where occurrence_id = in_occurrence_id and taxon_verbatim_id = in_taxon_verbatim_id;
		END IF;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.add_raw_event_1(
	in_event_id text,
	in_longitude double precision,
	in_latitude double precision,
	in_event_type_id integer,
	in_event_date date)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN

    IF NOT EXISTS(SELECT event_id FROM public.raw_events where event_id=in_event_id)
    THEN
		INSERT INTO public.raw_events(
			event_id, longitude, latitude, coordinates, event_type_id, event_date)
		VALUES (in_event_id, in_longitude, in_latitude, POINT(in_longitude, in_latitude), in_event_type_id, in_event_date);
		END IF;
	
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.add_raw_taxon(
	in_taxon_id text,
	in_kingdom text,
	in_phylum text,
	in_class_taxon text,
	in_order text,
	in_family text,
	in_subfamily text,
	in_genus text,
	in_subgenus text,
	in_species text,
	in_scientific_name text,
	in_canonical_name text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN

    IF NOT EXISTS(SELECT in_taxon_id FROM public.raw_taxa where taxon_id=in_taxon_id)
    THEN
		INSERT INTO public.raw_taxa(
			taxon_id, kingdom, phylum, class_taxon, "order", family, subfamily, genus, subgenus, species, scientific_name, canonical_name)
			VALUES (in_taxon_id, in_kingdom, in_phylum, in_class_taxon, in_order, in_family, in_subfamily, in_genus, in_subgenus, in_species, in_scientific_name, in_canonical_name);	
    END IF;
	
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.add_taxon(
	taxon_id text,
	verb_source text,
	verb_id text)
    RETURNS TABLE(txt text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN

    IF NOT EXISTS(SELECT id FROM public.taxon_id_verbatim where verbatim_source = verb_source AND verbatim_id = verb_id)
    THEN
		INSERT INTO public.taxon_id_verbatim(
		id, verbatim_source, verbatim_id)
		VALUES (taxon_id, verb_source, verb_id);	
    END IF;
	
	RETURN QUERY
	SELECT id FROM public.taxon_id_verbatim where verbatim_source = verb_source AND verbatim_id = verb_id;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.is_in_verbatim_ids(
	in_verbatim_ids text[],
	in_source_id text)
    RETURNS TABLE(_id text, _verbatim_source text, _verbatim_id text, _taxon_id text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
BEGIN
	RETURN QUERY
	select id, verbatim_source, verbatim_id, taxon_id
	FROM public.taxon_id_verbatim where verbatim_id = ANY(in_verbatim_ids) and verbatim_source = in_source_id;
	
END;
$BODY$;