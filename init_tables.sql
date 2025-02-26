-- Create table rec_raw_news if it does not exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'rec_raw_news') THEN
        CREATE TABLE public.rec_raw_news (
            rec_raw_id serial4 NOT NULL,
            rec_raw_scrape_date TIMESTAMP,
            rec_raw_title text,
            rec_source_id INT,
            rec_raw_source text,
            rec_raw_text text,
            rec_raw_content_hash text,
            rec_raw_content text,
            rec_raw_published_date timestamp,
            rec_raw_source_url text,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT rec_raw_news_pkey PRIMARY KEY (rec_raw_id)
        );
    END IF;
END $$;

-- Create table rec_news_analysis if it does not exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'rec_news_analysis') THEN
        CREATE TABLE public.rec_news_analysis (
            rec_analysis_id serial4 NOT NULL,
            rec_raw_news_id int4 NULL,
            rec_news_keyword_used text NOT NULL,
            rec_news_analysis_date_time timestamp NOT NULL,
            rec_news_summary text NULL,
            rec_analysis_algorithm_version varchar(50) NULL,
            rec_news_metadata jsonb NULL,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
            rec_source_id int4 NULL,
            rec_content_hash text NULL,
            rec_raw_content_hash text NULL,
            CONSTRAINT rec_news_analysis_pkey PRIMARY KEY (rec_analysis_id),
            CONSTRAINT unique_content_hash_in_analysis UNIQUE (rec_content_hash),
            CONSTRAINT fk_rec_raw_news FOREIGN KEY (rec_raw_news_id) REFERENCES public.rec_raw_news(rec_raw_id)
        );
    END IF;
END $$;

-- Create table rec_crypto_market_data if it does not exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'rec_crypto_market_data') THEN
        CREATE TABLE public.rec_crypto_market_data (
            rec_crypto_data_id bigserial NOT NULL,
            rec_raw_news_id int4 NULL,
            rec_source_id int4 NOT NULL,
            rec_api_url text NULL,
            rec_crypto_data jsonb NOT NULL,
            rec_crypto_data_type varchar(50) NULL,
            inserted_at timestamp DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
            rec_crypto_trending_data jsonb NOT NULL,
            CONSTRAINT rec_crypto_market_data_pkey PRIMARY KEY (rec_crypto_data_id),
            CONSTRAINT fk_rec_raw_news FOREIGN KEY (rec_raw_news_id) REFERENCES public.rec_raw_news(rec_raw_id)
        );
    END IF;
END $$;

-- Create table rec_sources if it does not exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'rec_sources') THEN
        CREATE TABLE public.rec_sources (
            rec_source_id serial4 NOT NULL,
            rec_source_name varchar(255) NOT NULL,
            rec_source_type varchar(50) NULL,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamp DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT rec_sources_pkey PRIMARY KEY (rec_source_id),
            CONSTRAINT rec_sources_rec_source_type_check CHECK (
                (rec_source_type)::text = ANY (ARRAY['news', 'crypto', 'social']::text[])
            )
        );
    END IF;
END $$;

-- Create table rec_crypto_analysis if it does not exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'rec_crypto_analysis') THEN
        CREATE TABLE public.rec_crypto_analysis (
            rec_crypto_analysis_id BIGSERIAL PRIMARY KEY,
            rec_news_analysis_id BIGINT NOT NULL,
            fetch_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            rec_keyword_crypto_data JSONB NOT NULL,
            rec_coingecko_coin_id TEXT,
            CONSTRAINT fk_rec_news_analysis FOREIGN KEY (rec_news_analysis_id) REFERENCES public.rec_news_analysis(rec_analysis_id)
        );
    END IF;
END $$;
