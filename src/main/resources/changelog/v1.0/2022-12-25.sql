-- liquibase formatted sql

-- changeset kirill-parfenov:create-topics_offsets-table
CREATE TABLE IF NOT EXISTS topics_offsets (
    id          UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    key_         varchar unique,
    topic        varchar,
    offset_      bigint,
    partition_   bigint,
    created_at bigint DEFAULT EXTRACT('epoch' from now()) * 1000
);
grant all privileges on topics_offsets to kafka_user;