BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS trackster_client_users CASCADE;
DROP TABLE IF EXISTS trackster_users CASCADE;
DROP TABLE IF EXISTS trackster_clients CASCADE;
DROP TABLE IF EXISTS trackster_roles CASCADE;

CREATE TABLE trackster_roles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    role_code VARCHAR(64) NOT NULL,
    role_name VARCHAR(128) NOT NULL,
    description VARCHAR(512),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_trackster_roles_role_code
        UNIQUE (role_code)
);

CREATE TABLE trackster_clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id VARCHAR(32) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    company_email VARCHAR(255),
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_trackster_clients_client_id
        UNIQUE (client_id)
);

CREATE TABLE trackster_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    full_name VARCHAR(255),
    global_role_id UUID,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_trackster_users_username
        UNIQUE (username),

    CONSTRAINT uq_trackster_users_email
        UNIQUE (email),

    CONSTRAINT fk_trackster_users_global_role
        FOREIGN KEY (global_role_id)
        REFERENCES trackster_roles(id)
);

CREATE TABLE trackster_client_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id VARCHAR(32) NOT NULL,
    user_id UUID NOT NULL,
    role_id UUID NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_trackster_client_users_client
        FOREIGN KEY (client_id)
        REFERENCES trackster_clients(client_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_trackster_client_users_user
        FOREIGN KEY (user_id)
        REFERENCES trackster_users(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_trackster_client_users_role
        FOREIGN KEY (role_id)
        REFERENCES trackster_roles(id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT uq_trackster_client_users_client_user
        UNIQUE (client_id, user_id)
);

CREATE INDEX idx_trackster_roles_role_code
ON trackster_roles(role_code);

CREATE INDEX idx_trackster_users_username
ON trackster_users(username);

CREATE INDEX idx_trackster_users_global_role
ON trackster_users(global_role_id);

CREATE INDEX idx_trackster_client_users_client_id
ON trackster_client_users(client_id);

CREATE INDEX idx_trackster_client_users_user_id
ON trackster_client_users(user_id);

CREATE INDEX idx_trackster_client_users_role_id
ON trackster_client_users(role_id);

COMMIT;