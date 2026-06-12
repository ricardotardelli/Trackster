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
        UNIQUE (role_code),

    CONSTRAINT ck_trackster_roles_role_code
        CHECK (role_code IN (
            'trackster_admin',
            'client_admin',
            'client_user'
        ))
);

CREATE TABLE trackster_clients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id VARCHAR(32) NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    company_email VARCHAR(255),
    contact_name VARCHAR(255),
    country VARCHAR(255),
    phone VARCHAR(64),
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_trackster_clients_client_id
        UNIQUE (client_id),

    CONSTRAINT ck_trackster_clients_status
        CHECK (status IN (
            'active',
            'inactive',
            'suspended'
        ))
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

    CONSTRAINT ck_trackster_users_status
        CHECK (status IN (
            'active',
            'inactive',
            'suspended'
        )),

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

    CONSTRAINT ck_trackster_client_users_status
        CHECK (status IN (
            'active',
            'inactive',
            'suspended'
        )),

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

CREATE INDEX idx_trackster_clients_client_id
ON trackster_clients(client_id);

CREATE INDEX idx_trackster_clients_status
ON trackster_clients(status);

CREATE INDEX idx_trackster_users_username
ON trackster_users(username);

CREATE INDEX idx_trackster_users_global_role
ON trackster_users(global_role_id);

CREATE INDEX idx_trackster_users_status
ON trackster_users(status);

CREATE INDEX idx_trackster_client_users_client_id
ON trackster_client_users(client_id);

CREATE INDEX idx_trackster_client_users_user_id
ON trackster_client_users(user_id);

CREATE INDEX idx_trackster_client_users_role_id
ON trackster_client_users(role_id);

CREATE INDEX idx_trackster_client_users_status
ON trackster_client_users(status);

INSERT INTO trackster_roles (
    role_code,
    role_name,
    description
)
VALUES
(
    'trackster_admin',
    'Trackster Administrator',
    'Global Trackster administrator with access to platform-level administration.'
),
(
    'client_admin',
    'Client Administrator',
    'Client tenant administrator with access to manage users for a specific client.'
),
(
    'client_user',
    'Client User',
    'Standard client user with access to Trackster tenant functionality.'
);

INSERT INTO trackster_clients (
    client_id,
    company_name,
    company_email,
    contact_name,
    country,
    phone,
    status
)
VALUES
(
    '00000000',
    'Trackster Co.',
    'contact@trackster.pt',
    'Ricardo Tardelli',
    'Portugal',
    '+351910869867',
    'active'
),
(
    '00000001',
    'Client A',
    'admin-a@example.com',
    'Client A Administrator',
    'Portugal',
    '+351911111111',
    'active'
),
(
    '00000002',
    'Client B',
    'admin-b@example.com',
    'Client B Administrator',
    'Portugal',
    '+351922222222',
    'active'
);

INSERT INTO trackster_users (
    username,
    email,
    full_name,
    global_role_id,
    status
)
VALUES
(
    'kadut',
    'contact@trackster.pt',
    'Ricardo Tardelli',
    (
        SELECT id
        FROM trackster_roles
        WHERE role_code = 'trackster_admin'
    ),
    'active'
);

INSERT INTO trackster_users (
    username,
    email,
    full_name,
    status
)
VALUES
(
    'trackster.co.user',
    'co.user@trackster.local',
    'Trackster Co. User',
    'active'
),
(
    'trackster.co.ops',
    'co.ops@trackster.local',
    'Trackster Co. Ops',
    'inactive'
),
(
    'client.a.admin',
    'admin-a@example.com',
    'Client A Admin',
    'active'
),
(
    'client.a.user',
    'user-a@example.com',
    'Client A User',
    'active'
),
(
    'client.b.admin',
    'admin-b@example.com',
    'Client B Admin',
    'active'
),
(
    'client.b.ops',
    'ops-b@example.com',
    'Client B Ops',
    'active'
),
(
    'client.b.user.one',
    'user-one-b@example.com',
    'Client B User One',
    'active'
),
(
    'client.b.user.two',
    'user-two-b@example.com',
    'Client B User Two',
    'suspended'
);

INSERT INTO trackster_client_users (
    client_id,
    user_id,
    role_id,
    status
)
SELECT
    '00000000',
    u.id,
    r.id,
    u.status
FROM trackster_users u
JOIN trackster_roles r
    ON r.role_code =
        CASE
            WHEN u.username = 'kadut'
                THEN 'client_admin'
            ELSE 'client_user'
        END
WHERE u.username IN (
    'kadut',
    'trackster.co.user',
    'trackster.co.ops'
);

INSERT INTO trackster_client_users (
    client_id,
    user_id,
    role_id,
    status
)
SELECT
    '00000001',
    u.id,
    r.id,
    u.status
FROM trackster_users u
JOIN trackster_roles r
    ON r.role_code =
        CASE
            WHEN u.username = 'client.a.admin'
                THEN 'client_admin'
            ELSE 'client_user'
        END
WHERE u.username IN (
    'client.a.admin',
    'client.a.user'
);

INSERT INTO trackster_client_users (
    client_id,
    user_id,
    role_id,
    status
)
SELECT
    '00000002',
    u.id,
    r.id,
    u.status
FROM trackster_users u
JOIN trackster_roles r
    ON r.role_code =
        CASE
            WHEN u.username IN (
                'client.b.admin',
                'client.b.ops'
            )
                THEN 'client_admin'
            ELSE 'client_user'
        END
WHERE u.username IN (
    'client.b.admin',
    'client.b.ops',
    'client.b.user.one',
    'client.b.user.two'
);

COMMIT;