DROP TABLE IF EXISTS tags, tagnames, likes, papers, users;

CREATE TABLE IF NOT EXISTS users(
    username VARCHAR(50) NOT NULL,
    password VARCHAR(32) NOT NULL,
    PRIMARY KEY(username)
);

CREATE TABLE IF NOT EXISTS papers(
    pid  SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    title VARCHAR(50),
    begin_time TIMESTAMP NOT NULL,
    description VARCHAR(500),
    data TEXT,
    FOREIGN KEY(username) REFERENCES users ON DELETE CASCADE
);

CREATE INDEX paper_text_idx ON papers USING gin(to_tsvector('english', data));

CREATE TABLE IF NOT EXISTS tagnames(
    tagname VARCHAR(50) NOT NULL,
    PRIMARY KEY(tagname)
);

CREATE TABLE IF NOT EXISTS likes(
    pid INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    like_time TIMESTAMP NOT NULL,
    PRIMARY KEY(pid, username),
    FOREIGN KEY(pid) REFERENCES papers ON DELETE CASCADE,
    FOREIGN KEY(username) REFERENCES users ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tags(
    pid INT NOT NULL,
    tagname VARCHAR(50) NOT NULL,
    PRIMARY KEY(pid, tagname),
    FOREIGN KEY(pid) REFERENCES papers ON DELETE CASCADE,
    FOREIGN KEY(tagname) REFERENCES tagnames ON DELETE CASCADE
);








CREATE TABLE IF NOT EXISTS "User" (
    username VARCHAR(50),
    password VARCHAR(32) NOT NULL,
    PRIMARY KEY (username)
);

CREATE TABLE IF NOT EXISTS "Paper" (
    id SERIAL,
    title VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    content TEXT NOT NULL,
    description VARCHAR(500),
    author VARCHAR(50) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (author) REFERENCES "User" ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS "Like" (
    username VARCHAR(50) NOT NULL,
    paperid Integer NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (username, paperid),
    FOREIGN KEY (username) REFERENCES "User" ON DELETE CASCADE,
    FOREIGN KEY (paperid) REFERENCES "Paper" ON DELETE CASCADE
);

-- Check Tag content alphanumeric  in web app
CREATE TABLE IF NOT EXISTS "Tag" (
    id SERIAL,
    content VARCHAR(50) NOT NULL,
    paperid Integer NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (paperid) REFERENCES "Paper" ON DELETE CASCADE
);