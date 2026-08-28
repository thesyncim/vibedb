-- A real declared table: six columns, a primary key, required fields, and
-- an optional city. Run on the embedded backend or local RF3 PostgreSQL endpoint.
-- Execute each statement separately in Auto mode. Insert the example rows once.

CREATE TABLE IF NOT EXISTS employees (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    team TEXT NOT NULL,
    city TEXT,
    score INTEGER NOT NULL,
    active BOOLEAN NOT NULL
);

INSERT INTO employees (id, name, team, city, score, active)
VALUES
    ('alex', 'Alex', 'Engineering', 'Lisbon', 92, true),
    ('blair', 'Blair', 'Design', 'Porto', 87, true),
    ('casey', 'Casey', 'Engineering', 'Coimbra', 78, false);

SELECT id, name, team, city, score, active
FROM employees
ORDER BY score DESC;

SELECT name, city, score
FROM employees
WHERE active = true AND score >= 85
ORDER BY score DESC;

SELECT team, COUNT(*) AS members, AVG(score) AS average_score
FROM employees
GROUP BY team
ORDER BY team;
