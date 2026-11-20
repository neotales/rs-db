ALTER TABLE {{schema}}.{{table}} ADD COLUMN active boolean NOT NULL DEFAULT true;
INSERT INTO {{schema}}.{{table}} VALUES (2, 'second', true);
