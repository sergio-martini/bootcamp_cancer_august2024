-- Create a lookup table for gender
CREATE TABLE gender_lookup (
    code CHAR(1) PRIMARY KEY,
    description VARCHAR(50)
);

-- Populate the lookup table
INSERT INTO gender_lookup (code, description) VALUES
('1', 'Male'),
('2', 'Female'),
('9', 'Indeterminate'),
('X', 'Not Known');

-- Query to join patient data with the gender lookup
SELECT p.PATIENTID, g.description AS Gender
FROM sim_av_patient AS p
JOIN gender_lookup AS g
ON p.GENDER = g.code;

-- Lookup table for Ethnicity
CREATE TABLE zethnicity (
    code CHAR(1) PRIMARY KEY,
    description VARCHAR(255)
);

-- Insert data into the Ethnicity lookup table
INSERT INTO zethnicity (code, description) VALUES
('0', 'WHITE'),
('8', '8 OTHER'),
('A', 'WHITE BRITISH'),
('B', 'WHITE IRISH'),
('C', 'ANY OTHER WHITE BACKGROUND');

-- Lookup table for Gender
CREATE TABLE zgender (
    code CHAR(1) PRIMARY KEY,
    description VARCHAR(255)
);

-- Insert data into the Gender lookup table
INSERT INTO zgender (code, description) VALUES
('1', 'Male'),
('2', 'Female'),
('9', 'Indeterminate (Unable to be classified as either male or female)'),
('X', 'Not Known (PERSON STATED GENDER CODE not recorded)');
