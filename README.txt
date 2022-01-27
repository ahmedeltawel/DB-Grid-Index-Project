					~ TEAM MEMBERS ~
Team Name: Evergreen
Abdelrahman mahmoud hafez 	46-12167@T36
Ahmed ibrahim 				46-8041@T21
Anas elnemr 				46-6226@T21

# We've done the following additions(Bonuses):
1- Respected the precedence of Operations(AND, OR, XOR) while Selecting.
2- Added Support for parsing plain Text SQL Statements.

# These are tested examples passed to parseSQL(StringBuffer) method:
"SELECT * FROM transcripts WHERE gpa>4.0 and student_id>'45-0000';"
"INSERT INTO transcripts(gpa,student_id) VALUES (4.5,'63-4499')"
"UPDATE transcripts SET student_id='99-9999' WHERE gpa=3.1964"
"DELETE FROM transcripts WHERE gpa=3.1964 AND course_name='iQrcnv'"
"CREATE INDEX idx ON transcripts (gpa,student_id)"
"CREATE TABLE sami (gpa decimal(5,2),student_id varchar(25),PRIMARY KEY (gpa))"

db.parseSQL((new StringBuffer()).append("CREATE INDEX idx ON transcripts (gpa,student_id"));

# The dependencies defined in the pom.xml file:
1. JSQLParser / JSqlParser com.github.jsqlparser:jsqlparser	4.0
2. com.github.skebir:prettytable 							1.0

