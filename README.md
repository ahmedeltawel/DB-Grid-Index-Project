# Evergreen Team Project

## Team Members
- Abdelrahman Mahmoud Hafez (46-12167@T36)
- Ahmed Ibrahim (46-8041@T21)
- Anas El Nemr (46-6226@T21)

## Project Overview
This project involves the creation of a system that parses and executes SQL statements, respecting the precedence of operations such as AND, OR, and XOR. Additionally, the system supports parsing plain text SQL statements.

## Bonus Features
1. **Operation Precedence**: The system respects the precedence of logical operations (AND, OR, XOR) while selecting records.
2. **Plain Text SQL Parsing**: Added support for parsing plain text SQL statements.

## Tested Examples
The following are examples of SQL statements that have been successfully tested with the `parseSQL(StringBuffer)` method:

1. **SELECT Statement**: 
    ```sql
    SELECT * FROM transcripts WHERE gpa > 4.0 AND student_id > '45-0000';
    ```
2. **INSERT Statement**: 
    ```sql
    INSERT INTO transcripts(gpa, student_id) VALUES (4.5, '63-4499');
    ```
3. **UPDATE Statement**: 
    ```sql
    UPDATE transcripts SET student_id = '99-9999' WHERE gpa = 3.1964;
    ```
4. **DELETE Statement**: 
    ```sql
    DELETE FROM transcripts WHERE gpa = 3.1964 AND course_name = 'iQrcnv';
    ```
5. **CREATE INDEX Statement**: 
    ```sql
    CREATE INDEX idx ON transcripts (gpa, student_id);
    ```
6. **CREATE TABLE Statement**: 
    ```sql
    CREATE TABLE sami (
        gpa decimal(5,2), 
        student_id varchar(25), 
        PRIMARY KEY (gpa)
    );
    ```

## Code Example
```java
db.parseSQL((new StringBuffer()).append("CREATE INDEX idx ON transcripts (gpa, student_id"));
    ```

<dependency>
    <groupId>com.github.jsqlparser</groupId>
    <artifactId>jsqlparser</artifactId>
    <version>4.0</version>
</dependency>
<dependency>
    <groupId>com.github.skebir</groupId>
    <artifactId>prettytable</artifactId>
    <version>1.0</version>
</dependency>
