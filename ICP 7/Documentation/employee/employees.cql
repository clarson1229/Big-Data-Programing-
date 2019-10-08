USE test;
CREATE TABLE employees (
	employee_id int,
	department text,
	lastname text,
	years_with_company int,
	hiredate date,
	jobtitle text,
	salary int,
	managerid int,
	PRIMARY KEY(employee_id)
);

COPY employees (employee_id, department, lastname, years_with_company,hiredate,jobtitle,salary,managerid) FROM 'employee_entries.csv' WITH HEADER=True AND DELIMITER='|';