create table advisors(
    advisorId int primary key, 
    advisorName text
);

create table students(
    studentId int primary key, 
    firstname text, 
    lastname text, 
    email text, 
    major text, 
    advisorId int not null, 
    foreign key (advisorId) references advisors(advisorId)
);

create table courses(
    courseId int primary key, 
    courseName text, 
    creditHours int
);

create table enrollment(
    enrollmentId int primary key, 
    studentId int, 
    courseId int, 
    semester text, 
    grade text,
    foreign key (studentId) references students(studentId),
    foreign key (courseId) references courses(courseId)
);

create table instruct(
    instructorId int,
    courseId int,
    primary key (instructorId, courseId),
    foreign key (instructorId) references advisors(advisorId),
    foreign key (courseId) references courses(courseId)
);

-- There is no redundancies of the observed, because we use a table advisors to store all advisor/instructor information and adds a foreign key in students.
-- We also remove the instructorId, instructorName from courses and add a relationship table instruct to store corseId and instructorId as foreign keys.