DROP TABLE IF EXISTS JOHNS;
DROP VIEW IF EXISTS AverageHeightWeight, AverageHeight;

/*QUESTION 0
EXAMPLE QUESTION
What is the highest salary in baseball history?
*/
Select 1
;
/*SAMPLE ANSWER*/
SELECT MAX(salary) as Max_Salary
FROM Salaries;

/*QUESTION 1
Select the first name, last name, and given name of players who are taller than 6 ft
[hint]: Use "People"
*/
SELECT 1, 1, 1 -- replace with your code
;

/*QUESTION 2
Create a Table of all the distinct players with a first name of John who were born in the United States and
played at Fordham university
Include their first name, last name, playerID, and birth state
Add a column called nameFull that is a concatenated version of first and last
[hint] Use a Join between People and CollegePlaying
*/
CREATE Table JOHNS
-- replace with your code
;

/*QUESTION 3
Delete all Johns from the above table whose total career runs batted in is less than 2
[hint] use a subquery to select these johns from people by playerid
[hint] you may have to set sql_safe_updates = 1 to delete without a key
*/
Delete From JOHNS
-- replace with your code
;

/*QUESTION 4
Group together players with the same birth year, and report the year, 
 the number of players in the year, and average height for the year
 Order the resulting by year in descending order. Put this in a view
 [hint] height will be NULL for some of these years
*/
CREATE VIEW AverageHeight(X, X, X)
AS
  SELECT 1, 1, 1 -- replace with your code
;

/*QUESTION 5
Using Question 3, only include groups with an average weight >180 lbs,
also return the average weight of the group. This time, order by ascending
*/
CREATE VIEW AverageHeightWeight(X, X, X, X)
AS
  SELECT 1, 1, 1, 1 -- replace with your code
;
select * from schools where state = 'NY';

/*QUESTION 6
Find the players who made it into the hall of fame who played for a college located in NY
return the player ID, first name, last name, and school ID. Order the players by School alphabetically.
Update all entries with full name Columbia University to 'Columbia University!' in the schools table
*/
SELECT 1, 1, 1, 1 -- replace with your code
;

/*QUESTION 7
Find the team id, yearid and average HBP for each team using a subquery.
Limit the total number of entries returned to 100
group the entries by team and year and order by descending values
[hint] be careful to only include entries where AB is > 0
*/
SELECT 1, 1, 1 -- replace with your code
;
