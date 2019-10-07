DROP TABLE IF EXISTS JOHNS;
DROP VIEW IF EXISTS AverageHeightWeight, AverageHeight;

/*QUESTION 0
EXAMPLE QUESTION
What is the highest salary in baseball history?
*/
SELECT MAX(salary) as Max_Salary
FROM Salaries;

/*QUESTION 1
Select the first name, last name, and given name of players who are taller than 6 ft
[hint]: Use "People"
*/
SELECT nameFirst, nameLast, nameGiven from People where height > 72;

/*QUESTION 2
Create a Table of all players with a first name of John who were born in the United States and
played at Fordham university
Include their first name, last name, playerID, and birth state
[hint] Use a Join between People and CollegePlaying
*/
/* 
The table CollegePlaying contains multiple entries of the same player (playerID) for different yearIDs.
*/
CREATE Table JOHNS as
	SELECT distinct nameFirst, nameLast, People.playerID, birthState
    FROM People JOIN CollegePlaying
    ON People.playerID = CollegePlaying.playerID
    WHERE People.nameFirst = "John" and People.birthCountry = "USA" and CollegePlaying.schoolID = "fordham";

/*QUESTION 3
Delete all Johns from the above table whose total career home runs is less than 10
[hint] use a subquery to select these johns from people by playerid
*/
SET SQL_SAFE_UPDATES = 0;
Delete From JOHNS
WHERE EXISTS(
SELECT People.playerID
	FROM People join Batting
    ON People.playerID = Batting.playerID
    WHERE Batting.HR < 10
AND JOHNS.playerID = People.playerID
);
SET SQL_SAFE_UPDATES = 1;


/*QUESTION 3
Group together players with the same birth year, and report the year, 
 the number of players in the year, and average height for the year
 Order the resulting by year in descending order. Put this in a view
 [hint] height will be NULL for some of these years
*/
CREATE VIEW AverageHeight(birthYear, player_count, average_height)
AS
  SELECT birthYear, count(playerID), avg(height) FROM People GROUP BY birthYear ORDER BY birthYear DESC;

/*QUESTION 4
Using Question 3, only include groups with an average weight >180 lbs,
also return the average weight of the group. This time, order by ascending
*/
CREATE VIEW AverageHeightWeight(birthYear, player_count, average_height, average_weight)
AS
SELECT avg_wt.birthYear, player_count, average_height, average_weight
FROM
  (SELECT birthYear, avg(weight) as average_weight FROM People GROUP BY birthYear) avg_wt
  JOIN AverageHeight ON AverageHeight.birthYear = avg_wt.birthYear
  WHERE average_weight > 180
  ORDER BY birthYear ASC;

#select * from schools where state = 'NY';

/*QUESTION 5
Find the players who made it into the hall of fame who played for a college located in NY
return the player ID, first name, last name, and school ID. Group the players by School.
Update all entries with full name Columbia University to 'Columbia University!' in the orginal table
*/
SELECT People.playerID, nameFirst, nameLast, schoolID
FROM
People JOIN 
	(SELECT HallOfFame.playerID as playerID, schoolID
	FROM HallOfFame JOIN
		(SELECT playerID, CollegePlaying.schoolID 
		FROM CollegePlaying 
		JOIN Schools ON CollegePlaying.schoolID = Schools.schoolID
		WHERE Schools.state = "NY") school 
	ON HallOfFame.playerID = school.playerID) ny_halloffame
ON People.playerID = ny_halloffame.playerID ORDER BY schoolID;

SET SQL_SAFE_UPDATES = 0;
UPDATE Schools set name_full = "Columbia University!" where name_full = "Columbia University";
SET SQL_SAFE_UPDATES = 1;

/*QUESTION 6
Find the team id, yearid and average BP for each team using a subquery.
Limit the total number of entries returned to 100
group the entries by team and year and order by descending values
[hint] be careful to only include entries where AB is > 0
*/
SELECT teamID, yearID, avg(HBP) as average_hbp
FROM
(SELECT teamID, yearID, HBP
FROM Teams
WHERE AB > 0) t
GROUP BY teamID, yearID
ORDER BY average_hbp DESC
LIMIT 100;


