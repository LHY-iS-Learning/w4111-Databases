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
SELECT nameFirst, nameLast, nameGiven from People where height > 72;

/*QUESTION 2
Create a Table of all the distinct players with a first name of John who were born in the United States and
played at Fordham university
Include their first name, last name, playerID, and birth state
Add a column called nameFull that is a concatenated version of first and last
[hint] Use a Join between People and CollegePlaying
*/
CREATE Table JOHNS AS
	SELECT DISTINCT
		nameFirst, nameLast, People.playerID, birthState,
		concat(nameFirst, ' ', nameLast) as `nameFull`
	FROM
		People
	JOIN
		CollegePlaying
	ON
		People.playerID = CollegePlaying.playerID
	WHERE
		People.nameFirst = "John" AND
		People.birthCountry = "USA" AND
		CollegePlaying.schoolID = "fordham";

/*QUESTION 3
Delete all Johns from the above table whose total career runs batted in is less than 2
[hint] use a subquery to select these johns from people by playerid
[hint] you may have to set sql_safe_updates = 1 to delete without a key
*/
SET sql_safe_update = 0;
Delete From JOHNS
WHERE EXISTS(
	SELECT
		People.playerID
	FROM
		People
	JOIN
		Batting
	ON
		People.playerID = Batting.playerID
	WHERE
		Batting.RBI < 2
	AND
		JOHNS.playerID = People.playerID
);
SET sql_safe_update = 1;


/*QUESTION 4
Group together players with the same birth year, and report the year,
	the number of players in the year, and average height for the year
	Order the resulting by year in descending order. Put this in a view
	[hint] height will be NULL for some of these years
*/
CREATE VIEW AverageHeight(birthYear, num, avgHeight)
AS
	SELECT
		birthYear, count(playerID), avg(height)
	FROM
		People
	GROUP BY
		birthYear
	ORDER BY
		birthYear
	DESC;

/*QUESTION 5
Using Question 3, only include groups with an average weight >180 lbs,
also return the average weight of the group. This time, order by ascending
*/
CREATE VIEW AverageHeightWeight(birthYear, num, avgHeight, avgWeight)
AS
	SELECT
		avg_weight.birthYear, AverageHeight.num, AverageHeight.avgHeight, avgWeight
	FROM
		(SELECT
			birthYear, avg(weight) as avgWeight
		FROM
			People
		GROUP BY
			birthYear
	) avg_weight
	JOIN
		AverageHeight
	ON
		AverageHeight.birthYear = avg_weight.birthYear
	WHERE
		avgWeight > 180
	ORDER BY
		birthYear
	ASC;

/*QUESTION 6
Find the players who made it into the hall of fame who played for a college located in NY
return the player ID, first name, last name, and school ID. Order the players by School alphabetically.
Update all entries with full name Columbia University to 'Columbia University!' in the schools table
*/
SELECT
	People.playerID, nameFirst, nameLast, schoolID
FROM
	People
JOIN
	(SELECT
		college_hof.playerID, Schools.schoolID
	FROM
		Schools
	JOIN
		(SELECT
			CollegePlaying.playerID, schoolID
		FROM
			HallofFame
		JOIN
			CollegePlaying
		ON
			HallofFame.playerID = CollegePlaying.playerID
		) college_hof
	ON
		college_hof.schoolID = Schools.schoolID
	WHERE
		Schools.state = "NY"
	) ny_schools
ON
	People.playerID = ny_schools.playerID
ORDER BY
	schoolID
ASC;


SET SQL_SAFE_UPDATES = 0;
UPDATE
	Schools
SET
	name_full = "Columbia University!"
WHERE
	name_full = "Columbia University";
SET SQL_SAFE_UPDATES = 1;

/*QUESTION 7
Find the team id, yearid and average HBP for each team using a subquery.
Limit the total number of entries returned to 100
group the entries by team and year and order by descending values
[hint] be careful to only include entries where AB is > 0
*/
SELECT
	teamID, yearID, avg(HBP)
FROM
	(SELECT
		teamID, yearID, HBP
	FROM
		teams
	WHERE
		AB > 0
	)t
GROUP BY
	teamID, yearID
ORDER BY
	avg(HBP)
DESC
LIMIT 100;
