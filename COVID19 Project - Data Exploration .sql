/* 
COVID19 Data Exploration
Skills used:	Joins
		CTE's
		Windows Functions
		Aggregate Functions
		Creating Views
		Converting Data Types
*/

-- Data exploration for CovidDeaths dataset
SELECT *
FROM Covid19.dbo.CovidDeaths
WHERE continent IS NOT NULL  -- we removed all the records where continent is null because those records gave us results for the entire continent and not for a specific country
ORDER BY 3,4

-- Removing duplicate rows by using CTE
WITH identified_duplicates (location, date, total_cases, new_cases, total_deaths, population, duplicate_count) AS
	(
	SELECT 	location,
		date,
		total_cases,
		new_cases,
		total_deaths,
		population,
		ROW_NUMBER() OVER (PARTITION BY location, date ORDER BY location, date) AS duplicate_count
	FROM Covid19.dbo.CovidDeaths
	)
	DELETE
	FROM identified_duplicates
	WHERE duplicate_count > 1


-- Selecting the data that will be used
SELECT	location,
	date,
	total_cases,
	new_cases,
	total_deaths,
	population
FROM Covid19.dbo.CovidDeaths
WHERE continent IS NOT NULL
order by 1, 2

-- How many cases are there in this country and what's the percentage of those people who died had covid?
-- Likelihood of dying if you have Covid in Canada
SELECT	location,
	date,
	total_cases,
	total_deaths,
	ROUND((total_deaths/total_cases)*100,2) AS DeathRate
FROM Covid19.dbo.CovidDeaths
WHERE location = 'Canada'
AND continent IS NOT NULL
order by 1, 2


-- Total Cases vs Population
-- Percentage of population that got Covid in Canada
SELECT	location,
	date,
	total_cases,
	population,
	ROUND((total_cases/population)*100,2) AS CovidRate
FROM Covid19.dbo.CovidDeaths
WHERE location = 'Canada'
AND continent IS NOT NULL
order by 1, 2


-- Countries have the highest Covid rate (in respective to their population)
SELECT	location,
	MAX(total_cases) AS HighestCovidCount,
	population,
	ROUND(MAX((total_cases/population)*100),2) AS CovidRate
FROM Covid19.dbo.CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location, population
order by CovidRate desc


-- Highest death count ranked by country
SELECT	location,
	MAX(cast(total_deaths AS int)) AS HighestDeathCount  
FROM Covid19.dbo.CovidDeaths
WHERE continent IS NOT NULL 
GROUP BY location
order by HighestDeathCount desc


-- Highest death count ranked by continent
SELECT	location,
	MAX(cast(total_deaths AS int)) AS HighestDeathCount  --cast function changes the data type from varchar to int
FROM Covid19.dbo.CovidDeaths
WHERE continent IS NULL -- filters for only continents
GROUP BY location
order by HighestDeathCount desc


-- Overall global death rate
SELECT	SUM(new_cases) AS total_cases,
	SUM(cast(new_deaths AS int)) AS total_deaths,
	ROUND(SUM(cast(new_deaths AS int))/SUM(new_cases)*100,2) AS DeathRate
FROM Covid19.dbo.CovidDeaths
WHERE continent is NOT NULL
ORDER BY 1,2


-- Global death rate per day
SELECT	date,
	SUM(new_cases) AS total_cases,
	SUM(cast(new_deaths AS int)) AS total_deaths,
	ROUND(SUM(cast(new_deaths AS int))/SUM(new_cases)*100,2) AS DeathRate
FROM Covid19.dbo.CovidDeaths
WHERE continent is NOT NULL
GROUP BY date
ORDER BY 1,2


-- Join CovidVaccine and CovidDeaths dataset together
SELECT *
FROM Covid19.dbo.CovidDeaths CovidDeaths
JOIN Covid19.dbo.CovidVaccines CovidVaccines
	ON CovidDeaths.location = CovidVaccines.location
	AND CovidDeaths.date = CovidVaccines.date
ORDER BY CovidDeaths.date


-- Total population vs vaccinations
SELECT	CovidDeaths.continent,
	CovidDeaths.location,
	CovidDeaths.date,
	CovidDeaths.population,
	CovidVaccines.new_vaccinations
FROM Covid19.dbo.CovidDeaths CovidDeaths
JOIN Covid19.dbo.CovidVaccines CovidVaccines
	ON CovidDeaths.location = CovidVaccines.location
	AND CovidDeaths.date = CovidVaccines.date
WHERE CovidDeaths.continent IS NOT NULL


-- Total new vaccinations for each country by day on a rolling basis
SELECT	CovidDeaths.continent,
	CovidDeaths.location,
	CovidDeaths.date,
	CovidDeaths.population,
	CovidVaccines.new_vaccinations,
	SUM(CAST(CovidVaccines.new_vaccinations AS INT)) OVER (PARTITION BY CovidDeaths.location
	ORDER BY CovidDeaths.location, CovidDeaths.date) AS RollingPeopleVaccinated
FROM Covid19.dbo.CovidDeaths CovidDeaths
JOIN Covid19.dbo.CovidVaccines CovidVaccines
	ON CovidDeaths.location = CovidVaccines.location
	AND CovidDeaths.date = CovidVaccines.date
WHERE CovidDeaths.continent IS NOT NULL


-- Calculate RollingPercentagePeopleVaccinated using CTE
WITH PopulationvsVaccination (Continent, Location, date, population, New_Vaccinations, RollingPeopleVaccinated)
AS
(
	SELECT	CovidDeaths.continent,
		CovidDeaths.location,
		CovidDeaths.date,
		CovidDeaths.population,
		CovidVaccines.new_vaccinations,
		SUM(CAST(CovidVaccines.new_vaccinations AS INT)) OVER (PARTITION BY CovidDeaths.location
		ORDER BY CovidDeaths.location, CovidDeaths.date) AS RollingPeopleVaccinated
	FROM Covid19.dbo.CovidDeaths CovidDeaths
	JOIN Covid19.dbo.CovidVaccines CovidVaccines
		ON CovidDeaths.location = CovidVaccines.location
		AND CovidDeaths.date = CovidVaccines.date
	WHERE CovidDeaths.continent IS NOT NULL
)
SELECT	*,
	ROUND((RollingPeopleVaccinated/population)*100,2) AS RollingPercentagePopulationVaccinated
FROM PopulationvsVaccination


-- Calculate RollingPercentagePeopleVaccinated in Canada using CTE
WITH PopulationvsVaccination (Continent, Location, date, population, New_Vaccinations, RollingPeopleVaccinated)
AS
(
	SELECT	CovidDeaths.continent,
		CovidDeaths.location,
		CovidDeaths.date,
		CovidDeaths.population,
		CovidVaccines.new_vaccinations,
		SUM(CAST(CovidVaccines.new_vaccinations AS INT)) OVER (PARTITION BY CovidDeaths.location
		ORDER BY CovidDeaths.location, CovidDeaths.date) AS RollingPeopleVaccinated
	FROM Covid19.dbo.CovidDeaths CovidDeaths
	JOIN Covid19.dbo.CovidVaccines CovidVaccines
		ON CovidDeaths.location = CovidVaccines.location
		AND CovidDeaths.date = CovidVaccines.date
	WHERE CovidDeaths.continent IS NOT NULL
)
SELECT	*,
	ROUND((RollingPeopleVaccinated/population)*100,2) AS RollingPercentagePopulationVaccinated
FROM PopulationvsVaccination
WHERE location = 'Canada'


-- Delta of new cases compared to previous day in Canada
SELECT	location,
	date,
	new_cases,
	new_cases - LAG(new_cases,1) OVER (PARTITION BY location
	ORDER BY date) AS new_cases_delta
FROM Covid19.dbo.CovidDeaths CovidDeaths
WHERE location = 'Canada'


-- Creating view of rolling vaccinations in Canada
CREATE VIEW CanadaRollingVaccination AS
WITH PopulationvsVaccination (Continent, Location, date, population, New_Vaccinations, RollingPeopleVaccinated)
AS
(
	SELECT	CovidDeaths.continent,
		CovidDeaths.location,
		CovidDeaths.date,
		CovidDeaths.population,
		CovidVaccines.new_vaccinations,
		SUM(CAST(CovidVaccines.new_vaccinations AS INT)) OVER (PARTITION BY CovidDeaths.location
		ORDER BY CovidDeaths.location, CovidDeaths.date) AS RollingPeopleVaccinated
	FROM Covid19.dbo.CovidDeaths CovidDeaths
	JOIN Covid19.dbo.CovidVaccines CovidVaccines
		ON CovidDeaths.location = CovidVaccines.location
		AND CovidDeaths.date = CovidVaccines.date
	WHERE CovidDeaths.continent IS NOT NULL
)
SELECT	*,
	ROUND((RollingPeopleVaccinated/population)*100,2) AS RollingPercentagePopulationVaccinated
FROM PopulationvsVaccination
WHERE location = 'Canada'

SELECT *
FROM CanadaRollingVaccination

