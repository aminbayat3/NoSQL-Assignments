import {
  connectToDatabase,
  closeDatabaseConnection,
  db
} from "./dbConnection.js";

const collectionName = "video_movieDetails";
const videoMovieDetailsCollection = db.collection(collectionName);

// query part a
const findMoviesByDirector = async (directorName) => {
  try {
    const query = { director: directorName };

    const movies = await videoMovieDetailsCollection.find(query).toArray();

    return movies;
  } catch (error) {
    console.error("Error:", error);
  }
};

// query part b
const findMoviesTitleByDirector = async (directorName) => {
  try {
    const query = { director: directorName };

    const movies = await videoMovieDetailsCollection.find(query).toArray();

    return movies.map((movie) => movie.title);
  } catch (error) {
    console.error("Error:", error);
  }
};

// query part c
const findMoviesByDirectorOrUnknown = async (directorName) => {
  try {
    const query = { $or: [{ director: directorName }, { director: null }] };

    const movies = await videoMovieDetailsCollection.find(query).toArray();

    return movies.map((movie) => movie.title);
  } catch (error) {
    console.error("Error:", error);
  }
};

// query part D
const findMoviesByDirectorBeforeYear = async (directorName, year) => {
  try {
    const query = { director: directorName, year: { $lt: year } };

    const movies = await videoMovieDetailsCollection.find(query).toArray();

    return movies;
  } catch (error) {
    console.error("Error:", error);
  }
};

//Part F
const findMoviesByActors = async (actorNames) => {
  try {
    const query = { actors: { $all: actorNames } };

    const movies = await videoMovieDetailsCollection.find(query).toArray();

    return movies;
  } catch (error) {
    console.error("Error:", error);
  }
};

//Part G
const findMoviesByEitherActors = async (actorNames) => {
  try {
    const query = { actors: { $in: actorNames } };

    const movies = await videoMovieDetailsCollection.find(query).toArray();

    return movies;
  } catch (error) {
    console.error("Error:", error);
  }
};

//Part H
const findMoviesByActorsWithMinAwards = async (actorNames, minAwards) => {
  try {
    const query = {
      actors: { $all: actorNames },
      "awards.wins": { $gte: minAwards },
    };

    const movies = await videoMovieDetailsCollection.find(query).toArray();

    return movies;
  } catch (error) {
    console.error("Error:", error);
  }
};

const ExerciseOnePartA = async () => {
  console.log("Exercise One PartA");
  const directorName = "Roland Emmerich";
  const movies = await findMoviesByDirector(directorName);
  movies.length > 0
    ? console.log("movies: ", movies)
    : console.log(`No movies found for director: ${directorName}`);

  console.log(
    "\n ****************************************************************************************** \n"
  );
};

const ExerciseOnePartB = async () => {
  console.log("Exercise One Part B");
  const directorName = "Roland Emmerich";
  const titles = await findMoviesTitleByDirector(directorName);
  titles.length > 0
    ? console.log("titles", titles)
    : console.log(`No titles found for director: ${directorName}`);

  console.log(
    "\n ****************************************************************************************** \n"
  );
};

const ExerciseOnePartC = async () => {
  console.log("Exercise One Part C");
  const directorName = "Jon Brewer";
  const movieTitles = await findMoviesByDirectorOrUnknown(directorName);
  movieTitles.length > 0
    ? console.log(`Movie Titles from ${directorName}`, movieTitles)
    : console.log(`No movie titles found for director: ${directorName}`);

  console.log(
    "\n ****************************************************************************************** \n"
  );
};

const ExerciseOnePartD = async () => {
  console.log("Exercise One Part D");
  const directorName = "Curt McDowell";
  const year = 1981;
  const movies = await findMoviesByDirectorBeforeYear(directorName, year);
  movies.length > 0
    ? console.log(
        `movies directed by ${directorName} Before year ${year}`,
        movies
      )
    : console.log(`No titles found for director: ${directorName}`);

  console.log(
    "\n ****************************************************************************************** \n"
  );
};

const ExerciseOnePartF = async () => {
  console.log("Exercise One Part F");
  const actors = ["Will Smith", "Martin Lawrence"];
  const movies = await findMoviesByActors(actors);
  movies.length > 0
    ? console.log("movies with actors " + actors, movies)
    : console.log(`No movies found for the actors: ${actors}`);

  console.log(
    "\n ****************************************************************************************** \n"
  );
};

const ExerciseOnePartG = async () => {
  console.log("Exercise One Part G");
  const actors = ["Will Smith", "Martin Lawrence"];
  const movies = await findMoviesByEitherActors(actors);
  movies.length > 0
    ? console.log("movies with either actors " + actors, movies)
    : console.log(`No movies found for either actors: ${actors}`);

  console.log(
    "\n ****************************************************************************************** \n"
  );
};

const ExerciseOnePartH = async () => {
  console.log("Exercise One Part H");
  const actors = ["Will Smith", "Martin Lawrence"];
  const minAwards = 2;
  const movies = await findMoviesByActorsWithMinAwards(actors, minAwards);
  movies.length > 0
    ? console.log(`movies with actors: ${actors} and atleast ${minAwards} awards `, movies)
    : console.log(`No movies found for actors: ${actors} with atleast ${minAwards} awards`);

  console.log(
    "\n ****************************************************************************************** \n"
  );
};

const ExerciseOne = async () => {
  await connectToDatabase();

  await ExerciseOnePartA();
  await ExerciseOnePartB();
  await ExerciseOnePartC();
  await ExerciseOnePartD();
  await ExerciseOnePartF();
  await ExerciseOnePartG();
  await ExerciseOnePartH();

  await closeDatabaseConnection();
};

ExerciseOne();
